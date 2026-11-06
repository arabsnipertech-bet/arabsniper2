"""Publish evidence-led Telegram content from frozen picks and official audits only."""
import json
import math
import os
import subprocess
from datetime import datetime, timedelta, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from telegram_gold_promo import telegram, posted_before

ROME=ZoneInfo("Europe/Rome")
CHANNEL="@arabsniper_signals"
SLOTS={
    "45 8 * * *":"audit","58 8 * * *":"audit",
    "45 9 * * *":"audit","58 9 * * *":"audit",
    "45 14 * * *":"market","58 14 * * *":"market",
    "45 15 * * *":"market","58 15 * * *":"market"
}
LABELS={"over15_ht":"Over 1,5 PT","over25_ht":"Over 2,5 PT","gg_ht":"Goal PT","over25_ft":"Over 2,5 FT","over35_ft":"Over 3,5 FT"}


def choose_slot(now,event,cron,recovery_slot=None):
    if event=="schedule":
        if cron not in SLOTS:
            return None
        selected=SLOTS[cron]
    elif event=="workflow_dispatch":
        if recovery_slot not in ("audit","market"):
            return None
        selected=recovery_slot
    else:
        return None
    local=now.astimezone(ROME)
    scheduled_hour,scheduled_minute=(10,45) if selected=="audit" else (16,45)
    due=datetime.combine(local.date(),time(scheduled_hour,scheduled_minute),tzinfo=ROME)
    if not timedelta(0)<=local-due<=timedelta(minutes=38):
        return None
    if event=="schedule":
        # Reject a cron intended for the wrong CET/CEST season.
        minute,hour=map(int,cron.split()[:2])
        expected_utc=due.astimezone(timezone.utc)
        expected_pairs={(expected_utc.hour,expected_utc.minute),
                        ((expected_utc+timedelta(minutes=13)).hour,
                         (expected_utc+timedelta(minutes=13)).minute)}
        if (hour,minute) not in expected_pairs:
            return None
    return selected


def audit_message(report):
    """Show the entire verified posted population, not just the winners."""
    data=report.get("selections_total") or {}
    total,won,lost,pending=(data.get(k) for k in ("total","won","lost","pending"))
    if not all(type(x) is int and x>=0 for x in (total,won,lost,pending)) or total!=won+lost+pending or total==0:
        return None
    settled=won+lost
    day=report.get("date")
    if not isinstance(day,str) or len(day)!=10:
        return None
    lines=[f"📊 ARABSNIPER · RISCONTRI TRASPARENTI {day}",
           "Solo pronostici effettivamente pubblicati e confrontati con risultati ufficiali.",
           f"Selezioni: {total} | ✅ Vinte: {won} | ❌ Perse: {lost} | ⏳ Da verificare: {pending}"]
    if settled:
        lines.append(f"Riscontro sui {settled} esiti chiusi: {won/settled*100:.1f}%")
    pnl=data.get("theoretical_pnl")
    if settled and isinstance(pnl,(int,float)) and math.isfinite(pnl):
        lines.append(f"Saldo teorico (1 unità/selezione): {pnl:+.2f} unità, senza commissioni")
    ticket=report.get("triple") or {}
    if ticket.get("status") in ("won","lost","pending"):
        verdict={"won":"✅ centrata","lost":"❌ non centrata","pending":"⏳ in attesa"}[ticket["status"]]
        lines.append("Tripla ore 13: "+verdict)
    lines+=["Non nascondiamo le selezioni non riuscite. Le quote e i risultati passati non garantiscono risultati futuri.",
            "🎁 Gold 7 giorni senza carta, addebiti o rinnovo: https://t.me/ArabSniperBot",
            "18+ · Gioca responsabilmente."]
    return "\n".join(lines)


def market_message(selections):
    valid=[]
    for p in selections:
        delta=p.get("delta_pct")
        oci=p.get("oci")
        odd=p.get("odd")
        if (not all(type(x) in (int,float) and math.isfinite(x) for x in (delta,oci,odd))
            or odd<=1 or not p.get("match") or p.get("market") not in LABELS
            or not p.get("bookmaker") or abs(delta)>95 or abs(oci)>1000):
            continue
        valid.append(p)
    if not valid:
        return None
    # Reuse existing Gold/Platinum qualification: never upgrade a non-eligible match.
    p=max(valid,key=lambda x:(x["source_tier"]=="platinum",abs(x["delta_pct"]),x["oci"]))
    movement=("in calo" if p["delta_pct"]<0 else "in aumento" if p["delta_pct"]>0 else "stabile")
    reasons=[f"Movimento quota {movement}: {p['delta_pct']:+.1f}%",
             f"Indice OCI del modello: {p['oci']:+.1f}",
             f"Operatore rilevato: {p['bookmaker']}"]
    if p.get("market_confirmed"):
        reasons.append("Conferma di mercato segnalata dal modello")
    if p.get("stable"):
        reasons.append("Segnale di stabilità presente")
    kickoff=datetime.fromisoformat(p["kickoff"]).astimezone(ROME).strftime("%H:%M")
    lines=[f"🔎 ARABSNIPER · PERCHÉ QUESTA ANALISI ({p['source_tier'].upper()})",
           str(p["match"]),f"{LABELS[p['market']]} · Quota osservata {p['odd']:.2f} · Inizio {kickoff}",
           "Motivazioni basate sul dato registrato:"]
    lines.extend("• "+x for x in reasons)
    lines+=["I movimenti delle quote descrivono il mercato: non assicurano l'esito.",
            "🎁 Prova Gold 7 giorni, senza carta o rinnovo: https://t.me/ArabSniperBot",
            "18+ · Gioca responsabilmente."]
    return "\n".join(lines)


EDUCATIONAL_THEMES=(('OCI: CONFERMA O CONTRADDIZIONE?',
  'Un OCI positivo descrive un segnale interno sui movimenti di mercato: non significa che la probabilità di '
  'vincita sia pari al suo valore. Se la quota scende ma la stima statistica resta debole, abbiamo una '
  'divergenza da approfondire. Leggiamo insieme OCI, prezzo disponibile e probabilità, senza trasformare un '
  'solo indicatore in un pronostico.'),
 ('AGGIO: IL MARGINE NASCOSTO NELLE QUOTE',
  'Esempio su un mercato a due esiti: quote 1,80 e 2,10 implicano 55,56% e 47,62%. La somma è 103,18%: '
  'l’eccedenza di 3,18 punti è l’overround, spesso chiamato aggio. Non è il profitto garantito del '
  'bookmaker. Confrontarlo nel tempo aiuta a capire se è cambiata la valutazione della partita oppure anche '
  'il margine applicato.'),
 ('DROP: PREZZO E PROBABILITÀ NON SONO LA STESSA COSA',
  'Da 2,00 a 1,80 la quota perde il 10%. La probabilità implicita passa però dal 50% al 55,56%: +5,56 punti '
  'percentuali, prima del margine. Un drop misura il cambiamento del prezzo; non basta a dimostrare che oggi '
  'ci sia valore. Se il mercato ha già corretto molto la quota, il vantaggio iniziale potrebbe essere '
  'scomparso.'),
 ('LAMBDA: COME LEGGERE I GOL ATTESI',
  'Nella Poisson, lambda è il numero medio di gol atteso dal modello nel periodo considerato. Lambda casa '
  '1,6 e ospite 1,0 danno un totale medio di 2,6, sotto l’ipotesi di indipendenza. Non prevedono un '
  'risultato di 2,6 gol: descrivono una distribuzione di punteggi possibili. La qualità dipende da come '
  'stimiamo attacco, difesa e contesto.'),
 ('VALUE BET: IL PREZZO CONTA QUANTO LA SCELTA',
  'Con probabilità stimata del 55%, la quota equa teorica è 1/0,55 = 1,82 circa. A quota 2,00 il rendimento '
  'atteso teorico è 0,55 × 2 − 1 = +10%; a 1,70 diventa −6,5%. È la stessa previsione, ma cambia il prezzo. '
  'Il valore esiste soltanto se la probabilità stimata è attendibile: un errore del modello può '
  'cancellarlo.'),
 ('AUDIT: VERIFICARE IL MODELLO SENZA RISCRIVERE IL PASSATO',
  'Una verifica utile conserva prima del calcio d’inizio mercato, quota, probabilità e orario. Poi confronta '
  'quelle previsioni con gli esiti, includendo gli errori e separando primo tempo e finale. Scegliere '
  'soltanto a posteriori il mercato riuscito non misura la capacità predittiva. Per valutare un '
  'miglioramento servono nuovi dati, non solo quelli usati per correggere il modello.'),
 ('QUOTA VECCHIA, VALORE APPARENTE',
  'Il modello stima il 50% e mostra una quota 2,20 osservata due ore fa: il vantaggio teorico sarebbe +10%. '
  'Se ora il prezzo disponibile è 1,90, diventa −5%. Una scheda aggiornata non implica che ogni quota al suo '
  'interno sia nuova. Prima di interpretare il vantaggio confrontiamo l’orario del prezzo, il mercato esatto '
  'e l’offerta attuale.'),
 ('TOGLIERE IL MARGINE: UNA STIMA, NON LA VERITÀ',
  'Su due esiti a quota 1,80 e 2,10, le probabilità implicite sommano al 103,18%. Normalizzarle dividendo '
  'ciascuna per la somma dà circa 53,85% e 46,15%. È un metodo semplice per rimuovere proporzionalmente '
  'l’aggio, ma presuppone come sia distribuito il margine. La probabilità ottenuta resta una stima del '
  'mercato, non la probabilità vera.'),
 ('POISSON: DAI GOL ATTESI A UNA PROBABILITÀ',
  'Con un totale di gol modellato da una Poisson di lambda 2,6, la probabilità di superare 2 gol è 1 − P(0) '
  '− P(1) − P(2), circa 48,2%. La media di 2,6 non equivale quindi a una probabilità del 60% di Over 2,5. Il '
  'calcolo dipende dalle ipotesi: espulsioni, ritmi e dipendenza tra le squadre possono rendere il modello '
  'meno aderente alla gara.'),
 ('CASA E TRASFERTA: EVITARE MEDIE FUORVIANTI',
  'Una squadra può segnare molto in casa e poco in trasferta. Usare soltanto la sua media complessiva può '
  'sovrastimarne l’attacco fuori casa. Servono anche forza degli avversari e numerosità del campione: una '
  'serie contro difese deboli non vale automaticamente contro una difesa forte. Separare i contesti aiuta, '
  'ma pochi dati aumentano l’incertezza.'),
 ('PIÙ BOOKMAKER: CONFERME DAVVERO INDIPENDENTI?',
  'Un calo osservato su diversi operatori è più ampio di un movimento isolato, ma non tutte le conferme sono '
  'indipendenti: alcuni prezzi seguono gli stessi fornitori o operatori di riferimento. Conta anche la '
  'sequenza temporale. Un movimento persistente su più rilevazioni è diverso da dieci quote copiate quasi '
  'simultaneamente.'),
 ('RADAR BLU: MOVIMENTO FORTE, STATISTICA DEBOLE',
  'Un mercato può registrare un forte calo della quota mentre il modello resta prudente. Le due letture non '
  'sono necessariamente un errore: possono riflettere notizie recenti, limiti statistici o semplice domanda. '
  'Il Radar Blu segnala una dinamica da studiare; non trasforma automaticamente quel mercato in Gold. Serve '
  'capire la divergenza, non nasconderla.'),
 ('PERCHÉ IL BOOKMAKER CAMBIA LE QUOTE?',
  'Una quota può cambiare per nuove informazioni, puntate ricevute, gestione dell’esposizione o allineamento '
  'agli altri operatori. Formazioni, assenze e meteo possono incidere sulle stime. Dal solo drop non '
  'possiamo sapere quale causa abbia prevalso, né concludere che il bookmaker conosca un esito certo. Il '
  'movimento è un dato; la sua spiegazione va verificata.'),
 ('POCHI DATI: PERCHÉ LE STIME VANNO RIDIMENSIONATE',
  'Una media gol molto alta in quattro gare può dipendere dal caso o dagli avversari affrontati. Una tecnica '
  'statistica è avvicinare le stime più incerte a una media di riferimento, dando più autonomia ai campioni '
  'robusti. Non significa ignorare la forma recente: significa evitare che pochi risultati estremi dominino '
  'la previsione.'),
 ('CALIBRAZIONE: QUANDO UN 60% È CREDIBILE?',
  'Raggruppiamo molte previsioni registrate intorno al 60% e controlliamo quante si realizzano. Se il '
  'risultato osservato resta molto distante, il modello può essere troppo ottimista o prudente. La verifica '
  'va fatta anche per mercato e fascia di quota, con campioni adeguati. Una buona calibrazione non assicura '
  'profitto: conta anche il prezzo a cui si gioca.'),
 ('FORMAZIONI UFFICIALI: COSA CAMBIA DAVVERO?',
  'L’assenza di un attaccante non implica automaticamente meno gol: contano sostituto, sistema di gioco e '
  'avversario. Inoltre il mercato potrebbe aver già incorporato la notizia prima dell’annuncio ufficiale. '
  'Confrontare l’orario della notizia con quello del movimento aiuta a evitare di interpretare come nuova '
  'un’informazione già inclusa nella quota.'),
 ('INVERSIONE DELLA QUOTA: IL PRIMO SEGNALE REGGE?',
  'Una quota scende da 2,20 a 1,90 e poi risale a 2,10. Rispetto all’apertura è ancora più bassa, ma il '
  'movimento recente va nella direzione opposta. Guardare solo apertura e ultimo prezzo nasconde questa '
  'inversione. Per interpretarla servono rilevazioni intermedie, orari e confronto tra operatori: il '
  'percorso può contare quanto il punto di arrivo.'),
 ('QUOTA DI CHIUSURA E CLV',
  'Prendere 2,10 su un esito che chiude a 1,90 significa aver ottenuto un prezzo migliore rispetto alla '
  'chiusura; il rapporto 2,10/1,90 − 1 è circa +10,5%. È una misura di CLV sullo stesso mercato e con '
  'condizioni confrontabili. Non garantisce che la giocata vinca e la chiusura non è infallibile, ma su '
  'molte osservazioni può aiutare a valutare la qualità del prezzo.'),
 ('LIQUIDITÀ: LO STESSO DROP NON PESA SEMPRE UGUALE',
  'In un mercato poco liquido può bastare una puntata contenuta per spostare il prezzo. Un movimento analogo '
  'in un mercato più liquido può avere una dinamica diversa. Se non disponiamo dei volumi, però, non '
  'possiamo dedurli dalla sola quota. Per questo leggiamo ampiezza, persistenza e diffusione del movimento '
  'senza inventare quanto denaro sia entrato.'),
 ('SENSIBILITÀ: QUANTO CAMBIA LA STIMA SE CAMBIA LAMBDA?',
  'Nel modello Poisson, un piccolo cambiamento dei gol attesi può modificare sensibilmente la probabilità di '
  'un mercato. Se il vantaggio teorico sparisce appena riduciamo leggermente lambda, la proposta è fragile '
  'rispetto alle ipotesi. Confrontare scenari plausibili aiuta a distinguere un margine robusto da uno che '
  'dipende da una singola stima molto precisa.'),
 ('MERCATI CORRELATI: DUE SEGNALI NON SONO DUE CONFERME',
  'Over 2,5 e Over 3,5 della stessa partita dipendono dagli stessi gol: se si verifica il secondo, si '
  'verifica anche il primo. Non sono due prove indipendenti della qualità dell’analisi. Anche una combo va '
  'valutata con una probabilità congiunta: moltiplicare le probabilità dei singoli esiti è corretto solo '
  'quando si può assumere indipendenza.'))

def educational_message(day, slot=None):
    # Consecutive topics across both daily editions, starting 5 November 2026.
    index = day.toordinal()
    if slot is not None:
        anchor = datetime(2026, 11, 5).date().toordinal()
        index = anchor + 2 * (index - anchor) + (1 if slot == "16:45" else 0)
    title,body=EDUCATIONAL_THEMES[index%len(EDUCATIONAL_THEMES)]
    return (
      "📚 ARABSNIPER · UN MINUTO DI ANALISI\n\n"
      f"🔎 {title}\n\n{body}\n\n"
      "La rubrica educativa è indipendente dalla presenza di pronostici Gold/Platinum oggi.\n"
      "📊 Approfondisci sul sito: https://arabsniperbet.com\n"
      "📲 Condividi il canale: https://t.me/arabsniper_signals\n"
      "Solo maggiorenni · Le analisi non garantiscono risultati."
    )


def load_audit(day):
    repo=os.environ["GITHUB_REPOSITORY"]
    res=json.loads(subprocess.check_output(
        ["gh","api",f"repos/{repo}/actions/artifacts?per_page=100"],text=True))
    prefix=f"telegram-audit-{day}-"
    candidates=[a for a in res.get("artifacts",[]) if a["name"].startswith(prefix) and not a["expired"]]
    if not candidates:
        print("No audited report: do not publish unverifiable results")
        return None
    item=max(candidates,key=lambda a:a.get("created_at",""))
    path=Path("evidence-audit")
    path.mkdir(exist_ok=True)
    subprocess.check_call(["gh","run","download",str(item["workflow_run"]["id"]),
                           "-n",item["name"],"-D",str(path)])
    return json.loads((path/"report.json").read_text(encoding="utf-8"))


def main():
    now=datetime.now(ROME)
    slot=choose_slot(now,os.environ.get("GITHUB_EVENT_NAME"),os.environ.get("GITHUB_EVENT_SCHEDULE"),os.environ.get("RECOVERY_SLOT"))
    if not slot:
        print("Not an eligible scheduled execution; no publication")
        return
    name=f"telegram-evidence-{now:%Y-%m-%d}-{slot}-sent"
    if posted_before(name):
        print("Already sent this slot; no publication")
        return
    if slot=="audit":
        report=load_audit((now.date()-timedelta(days=1)).isoformat())
        message=audit_message(report) if report else None
    else:
        # Education always has an evergreen, accurate topic even when the live
        # private board has no qualified market. Avoid fragile 160 MB R2 reads
        # and never invent live signals or odds.
        message=educational_message(now.date())
    if not message:
        print("Verified inputs insufficient; skip publication")
        return
    token=os.environ.get("TELEGRAM_BOT_TOKEN","").strip()
    if not token:
        raise RuntimeError("Missing Telegram bot token in GitHub secrets")
    bot=telegram("getMe",token)
    if bot.get("username","").lower()!="arabsniperbot":
        raise RuntimeError("Unexpected bot")
    channel=telegram("getChat",token,chat_id=CHANNEL)
    if channel.get("type")!="channel" or channel.get("username","").lower()!=CHANNEL[1:]:
        raise RuntimeError("Unexpected channel")
    admin=telegram("getChatMember",token,chat_id=channel["id"],user_id=bot["id"])
    if admin.get("status")!="administrator" or not admin.get("can_post_messages"):
        raise RuntimeError("Missing posting permissions")
    sent=telegram("sendMessage",token,chat_id=channel["id"],text=message,
                  disable_web_page_preview="true")
    if sent.get("chat",{}).get("id")!=channel["id"]:
        raise RuntimeError("Wrong destination")
    Path("evidence-receipt.json").write_text(json.dumps({"slot":slot,"date":now.date().isoformat(),
                    "message_id":sent["message_id"]}),encoding="utf-8")
    with open(os.environ["GITHUB_OUTPUT"],"a",encoding="utf-8") as f:
        print(f"artifact={name}",file=f)
    print(f"Evidence published: https://t.me/arabsniper_signals/{sent['message_id']}")


if __name__=="__main__":
    main()
