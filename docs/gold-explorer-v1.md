# Isolamento del rilascio — aggiornamento 17 novembre 2026

La PR #220 è stata integrata in main al commit d5ff9b9b. Test PR e main superati; asset Pages verificati nel browser. Il backend inizialmente attendeva il mutex Gold occupato dalla Night programmata, partita alle 09:56 italiane.

La correzione separa il deploy dal mutex Gold. Tutti i writer Explorer (hook ordinario, CLI e import iniziale) acquisiscono un lease R2 esclusivo su `explorer/writer-lease.json`, con precondizioni atomiche If-None-Match/If-Match. Se occupato, l'hook rinvia soltanto Explorer senza attendere né compromettere Gold. Scadenza di tre ore superiore ai timeout dei job; rilascio condizionale che non può cancellare il lease di un successore. Test: contesa, gara atomica, crash/scadenza, vecchio writer, errore e isolamento degli originali.

Il precedente job Explorer ancora in coda viene annullato in modo mirato, verificando run ID, SHA e stato pending. Nessuna Night/Fast/Auditor viene annullata. Il nuovo rilascio mantiene backup, verifica SHA Worker, import dei soli archivi già esistenti, migrazione D1 e smoke online. I chunk locali di ogni giornata storica vengono rimossi dopo l'import per limitare lo spazio disco; gli originali remoti restano immutati.

Questa sezione aggiorna il precedente riferimento al mutex Gold. Nessuna nuova raccolta calcistica, nessun nuovo modello, nessuna modifica del cron Night.

---

# Aggiornamento operativo 17 novembre 2026

Bruno ha autorizzato merge, deploy e lancio (Meta: 40 € totali / 6 giorni), subordinati ai controlli e alla reale disponibilità online. Le precedenti indicazioni di attesa autorizzazione sotto sono superate.

La verifica reale su R2, D1 e Worker è riuscita. Il Worker online differisce da main: `scripts/explorer-worker-patch.py` aggiunge Explorer preservando byte per byte il codice precedente. Il codice reale modificato ha superato le suite auth, rinnovi ed Explorer. Il percorso usato dal client è `/api/auth/explorer/*`, già coperto dalla route Gold: nessuna nuova route DNS/Worker.

Rilascio: merge con messaggio `[release-explorer-v1]`. Il job una tantum nell'esistente workflow di test attende i test verdi, acquisisce il mutex già usato dagli archivi, salva il Worker in R2 privato `ops-backups/gold-explorer/`, applica le due tabelle personali, importa gli archivi esistenti e distribuisce solo la patch additiva. Attiva poi il flag Worker e verifica il rifiuto degli anonimi. Nessuna credenziale calcistica è assegnata al job. I workflow ordinari aggiornano Explorer dopo il salvataggio Gold; una variabile GitHub esplicita `GOLD_EXPLORER_ENABLED=false` disabilita la proiezione.

Rollback: disabilitare il flag Worker `GOLD_EXPLORER_ENABLED`, impostare la variabile GitHub a `false`, ripristinare il contenuto Worker dal backup privato e ripristinare gli asset Pages precedenti. Conservare tabelle personali e oggetti Explorer; non cambiare archivi Gold. Un errore durante il caricamento Worker/attivazione rimuove il nuovo flag e ripristina il precedente contenuto se non sono intervenuti altri deploy.

La verifica in sola lettura è documentata in Actions run 44049819676. Il rilascio e il collaudo online devono ancora essere registrati; questi test non equivalgono a una verifica di produzione completata.

---

# Gold Explorer V1 — integrazione e collaudo

Data: 17 novembre 2026. Branch `feature/gold-explorer-v1`, PR [#220](https://github.com/arabsnipertech-bet/arabsniper2/pull/220).

Il precedente checkpoint documentava una disconnessione. Il codice è stato ricostruito, salvato sul branch e sottoposto a collaudo. Questo documento descrive l'implementazione effettiva. **Nessun merge, deploy, modifica D1/configurazione produzione o lancio promozionale è autorizzato da questa consegna.**

## Architettura e audit

Lettura dei cinque feed Gold già prodotti da Night/Fast e degli archivi Auditor, proiezione in R2 `explorer/`, API privata nel Worker `stripe-checker`, interfaccia sulla stessa origine della Gold. Il modello V7 e i suoi collector non sono modificati.

Fonti individuate nel codice:

- `src/3appdays_runner.py` e workflow Night/Fast esistenti: produzione delle analisi, quota originale, successivi aggiornamenti. Nessun secondo avvio di questi processi.
- `matches[].multimarket.matrix.v7.markets`: universo analizzato e probabilità V7; `matrix.rows`: prove temporali/eseguibilità delle quote; `triad_commitments`/`triad`: identificazione ufficiali.
- `src/v7_archive.py`: validità degli snapshot tramite versione, fixture, kickoff, data pubblicazione e SHA256.
- `auditordata/prematch_DATE.json`, inclusi contenitori a parti verificati da `audit_storage`: pubblicazioni prematch originali in `analysis_updates`.
- `auditarchive/prematch_DATE_v7.json`: risultati già acquisiti, stato del match e pesi di liquidazione per mercato. Explorer non chiama il live né il provider risultati.
- `src/gold_storage.py`: feed privati R2 e checkpoint `state/current.json`. Explorer parte solo dopo il commit Gold; la sua eccezione viene contenuta.
- `workers/stripe-access/auth.mjs`, `worker.mjs`, `renewal.mjs`: cookie OTP, sessione di 24 ore e diritto Gold/prova esistente. `ACCESS_DB` e `GOLD_DATA` sono riutilizzati. Nessun prezzo, piano o durata Stripe cambia.

Il ramo era basato su `eb9abe79`; è stato confrontato con main `9547ed29`. Le differenze recenti di main osservate erano proiezioni pubbliche generate, non sorgenti da sovrascrivere. La PR conserva il lavoro di altre correzioni. Lo stato dei dati privati correnti non è certificato dall'audit dei sorgenti: l'ambiente ripreso non dispone di un checkpoint R2 autenticato completo. I collaudi end-to-end qui descritti usano dati di prova esplicitamente marcati, non quote o risultati inventati per la produzione.

## Matrice di compatibilità

| Funzione | Dato effettivamente utilizzato | Comportamento/limite |
|---|---|---|
| Tutti i mercati | `v7.markets` dello snapshot verificato | Nessun mercato aggiunto artificialmente |
| 1/X/2 FT | `result` nel catalogo V7 | Disponibile se presente nel feed |
| Over/Under PT, ST, FT | `total` e relativi periodi | Solo linee già calcolate e archiviate |
| GG/No Goal PT, ST, FT | `btts` | Probabilità originale, nessuna moltiplicazione |
| DNB, Asian totals, handicap | Catalogo `lateral_matrix.base_specs` | Visualizzati se presenti; liquidazioni parziali supportate |
| Combo | `market_scope.COMBO_SPECS` e altri mercati già nello snapshot | Probabilità congiunta V7 conservata, mai ricostruita |
| Doppia chance | Non inclusa nel catalogo base corrente | Nessuna promessa di disponibilità; leggibile solo se già archiviata |
| Apertura/prime quote/fonte | `v7.markets[].quote` e `matrix.rows` | Valori mancanti restano assenti; mai ricavati dal prezzo attuale |
| EV, EV_low, OCI, affidabilità | Indicatori V7 originali | Supporto statistico distinto da validazione e valore |
| Risultati | Archivio V7 `fixtures[].outcomes` | Solo stessa fixture e stesso kickoff; assenze non conteggiate come perse |
| Ricerche salvate | D1, account autenticato | Filtri riapplicati ai dati correnti |
| Preferiti | Snapshot canonico recuperato dal server | Quota al salvataggio immutabile, Fast successivo separato |
| Night/finale | Nuovi snapshot Explorer prospettici | Il passato non diventa retroattivamente una classifica Explorer |
| Vecchio storico | Pubblicazioni originali V7 verificabili | Sezione separata; nessuna attribuzione dei risultati a Explorer |

La copertura effettiva per partita dipende dal feed: un mercato previsto nel catalogo non implica che esistano quota utilizzabile, campione adeguato o risultato completo. La proiezione non risolve lacune della raccolta V7 aggiungendo chiamate.

## Funzioni implementate

Pagina `web/gold-explorer.html` e pannello laterale lazy-load in Gold, a tutta altezza su mobile. Dashboard; cinque classifiche; ricerca avanzata anche su mercati non raccomandati; filtri combinabili (campionato, squadra, mercato, quota, probabilità, EV/EV_low, OCI, supporto, categoria, ufficiali, tempo); Europe/Rome e cinque date di calendario. Il pulsante mostra altri dieci risultati. Classifiche diversificate per fixture, con accesso alle altre analisi dello stesso incontro.

Ricerche personali: creazione, nome, richiamo sui dati aggiornati, modifica e cancellazione. Preferiti persistenti e privati: massimo 500 per account, pagine da 20; 50 ricerche per account. Dettaglio: quote, fonte/date, Night/Fast, impronta forecast, motivi di attenzione, esito e punteggi PT/FT disponibili.

Storico 7/30/90 giorni o completo, universi distinti (tutti, validate, Top 10 certificate, ufficiali, alternative, preferiti). Statistiche: N, verificati, copertura, vinte/perse, hit rate, ROI teorico, EV prematch, Brier e calibrazione; aggregazione per campionato/mercato e confronto Night/finale. I filtri degli indicatori e del mercato si applicano anche ai campioni storici. Le tabelle indicano campioni separati, non dimostrano una superiorità causale.

## Criteri delle classifiche

Nessun modello o peso stimato nuovo. Si riutilizza l'ordinamento lessicografico V7: probabilità/EV_low/EV per Più Probabili; presenza della nicchia V7/EV/probabilità per le altre classifiche. Parità risolte deterministicamente per mercato e ID. Consigliate e Colpacci applicano `TRIAD_MARKETS` e `TRIAD_LIMITS` esistenti, inclusa la condizione Away del Colpaccio.

L'ammissione Explorer richiede quota utilizzabile, `supported=true`, tier V7, EV positivo ed EV_low non negativo. È un filtro prudenziale di valore e supporto già usato dalla logica V7; può escludere una selezione ufficiale di studio o una nicchia meno robusta, senza modificarla. Non è una nuova ottimizzazione statistica e non si rivendica un rendimento dimostrato. Le etichette Ufficiale, Alternativa, In osservazione e Non qualificata restano separate.

## Doppia certificazione e integrità

Il limite è **kickoff della singola partita meno 300 secondi**, confrontato in UTC. La tolleranza di cinque minuti è un margine operativo esplicito contro aggiornamenti a ridosso dell'avvio, non una soglia ottimizzata su esiti. Il congelamento viene materializzato dalla prima pubblicazione esistente successiva alla soglia. Nessun job aggiuntivo si sveglia al minuto del kickoff. Anche dopo un'interruzione di più giorni il catalogo conserva i riferimenti ancora da congelare.

Il primo Night verificato resta originale; l'ultimo Fast pubblicato entro il limite e con quota utilizzabile può diventare riferimento finale. Senza Fast valido si conserva Night con indicazione esplicita. Un forecast precedente all'inizio del job non viene falsamente certificato come nuova scansione Fast. Stato avviato, rinviato o cancellato e risultati già presenti escludono la partita dalla proiezione prematch. Un kickoff diverso crea un episodio distinto; i risultati non sono trasferiti tra episodi.

Oggetti e pagine certificati hanno chiave SHA256 del contenuto, sono riutilizzabili idempotentemente e verificati dall'API. Gli indici giornalieri possono avanzare per appendere nuove versioni/risultati; gli oggetti certificati e i dati Gold originali non vengono modificati. I Top 10 conservano gli ID nell'ordine registrato per ogni versione temporale. Il campione Top 10 aggregato usa l'appartenenza della partita alla versione del suo riferimento; le singole versioni restano consultabili separatamente.

Le quote recenti rispettano le finestre V7 (6 ore, 1 ora nelle ultime 3 ore); le quote stabili rispettano il limite originale di 120 ore. La rilettura non rinnova la scadenza. Timer locali rimuovono partite iniziate e quote scadute senza polling.

Per le ufficiali lo storico usa la quota del commitment. Se il commitment non conserva la probabilità originale e il timestamp differisce dallo snapshot, probabilità/EV/Brier originali sono indisponibili: non si sostituiscono con valori successivi. ROI a puntata unitaria solo su quote documentate; Brier/calibrazione solo su esiti binari con probabilità. Rimborso e liquidazioni parziali non diventano vittorie binarie.

## Sicurezza, chiamate e prestazioni

Ogni `/api/explorer/*` verifica feature flag, origine, sessione OTP e diritto Gold tramite la funzione di accesso esistente. POST richiede stessa origine, JSON entro 8 KiB e limite 30 scritture/minuto. Le query personali sono parametrizzate e account-scoped. Il client invia soltanto ID e hash per il preferito: quota/probabilità/email fornite dal client non possono sostituire il dato canonico.

R2: letture/scritture del projector confinate a `explorer/`; nessuna scrittura nei feed/calibrazioni/archivi originali. API Worker legge soltanto gli oggetti derivati e D1. Il codice Explorer non importa client calcistici né esegue fetch a provider. Il controllo dell'abbonamento riutilizza l'infrastruttura Stripe/cache già esistente; non è una chiamata calcistica.

Pagine da 250 righe e caricamento di quattro pagine per gruppo; rendering progressivo. Filtri/ranking locali senza richieste di rete. Preferiti paginati. Checksum degli archivi riutilizzati dal manifest Gold già calcolato; import storico ordinario limitato a una giornata modificata per pubblicazione. L'import completo opzionale legge soltanto gli archivi restaurati. Gli errori Explorer non fanno fallire la pubblicazione Gold già conclusa.

Limite: caricando lo storico completo il browser conserva in memoria le pagine richieste. Il costo cresce con l'archivio; usare i periodi limitati per la consultazione ordinaria. Non è ancora misurato il costo R2 della proiezione sul checkpoint privato completo corrente. Prima dell'attivazione controllare durata e dimensioni con quel checkpoint; se necessario lasciare il flag disabilitato, senza toccare i processi di raccolta.

## Collaudo ed evidenze

- Test dedicati Python: 11 superati, con socket outbound negati, validità/impronte, cutoff, stati match, idempotenza, congelamento dopo interruzione, fallback, Fast inutilizzabile, mancata mutazione degli originali, paging, scope storage, risultati esistenti e storico senza classifiche retroattive. Eccezione Explorer iniettata durante pubblicazione Gold: checkpoint e feed Gold preservati.
- Test Node dedicati: 7 superati. Sessione reale D1/cookie, accesso attivo/prova simulato attraverso la stessa funzione di autorizzazione, scadenza/revoca, isolamento CRUD, CSRF, limiti, favoriti canonici, checksum, filtri, ranking e statistiche. Fetch outbound vietato durante questi test.
- Test auth esistenti: 8 superati. Nessuna modifica ad autenticazione/prezzi.
- Prima CI: tutti i 877 test Python e tutti i file Node superati; tutti i browser Gold/Publisher esistenti superati. Il browser Explorer è arrivato a filtri, CRUD, storico, quattro viewport e stati di errore, poi ha rilevato una sessione Gold non simulata nel test del pannello; corretta la fixture di sessione e rilanciato.
- La suite locale completa ha eseguito 877 test con una dipendenza `cryptography` assente; non dichiarata verde. In CI la dipendenza è installata e lo stesso test è passato.
- Benchmark locale del solo filtro/ranking: 10.000 righe sintetiche, 10 ripetizioni, circa 75 ms per iterazione in questo ambiente. Non è una misura delle latenze produzione.
- CI del commit `5693c277`: [run 44044240731](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44044240731) interamente verde: **879 test Python**, tutti i file Node, tutti i browser Gold/Publisher ed Explorer. Browser Explorer: 1.250 righe, quattro viewport (1440/768/390/360 px), zero errori JavaScript, zero chiamate calcistiche, nessuna richiesta durante i filtri. Anche la CI Telegram/Stripe è verde. Le successive rifiniture e la consegna sono sottoposte a nuova CI.
- Screenshot QA verificati visivamente: [desktop](qa/explorer-desktop.png), [tablet](qa/explorer-tablet.png), [smartphone](qa/explorer-mobile.png), [360 px](qa/explorer-small-mobile.png). Dati sintetici, non materiale promozionale.

I test di prova gratuita e abbonamento usano diritti controllati in isolamento; non sono login eseguiti su account reali in produzione. Il vincolo zero provider è verificato sul codice e sulle esecuzioni strumentate, non attraverso una nuova scansione calcistica.

## Distribuzione proposta — solo dopo approvazione

1. Verificare CI e diff aggiornati, assenza di conflitti e confronto con il nuovo main. Conservare versione precedente del Worker e deployment Pages. Nessun dispatch Night/Fast per Explorer.
2. Applicare `workers/stripe-access/explorer-schema.sql` al D1 `ACCESS_DB` esistente (due tabelle e un indice, istruzioni idempotenti). Conservare le tabelle in caso di rollback per non perdere preferiti/ricerche.
3. Generare con `python workers/stripe-access/build_production.py`, distribuire il bundle sul Worker esistente, conservando tutti i binding/segreti/route precedenti. Aggiungere esclusivamente la route `arabsniperbet.com/api/explorer/*` allo stesso Worker; verificare dominio canonico e `AUTH_ORIGIN`.
4. Verificare `AUTH_ENABLED=true`, `ACCESS_DB`, `GOLD_DATA`. Il nuovo `GOLD_EXPLORER_ENABLED` resta inizialmente disabilitato sia nel Worker sia nelle variabili GitHub.
5. Merge autorizzato della PR e pubblicazione Pages con le nuove risorse. URL previsto: `https://arabsniperbet.com/gold-explorer.html`. Non considerarlo online sulla base della sola presenza del link.
6. Prima del flag, eseguire la proiezione di verifica su copia privata isolata del checkpoint e bucket di staging. `python src/gold_explorer.py --seed-current --complete-backfill` legge solo i file Gold/archivi già restaurati. Non esegue scan, non fabbrica certificati Night/Fast pregressi. Se non disponibile un ambiente isolato, la verifica di integrazione resta pendente.
7. Dopo controllo dei dati reali, abilitare il flag GitHub per agganciarsi alla prossima pubblicazione Night/Fast già prevista; nessuna scansione dedicata. Abilitare il flag Worker quando l'indice è disponibile. L'import storico continua progressivamente; il catalogo espone le giornate ancora da importare.
8. Smoke test reale Gold pagato, prova valida, utente senza diritto e due account distinti; preferito su due dispositivi; ricerche CRUD; completezza campi/risultati; quote scadute; pannello e pagina; immutabilità Night/Fast. Confrontare feed/triade e contatori provider prima/dopo consultazione/filtri. Verificare che Night/Fast/Auditor/Publisher continuino normalmente.
9. Solo dopo esito positivo comunicare URL attivo e chiedere approvazione ai contenuti social.

Rollback: disabilitare i due flag Explorer, ripristinare Worker e Pages precedenti se necessario e rimuovere soltanto la nuova route. Conservare `explorer/` e tabelle personali. Nessun rollback dei dati Gold, dei forecast certificati, dei pagamenti o del motore V7. Il flag disabilitato rende l'API 503 e interrompe la proiezione, lasciando intatti i workflow esistenti.

## Lancio successivo

La fase promozionale viene preparata e approvata **dopo** la verifica online, nel repository `arabsnipertech-bet/arabsniper-growth`. Nessuna automazione o campagna è stata creata.

Bozza editoriale subordinata al rilascio:

> ARABSNIPER GOLD EXPLORER — LA NUOVA DIMENSIONE DELL'ANALISI CALCISTICA
>
> Esplora i mercati analizzati da Gold, confronta probabilità e quote, salva le tue ricerche e ritrova le selezioni nei preferiti. Consulta risultati e statistiche con riferimenti Night e prematch separati. La triade ufficiale V7 resta sempre riconoscibile.
>
> Incluso nel tuo accesso Gold, anche durante una prova gratuita valida. Scopri Explorer dalla pagina Gold. Analisi probabilistiche: nessuna garanzia di vincita.

Prima di pubblicare: verificare ogni funzione nominata e il link attivo; acquisire screenshot autentici senza dati personali; realizzare grafica Telegram/Facebook e creatività Meta coerenti con tali schermate; CTA alla prova Gold secondo i requisiti di ammissibilità esistenti (nessuna nuova prova promessa a chi non ne ha diritto). Definire obiettivo, pubblico maggiorenne, posizionamenti, budget e durata, verificare le policy Meta allora applicabili e chiedere l'approvazione specifica. Le immagini QA non sostituiscono screenshot autentici del servizio online.
