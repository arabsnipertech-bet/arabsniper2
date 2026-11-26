# Verifica V7 prima del weekend — 26 novembre 2026

La priorità emersa dai dati è completare i risultati dello storico recente.
Il confronto attuale non giustifica nuove soglie o sostituzioni automatiche di
mercato. V7 conserva il peso mercato 0,80, le lambda, i gate, i ruoli e il
selettore esistenti. Gold ed Explorer mantengono l'interfaccia attuale.

## Perimetro verificato

Sette giornate chiuse, 19–25 novembre, da un unico manifest privato verificato.
Il cutoff del rapporto è il 26 novembre alle 08:00 italiane. Ogni file e parte
compressa è verificato tramite checksum; nessuna fotografia viene riempita con
dati futuri. Il rapporto completo pubblica tutte le celle predefinite, comprese
quelle sfavorevoli, con 4.000 repliche bootstrap per fixture e giornata sulle
configurazioni registrate. Le configurazioni diverse restano separate.

| Data | Gare archiviate | Liquidate | In attesa | Rinvii | Void |
|---|---:|---:|---:|---:|---:|
| 19 novembre | 20 | 20 | 0 | 0 | 0 |
| 20 novembre | 84 | 75 | 1 | 8 | 0 |
| 21 novembre | 292 | 278 | 0 | 13 | 1 |
| 22 novembre | 187 | 172 | 0 | 15 | 0 |
| 23 novembre | 46 | 19 | 27 | 0 | 0 |
| 24 novembre | 60 | 20 | 40 | 0 | 0 |
| 25 novembre | 65 | 20 | 45 | 0 | 0 |
| Totale | **754** | **604** | **113** | **36** | **1** |

Le 112 attese del 23–25 novembre limitano la rappresentatività dei risultati
recenti e delle nuove coorti shadow. Una partita in attesa non è una persa.
Un risultato FT disponibile non implica automaticamente un PT disponibile.

Il log dell'Auditor `45723190302` registra il fallimento della lettura della
cache live, seguito dal recupero limitato a 20 ID. Il blocco V7 è stato salvato;
questo certifica gli output disponibili, non la completezza di tutti gli esiti.
Il successivo recupero ha elaborato il 9 novembre mentre restavano aperte
giornate molto più recenti: i blocchi mai tentati precedevano quelli già tentati.

## Risultati già presenti nella cache

Una verifica separata alle 08:28 italiane legge esclusivamente il Worker live,
senza richiamare API-Football e senza scritture. Non è un input retroattivo al
backtest delle 08:00.

| Data | Attese / PT mancante | Finali PT+FT già disponibili | Void o rinvio confermato | Ancora senza conferma |
|---|---:|---:|---:|---:|
| 22 novembre | 1 PT mancante | 1 | 0 | 0 |
| 23 novembre | 27 | 26 | 1 | 0 |
| 24 novembre | 40 | 36 | 4 | 0 |
| 25 novembre | 45 | 34 | 4 | 7 |

Sono quindi recuperabili dalla cache 96 finali delle gare in attesa, nove stati
amministrativi e un PT mancante. Gli altri sette casi restano non confermati.
Il singolo caso del 20 novembre è fuori dal perimetro della cache recente di
questa riconciliazione e resta distinto.

## Auto-miglioramento e qualità delle proposte

L'ultimo rapporto di apprendimento comprende 1.126 fixture. Il controllo
cronologico usa 427 fixture di training e 503 di holdout; le righe dei diversi
mercati sulla stessa partita non sono osservazioni indipendenti.

Il peso mantenuto è **0,80**. Il candidato scelto è ancora 0,80; la variante
0,75 non migliora il training. L'aggiornamento riporta `applied=false` e
`baseline_retained`. Sul holdout il Brier V7 è 0,177150 e quello fair 0,177688:
la piccola differenza puntuale non dimostra una superiorità stabile. Non si
introduce una modifica produttiva per inseguire l'ultima settimana.

Tra le scelte ufficiali liquidate nel campione, la Più Probabile centra
339/544, la Consigliata 199/531 e il Colpaccio 141/545. Sono ruoli, prezzi e
probabilità differenti: queste frequenze non ne stabiliscono una graduatoria
di valore economico. Il ROI resta un controllo, non sceglie soglie o mercati.

Le vecchie scelte ufficiali non conservano sempre la P originaria: nel campione
liquidato solo tre scelte per ruolo hanno la probabilità originale verificabile.
La probabilità dell'ultimo snapshot è invece disponibile per l'audit dei mercati.
I due campioni non vengono confusi. Il nuovo protocollo del 25 novembre
migliora la tracciabilità prospettica; i dati mancanti del passato restano mancanti.

## Over 1,5 PT e GG FT

Le coppie con entrambi i prezzi utilizzabili, supporto registrato e stesso
operatore restano piccole e appartengono a configurazioni diverse:

| Configurazione | Coppie liquidate | Over 1,5 PT centrati | GG FT centrati |
|---|---:|---:|---:|
| `435a2875…` | 12 | 6 | 6 |
| `bcc5b484…` | 6 | 4 | 4 |
| `233686dd…` | 2 | 0 | 0 |
| `71f30a67…` | 13 | 3 | 6 |

Le probabilità e i prezzi dei due eventi sono diversi. Il Brier appaiato è
confrontato solo con il fair dello stesso evento, mai sottraendo direttamente
il Brier del GG a quello dell'Over. Anche il braccio shadow GG FT nella
Consigliata mostra direzioni diverse tra configurazioni. Non emerge una regola
generale per sostituire l'Over con il GG o per forzare più Over in Explorer.

I nuovi osservatori «GG FT squadra debole» e «2 FT grezzo/mercato» hanno ancora
una sola giornata prospettica e pochissimi esiti valutabili nel rapporto
iniziale. Il bootstrap per giornata non è disponibile con un solo cluster.
Restano in shadow, con zero promozione automatica.

## Interventi mirati

1. La lettura della cache conserva il percorso esistente e aggiunge un unico
   fallback con lo stesso trasporto del watchdog live: GET sul medesimo endpoint,
   timeout e dimensione limitati, nessun refresh e nessuna richiesta al provider.
   Restano le verifiche di identità, kickoff, origine, istante e score a 90 minuti.
2. Il recupero dà precedenza alle giornate incomplete entro la finestra di
   quattro giorni della cache. Restano un solo blocco storico per ciclo,
   il limite di 20 ID, le finestre protette e il mutex già usato da Fast/Night.
3. Una riconciliazione una tantum riutilizza i risultati già disponibili per
   il 22–25 novembre. Non ha credenziali API-Football; protegge byte e SHA delle
   fotografie, kickoff, istanti, pesi e feed Gold prima di pubblicare gli esiti.
   Non addestra, non pubblica proposte, non aggiorna proiezioni promozionali
   e non invia messaggi. Gli esiti non verificabili restano in attesa.

La riparazione rende più completi i dati per il prossimo ciclo ordinario di
valutazione. Non promette un aumento immediato delle vincite e non certifica
nuove regole di selezione sulla base di campioni ancora incompleti.

## Evidenza riproducibile

- Primo rapporto in sola lettura: run `45773939454`.
- Rapporto con verifica della cache e correzioni: run `45775544346`.
- Il JSON completo conserva manifest, SHA dei file, tutte le celle, intervalli,
  calibrazione, Brier/log-loss, composizione e controlli ROI.
- L'esito della riconciliazione viene allegato al workflow del merge come
  `cached-v7-reconciliation`: i conteggi disponibili sopra non sono presentati
  come già pubblicati prima di quel controllo.
- Test dedicati verificano cache primaria/fallback, assenza di risultati
  inventati, PT zero contro PT mancante, timestamp futuri, priorità recente,
  integrità dei forecast e divieto di modificare pesi o feed.

## Riconciliazione: verifica e ripresa

Il primo blocco del run `45777948544` ha pubblicato il 25 novembre alle 09:04
italiane: 54 finali contro i 20 iniziali, quattro rinvii e sette gare ancora
in attesa. Sono 34 nuovi finali verificati. Il controllo ha preservato le
fotografie originali e non ha modificato pesi o feed Gold.

Il limite complessivo di 25 minuti ha interrotto la ricostruzione completa del
24 novembre prima della pubblicazione; non si considera concluso il recupero
delle altre date. La ripresa elabora una sola giornata per job, in sequenza
e sotto il mutex ordinario dei writer. Ogni giornata salva autonomamente il
rapporto; una verifica iniziale su ricevute e archivio compatto evita di
ricostruire i ledger se la cache non aggiunge esiti. Il tempo di osservazione
da solo non è un nuovo risultato. Un PT pari a zero può invece completare un
PT mancante. Conflitti, orari futuri e fixture diverse non attivano il recupero.

Le stesse protezioni impediscono scritture ai forecast, alle probabilità, ai
pesi e ai feed Gold. La ripresa non addestra e non promuove varianti shadow.

## Verifica conclusiva dopo il recupero — 26 novembre, 10:53 italiane

La riconciliazione [45786556834](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/45786556834)
è terminata con successo per tutte e quattro le date. Sono stati salvati
**96 nuovi finali e nove rinvii**; il PT mancante del 22 novembre è completo.
Le fotografie prematch, le P, le quote fissate, gli istanti, i kickoff e gli
SHA sono preservati. Nessun fitting, nessuna scrittura a pesi o feed Gold,
nessuna chiamata aggiuntiva ad API-Football e nessun invio social.

| Data | Finali nel ledger | Rinvii | In attesa |
|---|---:|---:|---:|
| 22 novembre | 172 | 15 | 0 |
| 23 novembre | 45 | 1 | 0 |
| 24 novembre | 56 | 4 | 0 |
| 25 novembre | 54 | 4 | 7 |

Il censimento 19–25 novembre contiene 754 registrazioni gara/data: 700 finali,
45 rinvii, un void e otto attese, contro le 113 iniziali. Non sono 754
osservazioni indipendenti: il confronto esclude 37 fixture_id con kickoff
conflittuali tra archivi. Gli altri raggruppamenti restano per fixture, data
e configurazione. Il 22 novembre ha FT e PT disponibili per tutti i suoi
172 finali. Il caso del 20 novembre resta una delle otto attese.

### Le ultime tre riprogrammazioni

Il controllo delle 10:53 rileva tre ulteriori rinvii del 25 novembre nella
cache Worker. Sono osservati alle **10:36:53 italiane**, dopo il passaggio
sul 25 acquisito alle 09:53:41. Il kickoff della ricevuta e quello della
fotografia coincidono come istante: cambia solo la rappresentazione UTC/CET.
Il provider colloca ora queste gare al 29 novembre. Queste tre chiusure
amministrative sono disponibili al successivo recupero ordinario; non sono
nuovi finali né perdite. Il ledger conserva otto attese al suo cutoff,
mentre cinque casi restano senza risoluzione disponibile: quattro del 25
e uno del 20. Non si aggiorna retroattivamente la fotografia del recupero.

### Over 1,5 PT e GG FT dopo i nuovi esiti

Il secondo rapporto usa cutoff **10:53:12** del 26 novembre, distinta dal
rapporto iniziale delle 08:00. Legge le stesse fotografie e applica le stesse
formule, celle e 4.000 repliche bootstrap, senza cambiare pesi o gate.

| Configurazione, stesso operatore | Coppie liquidate | Over 1,5 PT centrati | GG FT centrati |
|---|---:|---:|---:|
| `435a2875…` | 13 | 6 | 7 |
| `bcc5b484…` | 6 | 4 | 4 |
| `233686dd…` | 9 | 4 | 4 |
| `71f30a67…` | 26 | 10 | 15 |

Il GG centra più eventi in due configurazioni e pareggia nelle altre due,
ma ha probabilità iniziali e prezzi diversi. Questi conteggi non stabiliscono
quale scelta offra più valore. Il Brier resta appaiato al fair dello stesso
evento, mai confrontato direttamente fra i due esiti diversi.

Nella configurazione recente `71f30a67…`, su due giornate, il modello migliora
il proprio fair sugli Over PT e su altre code, ma rimane una sovrastima
media della frequenza osservata. La tabella comprende anche le celle
sfavorevoli e incerte; valori negativi del delta Brier favoriscono V7.
Si tratta di tutti i contratti del gruppo, **non del solo sottoinsieme Gold**.

| Mercato | n | P V7 media | Frequenza osservata | Delta Brier V7−fair | IC 95% per fixture |
|---|---:|---:|---:|---:|---|
| 1 FT | 90 | 42.1% | 46.7% | -0.002800 | [-0.010026; +0.003816] |
| 2 FT | 90 | 35.2% | 32.2% | +0.005233 | [-0.000751; +0.011160] |
| GG PT | 89 | 20.7% | 11.2% | -0.006774 | [-0.010130; -0.003329] |
| Over 1,5 PT | 89 | 37.2% | 28.1% | -0.005949 | [-0.010483; -0.001567] |
| Over 2,5 PT | 89 | 14.6% | 5.6% | -0.005275 | [-0.007377; -0.003110] |
| Over 1,5 ST | 89 | 49.1% | 47.2% | -0.001554 | [-0.005910; +0.002870] |
| GG FT | 89 | 55.3% | 49.4% | +0.001830 | [-0.003867; +0.007800] |
| Over 2,5 FT | 89 | 56.1% | 50.6% | +0.002508 | [-0.000905; +0.005841] |
| Over 3,5 FT | 89 | 34.5% | 23.6% | -0.004559 | [-0.008591; -0.000413] |

Questo è un segnale da replicare, non una superiorità prospettica consolidata:
il bootstrap per giornata ha solo due cluster. Non giustifica un aumento
automatico degli Over PT, una sostituzione con GG FT o nuove soglie live.
Il peso mercato resta 0,80. Il rapporto di apprendimento preesistente resta
distinto: non è stato rifatto durante il recupero.

### Nuovi osservatori, prezzo e proposte scartate

La configurazione nuova con prezzi eseguibili ha 19 fixture, tutte del
25 novembre. La coorte «GG FT squadra debole» ha sette casi, sei valutabili
contro il fair; il delta Brier è +0,000157, con IC per fixture
[−0,035693; +0,039961]. Non prova un vantaggio né sostiene un blocco rigido.
La divergenza grezzo/mercato su 2 FT ha tre casi, due valutabili: delta Brier
V7−fair +0,010343, grezzo−fair +0,124859. Due esiti su una giornata non
giustificano regole contro gli sfavoriti. Per entrambe le coorti manca
ancora il bootstrap su più giornate indipendenti.

Fra i GG deboli risultano una vincente su tre scelte ufficiali liquidate
e una vincente su tre escluse valutabili. Un blocco rigido avrebbe quindi
tolto anche una scelta ufficiale corretta. Le tre divergenze 2 FT sono
già escluse; le due liquidate sono perdenti. Non dimostrano un miglioramento
delle scelte live introducendo un altro blocco. Ruoli e selettore restano invariati. Le 232 righe escluse con EV positivo comprendono 57 vincenti, ma
non sono tutte selezioni eseguibili che superano tutti i gate: non diventano
automaticamente «vincite perse». I conteggi sono diagnostici, non una regola
di promozione. Il ROI resta esclusivamente un controllo.

L'audit del prezzo registra 11 confronti di eventi equivalenti e un solo
miglioramento. Non avviene alcuna sostituzione automatica di mercato; le
quote AH con rimborso o pagamento parziale restano escluse dall'equivalenza
binaria.

### Prove e pubblicazione integrale delle celle

- Ripresa operativa: [PR 271](https://github.com/arabsnipertech-bet/arabsniper2/pull/271).
- Secondo rapporto in sola lettura: [run 45796082467](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/45796082467).
- [Tutte le 36 celle e i nuovi osservatori](weekend-quality-20261126-cells.json),
  con log-loss, calibrazione, intervalli per fixture/giornata, prezzi,
  configurazioni, scelte escluse, coverage e riferimenti SHA. Nessuna cella
  corrente è eliminata perché sfavorevole. Il riepilogo shadow precedente
  al recupero rimane nel suo rapporto di apprendimento e nell'artefatto completo.
- Originale e rapporto successivo conservano cutoff e sorgenti distinti.
  La P originaria verificabile sale da tre a 17 scelte liquidate per ruolo;
  le altre probabilità storiche mancanti non vengono riempite.
- Collaudo completo: 1.121 test Python, 21 file Node e sei suite browser
  desktop/mobile. Gold ed Explorer pubblici rispondono; gli asset Explorer
  coincidono con quelli verificati. La Gold differisce solo per la normale
  protezione Cloudflare dei due link email.

La correzione operativa è rendere affidabile lo storico che alimenta le
valutazioni. I segnali quantitativi nuovi restano in verifica prospettica;
non si altera il selettore che sta già servendo Gold ed Explorer.
