# ArabSniperBet V7

## Baseline

Il riferimento di avvio è **5632c78ba0269a4c157e8fc6829730949d40fb69**, fissato nel ramo **rollback/pre-v7-2026-11-09**. Il Fast [42634556160](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/42634556160) ha ripristinato 27.079 oggetti unici verificati e ha salvato 27.095 file di stato privato e sette feed Gold.

| File della baseline | SHA-256 |
| --- | --- |
| src/lateral_matrix.py | 7434d29fcb1f67f5e7e35cb9a92df46d652311b48896a8eec21eb35e980fde42 |
| src/opportunity_policy.py | 457e51bc2f9d85269c0b24105cad80f23c82ec4194db2241c6c9c6ffe36b4394 |
| src/adaptive_calibration.py | e2b457ed455af7243e44a14d2028af5b3246acc87d5b96e65c82db4263ceb097 |
| data/arab_calibration_weights.json | 0d866bbfe2c5816123d4ed9c5dd4a1a5d69608ea46dbc1b937d12b70677b0d3e |

## Calcolo e selezioni

Le intensità indipendenti usano il profilo robusto esistente di attacco e difesa, i riferimenti del campionato e gli xG disponibili. Il secondo tempo usa anche gol fatti e subiti da punteggi PT e FT della stessa gara storica. Le intensità PT e ST si ripartiscono il volume FT. Le intensità FT per squadra sommano quelle dei due periodi.

La proiezione fair stima volume e ripartizione usando Over/Under e tutta la partizione 1X2. Un aggiustamento comune delle quote relative casa/ospite allinea la ripartizione sui due periodi. Il peso V7 interviene una sola volta sulle quattro intensità, tra stima statistica e proiezione fair. Le combo fra PT e FT sommano la probabilità dei punteggi PT moltiplicata per la probabilità condizionata dei gol ancora necessari nel secondo tempo.

Il valore atteso è il rendimento unitario teorico calcolato dalle frazioni di vincita, perdita e rimborso. Lineare e Alternativa richiedono un prezzo eseguibile, supporto statistico e convenienza propria. Radar Conversione osserva il GG PT che sale e l'Over 2,5 FT che scende. Radar Sorprese osserva GG PT ad alta quota con OCI molto negativo e segno 2 ad alta quota in rialzo. Over 3,5 PT può entrare come Colpaccio con supporto del proprio mercato. I segnali V7 sono sperimentali. Gold e Platinum usano i criteri ufficiali esistenti. Le osservazioni su OCI, variazione, persistenza, aggio e concordanza sono misurate per mercato.

## Confronto cronologico

Lo sviluppo dal 26 ottobre al 3 novembre ha selezionato il peso 0,80 sulla griglia 0,50–0,80. Il blocco dal 4 al 6 novembre comprende 163 partite e 1.304 coppie partita/mercato. È un replay sullo stesso storico del dossier. Dove mancavano i lambda grezzi recenti, il replay ha ricavato le intensità dai tassi di attacco e difesa del profilo della stessa analisi, omettendo la correzione H2H. Dove il workbook esponeva soltanto le tre probabilità fair 1X2, ha usato il vettore normalizzato per il replay. Il codice V7 di scansione accetta il vettore dello stesso operatore.

| Stima | Brier | Log-loss |
| --- | ---: | ---: |
| Poisson originale o ricavato dai profili | 0,203946 | 0,596270 |
| V5 registrata | 0,203550 | 0,595335 |
| V7 con peso 0,80 | 0,193074 | 0,569033 |
| Fair | 0,192309 | 0,567328 |

Sulle 235 favorite a quota sotto 1,50 nel replay, la frequenza osservata è 71,1%, il vecchio modello stima 46,1%, V7 67,1% e il fair 72,2%. Il fair resta il riferimento e la promozione del candidato richiede evidenza prospettica.

## Archivi, calibrazione e operatività

Il journal privato conserva i mercati canonici, i prezzi disponibili, la prima e l'ultima osservazione per fonte e la sequenza delle scansioni. Gli esiti sono scritti in un file separato per la data italiana di kickoff, con riferimento SHA-256 alla fotografia V7. Il registro distingue esiti ai 90 minuti, supplementari, rigori, sospensioni, rinvii, annullamenti e partite in attesa.

La ricalibrazione V7 usa 28 giorni ed emivita 14. Confronta soltanto esiti osservati entro il cutoff. Tiene separati almeno otto giorni di sviluppo e cinque giornate successive di verifica, rispettivamente con almeno 100 e 80 partite. Un passo entro 0,05 viene applicato al candidato V7 quando migliora Brier e log-loss rispetto al precedente, al fair e a V5.1. Il report separa aggiornamento e miglioramento e registra i confronti per mercato, fasce di probabilità, selezioni escluse, ruoli V7 e rendimento teorico per giornate. Formule e soglie strutturali restano soggette a revisione manuale.

| Ora Europe/Rome | Workflow |
| --- | --- |
| 00:01 | Consolidamento V7 con indice privato e partite ancora in attesa |
| 00:30 | Auditor e ricalibrazione con checkpoint |
| 02:00 | NightScan su cinque giornate |
| 04:30 | Finestra di recupero Auditor |
| 06:00–23:00 | Fast all'ora |
| 12:30–22:30 | Prezzi delle partite imminenti se il Fast è concluso e la quota API è sufficiente |

Fast, NightScan, Auditor e manutenzione V7 condividono il mutex di scrittura. La manutenzione dei prezzi ripristina solo i file privati necessari e conserva l'indice storico. I workflow social conservano i propri cicli e leggono il formato Gold compatibile.

## Verifica e rollback

La pubblicazione richiede suite Python e Node, anteprima browser desktop/mobile, prova del feed privato con una sessione autorizzata e riscontro dello stato Cloudflare. La promozione del motore ufficiale richiede giornate acquisite dopo il freeze.

Per ripristinare la produzione, fare revert del merge V7 su main e pubblicare il nuovo main mediante il workflow Cloudflare. Usare git revert -m 1 per il merge ordinario e git revert per lo squash. Il revert conserva i successivi commit dei feed pubblici. Conservare il manifest R2 corrente e gli archivi V7. Se serve correggere un feed, recuperare soltanto i relativi oggetti dal manifest verificato precedente sotto il mutex. Verificare sessione Gold, archivio e un ciclo Fast dopo la pubblicazione.
