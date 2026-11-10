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

## Attivazione V7 per Gold, Platinum e Studio

La revisione `v7-unified-v1` usa una distribuzione V7 per tutti i mercati prematch. L’audit conserva Poisson originale, stima calibrata, fair e V5.1 dove disponibile.

| Slot | Probabilità minima | Quota | EV | EV prudente |
|---|---:|---:|---:|---:|
| Più probabile | 45% | 1,55–2,15 | > 0 | ≥ 0% |
| Consigliata | 32% | 2,15–3,50 | > 0 | ≥ 0% |
| Colpaccio | 16% | ≥ 3,50 | > 0 | Verificato nei requisiti del livello assegnato |

Il risultato 2 nel Colpaccio richiede quota da 4,50 e movimento in salita. Ogni slot conserva mercato, fonte, quota, probabilità, EV, livello, orizzonte e nicchia. Uno slot resta vuoto quando mancano offerte valide o requisiti. L’anteprima pubblica espone un solo mercato confermato per partita.

Il modello tratta PT e ST con quattro intensità Poisson. Le intensità FT sono le somme per squadra. Le combo valutano gli eventi congiunti sul punteggio PT e i gol ST. La quota equa è `1 + L/W`. Per le combo senza prezzo diretto il riferimento è mostrato se entrambe le componenti hanno quote osservate valide.

Gold applica i controlli strutturali e almeno il 2% di EV prudente. Platinum eredita l’intero filtro Gold e richiede OCI da 30 a meno di 50, calo di almeno il 3%, persistenza da 0,50 a meno di 1 e assenza di inversioni. I trigger Favorita PT, Inversione 1X2, Rimbalzo PT e Simmetria 1°+2°T indirizzano la ricerca di alternative. I trigger non cambiano probabilità o EV.

### Controllo delle fasce OCI del 9 novembre

Replay del workbook storico su **1.407 partite e 8.560 coppie partita/mercato** del 26 ottobre–6 novembre. Si usa l'ultima riga prematch con quota registrata come recente o stabile da cache, escludendo quote esplicitamente sintetiche e timestamp successivi alla rilevazione. Le linee equivalenti sono deduplicate. Nei record più vecchi la disponibilità è quella dichiarata dal workbook, con timestamp di fonte e flag eseguibile spesso assenti. Il blocco finale del 4–6 novembre è cronologicamente separato, ma appartiene allo stesso storico del dossier.

| Coorte | N intero periodo | ROI teorico | N 4–6 novembre | ROI teorico |
|---|---:|---:|---:|---:|
| Over 1,5 PT, OCI da 0 a meno di 50 | 93 | +14,3% | 18 | +9,2% |
| Over 1,5 PT, OCI da 50 | 225 | −7,6% | 39 | −4,9% |
| Over 2,5 FT, OCI da 0 a meno di 50 | 104 | +13,5% | 15 | +17,5% |
| Over 2,5 FT, OCI da 50 | 290 | −11,6% | 51 | −22,9% |
| GG PT, OCI sotto −50 e quota salita almeno del 5% | 159 | +29,1% | 32 | +35,5% |
| Over 2,5 FT, persistenza tra 0 e 1 | 193 | +11,9% | 36 | +14,7% |
| Over 2,5 FT, quota scesa almeno del 5% | 54 | −29,6% | 13 | −35,2% |
| Over 2,5 FT in calo con GG PT in salita nello stesso snapshot | 71 | +5,4% | 17 | +6,2% |

Per separare l'OCI dal prezzo si è stimata, fino al 3 novembre, la differenza media tra frequenza e fair per quattro fasce OCI, con 50 osservazioni equivalenti a correzione zero. Sul blocco finale la correzione migliora leggermente Brier e log-loss di GG PT e Over 2,5 FT e peggiora entrambi su Over 1,5 PT, Over 2,5 PT, Over 3,5 FT e GG FT. Per Over 3,5 PT e combo dirette il campione OCI è insufficiente. Le fasce rimangono coorti di osservazione. L'autoapprendimento V7 continua a verificare il peso del fair su finestre temporali separate.

Per Over 2,5 e Over 3,5 FT, lo scarto positivo tra stima e fair può arrivare a 15 punti. L'EV prudente minimo aumenta di due punti se OCI raggiunge 50, la quota scende almeno del 5% o la persistenza del movimento è pari a 1. I tre criteri applicano un unico rialzo, cumulabile con il supplemento per l'aggio. Il minimo di base per GG PT e Over 2,5 PT è 2%. L'Over 2,5 PT usa il controllo OCI comune agli altri mercati Gold.

L'OCI-AS attribuisce 15 punti alla persistenza mediana quando è strettamente compresa tra 0 e 1. Il contributo è zero ai due estremi. I report V7 di Radar Sorprese e Colpaccio riportano il rendimento unitario con e senza le tre vincite maggiori, la numerosità residua e gli stessi dati per la persistenza intermedia. Si usano le quote dei segnali congelate prima del calcio d'inizio.

### Auditor e ripristino operativo

Auditor ripristina per primi gli oggetti del giorno chiuso, i journal prematch pertinenti e gli esiti V7 della finestra mobile. Liquida i risultati, genera il report di apprendimento e aggiorna `data/arab_quote_v7_weights.json`. Il checkpoint R2 verifica i quattro file con SHA-256 prima di avviare lo storico V5.1. Il ripristino dello storico e la calibrazione V5.1 hanno budget separati di 15 e 6 minuti entro la finestra protetta. Un’interruzione V5.1 registra `partial_v51_skip` e conserva il checkpoint V7. La pubblicazione dei risultati pubblici usa lo storico disponibile dopo il ripristino.

Per ripristinare la revisione precedente, fare revert del merge di questa release su `main`, distribuire il nuovo `main` su Cloudflare e avviare Fast Scan con `force_fast`. Il riferimento precedente è `26814f3c34136fc7a91463f27a36e193f909b4ac`. La variabile `V7_SHADOW_ONLY` appartiene alla versione precedente. Conservare gli oggetti R2 correnti e verificare feed e accesso Gold dopo il refresh.
