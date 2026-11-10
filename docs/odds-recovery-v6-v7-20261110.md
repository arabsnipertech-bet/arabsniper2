# Recupero quote V6 e integrazione V7 — 10 novembre 2026

Mandato: acquisizione anticipata, prima osservazione immutabile per mercato e
bookmaker, rilevazioni temporali persistenti, pubblicazione Gold e verifica reale.

## Punti di ripristino

- Codice iniziale: `bb0c466e132c9baad8ed8f4f7a0f4501325f0a93`.
- Ramo di ripristino: `restore/pre-odds-v6-v7-20261110-bb0c466e`.
- Confronto V6: `09131e57e347c79246f6493d93e52f7215ff204f` del 29 ottobre.
- Il diagnostico verifica anche un manifest R2 immutabile e i checksum dei file
  ripristinati. Il suo SHA-256 è nel rapporto `diagnostic.json` del workflow.
- Ripristino in un checkout isolato con le credenziali già configurate:
  `python src/gold_storage.py restore --manifest-sha <SHA256_VERIFICATO>`.
  Questo ripristina i file locali e non sposta il puntatore R2 di produzione.

## Confronto e cause verificate

La raccolta paginata della V6 rimane presente. La versione attuale aggiunge
cataloghi semantici, sorgenti asiatiche equivalenti, limiti HTTP, polling adattivo,
checkpoint privati e motore decisionale V7. Non è stato ripristinato il motore V6.

I commit `c5dc0fec`, `9dc51b5e`, `69bcbc39`, `bb0c466e` sono stati esaminati.
Il primo introduce la precedenza delle quote 1X2; gli altri aggiornano regressioni
che presumevano l'ordine precedente delle richieste. Il workflow finale
`42912744430` è verde: 704 test Python, Node e browser desktop/mobile. I tre run
precedenti sono falliti su quelle aspettative; non erano errori delle formule V7.

Difetti aggiuntivi rilevati e corretti:

1. `odds_collection.py` e `quote_cache.py` mancavano dai percorsi che attivano il
   Fast alla pubblicazione di codice. Ora una correzione alla raccolta attiva una
   raccolta reale e non viene classificata come semplice rivalutazione da cache.
2. La pausa globale delle query improduttive poteva bloccare un 1X2 mancante
   anche quando erano disponibili quote gol. Il recupero dell'esito principale
   rispetta ancora cooldown e budget, ma non eredita quel blocco da altre gare.
3. GG/NG finale mancava dalle famiglie del recupero mirato. Ora è incluso.
4. La ricerca anticipata copre i cinque giorni del Night, con al massimo due
   query mirate per una gara lontana e senza operatori; può cercare i gol anche
   se il provider non offre ancora 1X2. Nessun prezzo viene ricostruito.
5. Le statistiche precedevano le quote: errori o ritardi potevano impedire la
   rilevazione. Le quote del palinsesto vengono ora acquisite prima e riusate
   durante l'analisi con l'orario della rilevazione effettiva.
6. La cache di calcolo limita le serie a 48 punti ed elimina i match dopo due
   giorni. Ora un archivio separato conserva tutte le nuove acquisizioni reali,
   incluse le quote invariate, prima di qualsiasi fusione o selezione.

## Contratto dello storico

`quote_history.py` scrive `arab_quote_price_history_index.json` e partizioni
`arab_quote_price_history_YYYYMMDD.json`. La partizione è scelta alla prima
osservazione e resta la stessa anche se la partita viene rinviata.

Ogni serie identifica fixture, bookmaker, mercato/periodo/linea e provenienza.
Ogni rilevazione conserva quota, orario osservato, orario del fornitore, fonte,
query, variazione dalla prima e dalla precedente osservazione e checksum.
La prima osservazione per esito è immutabile e non rappresenta l'apertura
ufficiale del bookmaker. Un archivio referenziato ma assente causa un errore:
non viene creata silenziosamente una nuova prima quota.

La migrazione copia esclusivamente i punti originali e timestampati del ledger
esistente, senza modificarlo e senza inventare i punti già assenti. Le quote
sintetiche non diventano osservazioni reali. La rilettura della cache non produce
acquisizioni né rinnova la loro età. OCI continua a usare serie omogenee e reali
del ledger di calcolo, non il numero delle richieste HTTP.

L'archivio usa la medesima archiviazione senza perdita, con chunk immutabili,
checksum SHA-256, ripristino verificato e puntatore R2 scritto per ultimo. In caso
di fallimento dello scan, un checkpoint incrementale può conservare soltanto
le osservazioni raccolte, senza pubblicare nuove analisi Gold.

## Diagnosi reale iniziale

Workflow `42914493682` e verifica aggiuntiva `42914803041`, effettuati il
10 novembre, senza scritture in produzione:

| Partita dell'11 novembre | Fixture API | Campionato | API quote | Archivio / Gold |
|---|---:|---|---|---|
| Kawasaki Frontale–Kashiwa Reysol | 1648030 | J-League Cup, 101 | Risposta vuota | Quote assenti |
| Vissel Kobe–Machida Zelvia | 1648031 | J-League Cup, 101 | Risposta vuota | Quote assenti |

Le fixture esistono nel palinsesto. Le risposte generali, per mercati 1, 5, 6,
8 e 34 e per campionato/data non contengono bookmaker o quote. Non sono prezzi
scartati dal parser. Non è possibile trasformare l'esistenza della partita in
una quota che il fornitore non restituisce.

Copertura iniziale dell'intero insieme di cinque giorni, incluse gare già
iniziate/terminate: 247 fixture, 16 con 1X2 completo disponibile in Gold, 382
prezzi gol e 15 gare con OCI. I rapporti successivi distinguono anche le sole
gare future e confrontano V6 e V7 sulla stessa risposta API.

## Collaudo e rilascio

I test aggiunti verificano oltre 48 acquisizioni, prezzi invariati, più HTTP nello
stesso secondo, isolamento degli operatori, migrazione reale, ripristino R2,
rinvio della partita, archivio mancante, rifiuto di dati futuri/post kickoff,
raccolta prima di statistiche fallite e visibilità 1X2 senza pronostico idoneo.

La Gold mostra “Prima osservata → Attuale” e mantiene le quote 1X2 anche se
non esiste un pronostico idoneo. Formule, soglie e file probabilistici V7 non
sono stati modificati.

Stato: collaudo e verifica reale del candidato in corso. Questo documento non
certifica ancora un rilascio in produzione. Commit finale, workflow, misure
dopo il rilascio e limiti residui verranno registrati dopo le verifiche.
