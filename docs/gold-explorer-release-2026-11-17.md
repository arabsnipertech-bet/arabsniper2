# Gold Explorer V1 — report di rilascio e collaudo

Data: 17 novembre 2026, fuso Europe/Rome.
URL: https://arabsniperbet.com/gold-explorer

## Stato e perimetro

Il backend Explorer è stato attivato; il collaudo autenticato reale è riuscito nel [run 44062331156](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44062331156). Il controllo ha usato una sessione temporanea del proprietario, eliminata al termine insieme ai soli record QA creati. Non sono stati inviati OTP né modificati abbonamenti.

Implementati pagina dedicata e pannello Gold, cinque classifiche, filtri locali, dettaglio selezione, ricerche salvate e preferiti personali, storico V7 separato dai nuovi certificati Explorer, metriche e confronti Night/finale. Nessun cambio al cron Night, ai piani Stripe o alla triade ufficiale.

La certificazione Explorer è prospettica: non esistono classifiche Explorer pregresse ricostruite dai vincitori. Il primo Night/Fast utile alimenterà i nuovi riferimenti tramite le pubblicazioni Gold esistenti. In assenza di certificati la pagina lo spiega e apre l'archivio V7 precedente.

## Architettura e dati autentici

Cinque feed Gold e archivi Auditor esistenti → proiezione isolata R2 `explorer/` → API privata `/api/auth/explorer/*` nel Worker Gold → stessa origine web. D1 conserva solo ricerche e preferiti account-scoped. Lease R2 esclusivo per i writer Explorer; l'hook Gold non attende se il lease è occupato e contiene gli errori dopo il commit Gold.

Import iniziale completato: 30 archivi storici, 38.700 righe storiche V7, 518 risultati di fixture registrati e 30.149 righe correnti (413 fixture × 73 mercati nel campione verificato). Questi conteggi non implicano che ogni riga abbia quota, validazione o esito. Nel test dello storico di sette giorni: 36.989 righe e 32.922 esiti di mercato verificati; gli esiti di mercato non sono partite indipendenti.

Mercati rilevati: 1/X/2 FT, DNB, GG/No Goal PT/ST/FT, Over/Under PT/ST/FT, linee asiatiche e handicap, combinazioni già calcolate da V7. Doppia chance non presente nel catalogo base verificato. Probabilità congiunte non ricostruite; quote e campi mancanti restano assenti.

## Sicurezza e integrità

- API anonima: HTTP 401 verificato online.
- Sessione proprietario: CRUD ricerche/preferiti e lettura corrente/storico verificati online.
- Abbonamento attivo, prova valida, revoca/scadenza, isolamento tra account, origine/CSRF e limiti: verificati nelle suite isolate, anche applicate al Worker reale modificato. Non sono presentati come login online su account pagati o trial reali di terzi.
- Codice precedente del Worker preservato dalla patch additiva; binding preesistenti confrontati con backup privato. SHA finale Worker `525408978cea191e620a89d6fe3d5645779686c2f694f4943934b49aaef1c698`.
- Snapshot Gold originali non modificati. Oggetti Explorer certificati indirizzati dal contenuto; cutoff per partita a kickoff meno cinque minuti, congelamento materializzato dalla successiva pubblicazione esistente. Nessun nuovo scheduler calcistico.
- Zero chiamate API-Football nel preflight strumentato e nel browser reale; nessun collector o fetch verso fonti quote nel percorso Explorer. Filtri e cambio delle cinque classifiche senza richieste API aggiuntive. La verifica non consiste in un confronto dei contatori globali del provider mentre gli altri processi Gold sono attivi.

## Test e prestazioni

CI: 884 test Python, tutte le suite Node e tutti i controlli browser esistenti superati. Due test aggiuntivi del caricatore verificano limite globale, ordine, cache, deduplicazione e recupero dagli errori.

Primo collaudo reale: desktop 1440 px, tablet 820 px, smartphone 390 px; zero errori JavaScript e nessun overflow orizzontale. Caricamento completo corrente 27.937 ms; storico sette giorni 39.558 ms. Questi tempi hanno motivato la PR #224: pool globale di 12 letture degli archivi, richieste duplicate condivise e rimozione delle barriere tra gruppi di quattro richieste. Nessuna chiamata calcistica aggiunta.

Collaudo finale della PR #224: [run 44064180102](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44064180102), completato positivamente il 17 novembre alle 11:26 italiane. Corrente: **13.498 ms** (circa −52%); storico sette giorni: **19.954 ms** (circa −50%). Stesse 30.149 righe correnti, 36.989 righe storiche e 32.922 esiti verificati nel campione. Zero errori JavaScript, zero richieste ai provider calcistici, zero richieste durante filtri/cambio classifiche. Tre viewport reali superati. CRUD personale e storico reali nuovamente superati. Screenshot desktop e mobile finali ispezionati visivamente.

Il caricamento iniziale dell'intero universo e dello storico resta percepibile: non viene dichiarato istantaneo. Una volta acquisiti i dati, filtri e ordinamenti sono locali. L'ottimizzazione dimezza il tempo osservato senza aumentare il numero di richieste agli archivi.

Lo storico completo conserva le pagine richieste in memoria: costo crescente con il volume. I tempi misurati sono di una singola esecuzione CI, non garanzie per ogni connessione/dispositivo. I controlli funzionali non sostituiscono un collaudo di carico multiutente.

## Rilascio e rollback

PR integrate: [#220](https://github.com/arabsnipertech-bet/arabsniper2/pull/220), [#221](https://github.com/arabsnipertech-bet/arabsniper2/pull/221), [#222](https://github.com/arabsnipertech-bet/arabsniper2/pull/222), [#223](https://github.com/arabsnipertech-bet/arabsniper2/pull/223), [#224](https://github.com/arabsnipertech-bet/arabsniper2/pull/224). Branch funzionalità `feature/gold-explorer-v1`; rifinitura prestazioni `perf/explorer-archive-loading`.

L'import completo iniziale è riuscito; il primo caricamento Worker si è fermato con errore generico. Il tentativo successivo ha caricato correttamente il codice ma la verifica immediata ha letto ancora la versione precedente. La rilettura successiva ha confermato l'esatta patch. La PR #223 ha quindi attivato quel codice già verificato senza nuovo upload né reimportazione, con attesa limitata per la propagazione del flag. Il test finale autenticato è passato. Non rilanciare indiscriminatamente i job una tantum.

Rollback: disabilitare il flag Worker `GOLD_EXPLORER_ENABLED` e la variabile GitHub omonima; ripristinare, se necessario e dopo controllo delle modifiche concorrenti, il contenuto Worker dal backup privato `ops-backups/gold-explorer/44060683182.json` e gli asset Pages precedenti. Conservare tabelle personali e archivio Explorer; non ripristinare o modificare archivi Gold, probabilità, pagamenti o risultati originali.

Report JSON e screenshot autentici sono negli artifact Actions; le immagini in `docs/qa/` precedenti sono sintetiche e non vanno confuse con schermate del sito online.

## Attività separate

La proposta di revisione Night/Fast non è stata implementata. Nessuna pubblicazione promozionale o inserzione è stata avviata durante questo collaudo. Il materiale promozionale non fa parte delle evidenze tecniche qui certificate.
