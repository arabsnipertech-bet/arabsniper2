# Ripristino della selezione prematch — 21 novembre 2026

La politica `TRIAD_RECENT_SELECTION_V1` di #248 è ritirata dal percorso attivo.
Il suo filtro aggiuntivo confrontava sia l’osservazione sia l’aggiornamento del
provider con l’inizio della finestra di tre ore: scartava anche cache quotate
ancora valide secondo il motore. Il salvataggio dei soli slot con quota poteva
così perdere tutti gli impegni e il ciclo successivo nell’ultima ora dichiarava
«prima analisi tardiva», senza dimostrare l’assenza di pubblicazioni precedenti.

## Comportamento ripristinato

`TRIAD_LATEST_PREMATCH_V1` rielegge i ruoli dalla matrice dell’ultima analisi
prematch disponibile. Questo ripristina la modalità senza blocchi della triade
di sabato 14 novembre, verificata su
`58d48e745e2159516ae2c57ad5902b145ef01521`: stesso insieme di mercati, stesse fasce
di quota/probabilità, scelta sulla matrice corrente senza lock storico.

La prima quotazione di giorni prima resta una prova di apertura, non impone il
mercato attuale. Non ci sono scarti aggiuntivi per T−60/T−180 né declassamenti
automatici per l’ora del primo arrivo. Continuano a valere `price_usable`, i limiti
di validità della cache, le quote non eseguibili, i requisiti statistici, i
controlli di compatibilità e le qualifiche Gold/Platinum. Una quota realmente
scaduta non viene resa recente o riattivata. Le combo senza quota restano stime.

La nuova conferma registra data e prezzo effettivi; il predecessore completo
resta in `replaces` quando cambia il mercato o si migra dalla politica ritirata.
Il vecchio `study_only` imposto dal calendario non contamina la qualifica della
nuova elezione. Le qualifiche correnti derivano di nuovo dai dati del mercato.
Gli snapshot originali e gli esiti delle partite iniziate non vengono riscritti.

La Gold mostra «Ultima scelta prematch» per le nuove pubblicazioni. Per gli
snapshot della politica ritirata mostra «Fotografia prematch precedente» e la
data effettiva; rimuove l’affermazione non dimostrata «prima analisi tardiva».
Non trasforma retroattivamente uno Studio in una proposta qualificata.

Lambda, probabilità, fair dello stesso bookmaker, EV centrale/prudente e
ranking restano invariati. Si conservano le correzioni matematiche e il recupero
EV–aggio già in produzione, senza nuovi pesi basati sulle sconfitte del giorno.
Il ROI non entra nella selezione.
Il confronto AST con il 14 novembre conferma identiche anche le funzioni
`statistical_rates`, `market_rates`, `blend_rates` e `score_model`.

## Verifica e rilascio

I test riproducono cache con provider vecchio e osservazione recente, prima
analisi nell’ultima ora, impegni contaminati da #248, rielezione completa, cambio
ripetuto di mercato, timestamp invariati, quote scadute, partite già iniziate e
assenza di quote inventate. L’override esplicito `recent_selection={'enabled':
False}` conserva il percorso legacy per confronti controllati; non è il default.

Il rilascio usa una singola ripubblicazione delle cinque giornate dai dati
privati già salvati. `cached_refresh_safe` include il helper di sola selezione;
una modifica delle probabilità o del raccoglitore continua a richiedere una
scansione normale. Per questo ripristino **non** usare `[night-before-fast]` o
`[skip ci]`: il Fast da push deve eseguire il refresh cached esistente, senza
chiamate API-Football. Calendari e workflow restano invariati.

Il log di refresh riporta, prima/dopo per giornata, proposte quotate, stime,
slot vuoti e policy attiva. Sono soli aggregati: niente selezioni future,
quote, credenziali o dati di clienti. Verificare questi risultati e il successivo
salvataggio R2 prima di dichiarare completato il ripristino del servizio.

La suite tecnica e il confronto di disponibilità non sono una prova di maggiore
rendimento sportivo. Il rollback di emergenza deve preservare archivi e risultati,
senza riattivare il filtro ritirato o ricostruire pronostici a partita iniziata.
