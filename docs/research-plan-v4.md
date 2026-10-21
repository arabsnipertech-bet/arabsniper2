# ArabSniper: raccolta verificabile e ricerca del modello

## Rilascio v3.4: fondazione, non modello calibrato

- Proposta principale limitata a Over 1,5 PT, Over 2,5 PT, Over 2,5 FT,
  Over 3,5 FT e GG PT sì. Tutti gli altri eventi restano consultabili/archiviati.
- In assenza di vantaggio teorico positivo, la proposta è la più probabile nella
  fascia richiesta, non la quota rara con rendimento meno negativo. Etichetta
  esplicita: convenienza non dimostrata.
- Eliminato il limite del 60% che nascondeva grandi divergenze 1X2 appena sotto
  soglia. Il limite di divergenza di 25 punti resta provvisorio, non appreso.
- Statistiche raccolte incrementalmente: massimo 20 nuovi recuperi Fast e 80
  Night per esecuzione (variabili repository ARAB_FAST_STATS_BUDGET e
  ARAB_NIGHT_STATS_BUDGET). Il budget conta fixture richieste, non eventuali
  retry HTTP. Le quote del piano API non sono state verificate da questo rilascio.
- Risposte con xG e tiri per entrambe le squadre riutilizzate. Risposte parziali
  o vuote ritentate dopo 24 ore, errori dopo un'ora. Nessun dato inventato.
- Copertura per squadra pubblicata nel dossier: xG/tiri fatti e subiti, numero
  di osservazioni reali, provenienza. Uno zero valido conta come osservazione.
- Il report selection-replay-2026-10-20.json confronta la selezione a input
  identici. NON contiene risultati né dimostra rendimento o calibrazione.

## Problemi ancora aperti

Le medie non sono normalizzate per gli avversari. La probabilità di riferimento
min(statistica,book) è una regola comparativa provvisoria, NON una probabilità
calibrata né un limite di confidenza. La proposta non implica valore positivo.
Digiuno, OCI e accordo tra fonti non giustificano bonus automatici.
Le medie dei tiri FT non sono dati specifici PT. Non ricostruire tiri PT da FT.
Non assumere che un endpoint vuoto significhi copertura esclusa dal fornitore:
consultare la copertura campionato/stagione nel futuro collector.

## Sviluppo successivo e criteri di promozione

1. Dataset cronologico con timestamp della disponibilità: ultime 8 gare per
   descrivere ciascuna squadra, forma 4, PT 4 validi; risultati successivi
   esclusi dalle feature. Partite selezionate e scartate incluse.
2. Modello attacco/difesa normalizzato per avversari, vantaggio casa e campionato;
   regolarizzazione verso riferimento campionato per campioni piccoli.
   Separare il dataset di stima dei parametri dal breve campione di forma.
   Nessun peso fissato per ottenere la giocata desiderata su Wolves o Dortmund.
3. Confrontare baseline bookmaker, modello soli gol, modello con xG/tiri e
   combinazione statistica/mercato. Blocchi di addestramento, calibrazione e test
   successivi nel tempo; una fixture non può entrare in più blocchi tramite
   aggiornamenti diversi. Non ottimizzare sul test finale.
4. Metriche per mercato/campionato/fascia quota: Brier, log-loss, grafico di
   calibrazione, numerosità, rendimento a puntata costante, drawdown,
   intervalli di incertezza raggruppati per fixture, confronto quota finale.
   Un mese serve per un pilota, non prova un vantaggio stabile.
5. Confronti per ablazione: tenere xG, tiri, variazioni solo se migliorano il
   risultato fuori campione. OCI/drop/aggio non contano come tre prove indipendenti.
6. Pubblicare una probabilità finale soltanto con una procedura di combinazione
   verificata. Fino ad allora mostrare le fonti e la regola di confronto separatamente.
7. Conservare candidati e snapshot immutabili. Se una variante non migliora la
   baseline bookmaker, non promuoverla come modello superiore.

## Operatività

Night 5 giorni, Fast 2 giorni. Credenziali soltanto nei GitHub Secrets.
Nessuna integrazione exchange o acquisto di dati è incluso in questa modifica.
Volumi reali richiedono una fonte exchange; il margine delle quote non è il
profitto reale dell'operatore. Cloudflare espone i JSON generati dal backend:
la nuova UI non aggiorna le analisi vecchie. Verificare model_id e captured_at.

Fonti di progetto:
- https://www.api-football.com/documentation-v3
- https://arxiv.org/abs/1802.08848
- https://scikit-learn.org/stable/modules/calibration.html

## Audit giornaliero verificabile

L'auditor produce `auditarchive/prematch_YYYY-MM-DD_daily.json` e `.md` ad ogni
settlement. Il JSON conserva identità della fixture, versione, prima/ultima
osservazione separata, scelta `best_candidate` effettivamente congelata, cinque
mercati obiettivo (e la scelta storica se esterna), quote, probabilità distinte,
pro/contro, dati mancanti e risultato. Non ricostruisce una proposta con il modello
odierno. Senza aggiornamenti, l'ultima osservazione coincide con la prima.

Ogni riga rinvia alla collection/key dell'archivio risultati del giorno e contiene
SHA-256 del record originale, serializzato JSON UTF-8 con chiavi ordinate,
separatori compatti e senza `settlement`. L'auditor si blocca se un record già
archiviato differisce dalla fonte congelata. Git conserva le revisioni dei report;
`generated_at` e `source_revision` identificano l'esecuzione.

Le fixture sono deduplicate per versione/periodo/snapshot; prima e ultima non si
sommano. I risultati mancanti restano pendenti; le quote mancanti non entrano nel
ROI teorico. Il confronto statistica/book usa gli stessi eventi con entrambe le
probabilità. I dati retrospettivi restano distinti. Il report non aggiusta soglie,
non promuove varianti e non dimostra redditività. Prima di modificare il modello,
registrare l'ipotesi e confrontarla su un blocco temporale futuro separato.
