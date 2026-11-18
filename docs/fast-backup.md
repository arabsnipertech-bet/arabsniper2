# Fast Scan e recupero

Il Fast ordinario aggiorna le partite del giorno italiano alle 06:00, 07:30, 09:00 e ogni 90 minuti fino alle 22:30. Alle 15:30 il Fast esteso copre oggi, domani e dopodomani. Il primo giorno può essere riutilizzato fino alle 16:30 se il Fast delle 15:00 è stato pubblicato e la copertura del calendario è verificata. Un recupero manuale con `force_fast` mantiene la scansione a cinque giorni.

Il watchdog indipendente richiama `fast_scan_backup.yml` a intervalli di dieci minuti. Attende almeno 15 minuti dallo slot, verifica i passaggi di pubblicazione e non accoda Fast se c'è già uno scrittore attivo. Ordinario ed esteso hanno ricevute diverse. Uno slot fallito può essere ritentato dopo dieci minuti, fino a due tentativi falliti complessivi per slot. Alla partenza del successivo ordinario la priorità passa allo slot nuovo. Il backup registra lo slot, il perimetro e il motivo dell'intervento. La verifica GitHub effettua zero chiamate ad API-Football.

La ricevuta Fast, i dati raccolti e l'evidenza delle date vengono salvati nello stesso checkpoint privato verificato. Il puntatore R2 viene aggiornato alla fine. Il ripristino del Fast carica cache, giornate e i documenti letti da Home ed Explorer. Una pubblicazione Fast conserva i riferimenti ai journal e agli archivi storici nel manifest precedente. Il Night mantiene cinque giorni e continua a utilizzare il ripristino completo.

## Dispatch da cron-job.org

- Metodo POST all'endpoint Actions di `fast_scan_backup.yml` sul repository principale.
- Body di verifica `{"ref":"main","inputs":{"dry_run":true}}`.
- Body di esercizio `{"ref":"main","inputs":{"dry_run":false}}`.
- HTTP 204 conferma la richiesta. La pubblicazione risulta verificata quando il Fast ha salvato un checkpoint con la ricevuta dello slot.

## Rollback

Ripristinare il commit della PR con un revert su `main` se un Fast nuovo non pubblica ricevute valide, manca una giornata già acquisita o compare una regressione storica. Conservare i manifest R2 esistenti. Verificare l'ultimo Night e il successivo Fast prima di intervenire sui dati. Per il recupero Live, ripristinare prima il commit di Growth e poi la modifica al relay condiviso nel repository principale.
