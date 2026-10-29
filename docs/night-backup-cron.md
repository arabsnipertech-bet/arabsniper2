# Backup esterno del Night Scan

Il timer esterno è cron-job.org. Non esiste un secondo cron GitHub né una nuova
automazione ChatGPT. Il servizio chiama `night_scan_backup.yml`, che controlla
lo stato GitHub del Night e, solo se serve, avvia `night_scan.yml` su `main`.

Il controllo non consulta API-Football, quote, dati Gold o segreti R2. Non
sostituisce il guardiano autorevole `night_scan_guard.py`: se il Night originale
parte durante il controllo, il gruppo dei writer e il checkpoint privato
impediscono la doppia scansione completata per la stessa giornata di Roma.

Il backup non interviene con un Night in corso, in coda o in attesa; riconosce
un Night riuscito con avvio e completamento nella giornata corrente. Gli errori
GitHub fermano il controllo senza tentare avvii alla cieca. Un tentativo è
ammesso solo tra le 02:00 e le 05:59 Europe/Rome. `dry_run=true` non avvia mai
il Night. L'eventuale scansione recuperata usa la normale quota API esistente.

## Configurazione cron-job.org

- Titolo: `ArabSniper — backup Night Scan`.
- Ogni giorno alle 02:20, fuso `Europe/Rome`.
- POST a `https://api.github.com/repos/arabsnipertech-bet/arabsniper2/actions/workflows/night_scan_backup.yml/dispatches`.
- Corpo JSON: `{"ref":"main","inputs":{"dry_run":false}}`.
- Header `Accept: application/vnd.github+json`.
- Header `Content-Type: application/json`.
- Header `X-GitHub-Api-Version: 2022-11-28`.
- Header `Authorization: Bearer <token dedicato>`.
- Non salvare le risposte. Una risposta HTTP 204 conferma il dispatch, non la
  riuscita della scansione: controllare anche il riepilogo GitHub del backup
  e, quando avviato, il successivo Night.

Usare un token GitHub fine-grained limitato al repository `arabsniper2`, con
solo `Actions: read and write` e i metadati obbligatori. Questa autorizzazione
copre le Actions del repository, non soltanto il Night. Non usare un token
classico con accesso a tutti i repository. Non inserire il token nel corpo,
nell'URL, nel codice, nei log o nella chat. Conservarlo solo nell'header riservato
del job. La creazione e l'inserimento della credenziale richiedono l'utente;
registrare la scadenza per il rinnovo.

Il job va abilitato dopo aver inserito il token. Un test di collegamento durante
il giorno non avvia scansioni; il test `dry_run=true` è sicuro anche di notte.
Il backup risolve il mancato trigger, non un'indisponibilità generale dei
runner GitHub, un'approvazione pendente o un Night bloccato ancora in corso.
