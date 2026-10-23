# Gold OTP — bozza, non attivare in produzione

La modifica aggiunge codice email, cookie opaco HttpOnly e dati Gold letti da R2 privato.
Le vendite e il login di produzione restano sulla versione precedente fino al cutover.
I dati storicamente pubblicati su GitHub non diventano retroattivamente segreti.

## Prerequisiti e blocchi ancora aperti

1. Account Resend e dominio mittente verificato con i record DNS forniti dal servizio.
   Documentazione: https://developers.cloudflare.com/workers/tutorials/send-emails-with-resend/
   Non inviare chiavi in chat. Configurare i secret `RESEND_API_KEY` e `OTP_SECRET`
   (quest'ultimo casuale, almeno 32 byte), variabili `EMAIL_FROM` e `AUTH_ORIGIN`.
2. Eseguire auth-schema.sql nel D1 esistente ACCESS_DB, senza cancellare passes.
3. Creare bucket R2 privato GOLD_DATA: accesso pubblico e r2.dev disabilitati.
   Migrare il publisher dello scanner affinché carichi i file Gold su R2 prima del cutover.
   Questa PR NON modifica ancora i workflow dello scanner: il publisher è un blocco al rilascio.
   La homepage deve continuare a ricevere solo l'anteprima gratuita, non l'intero feed Gold.
4. Rimuovere i feed Gold correnti da pubblicazione GitHub/Pages e interrompere i successivi
   commit pubblici di dati Gold. Verificare tutti i percorsi, Pages preview e archivi.
   Le copie e i dati già pubblici non sono revocabili; proteggere almeno tutti i dati futuri.
5. Usare un dominio staging dello stesso sito per frontend e API, con Stripe test e D1/R2 separati.
   AUTH_ORIGIN deve corrispondere esattamente all'origine HTTPS staging.
   Un Worker workers.dev non condivide il cookie __Host- con arabsniperbet.com.
6. Il Worker ha ora due moduli: worker.mjs importa auth.mjs. Distribuire con Wrangler
   (entry point worker.mjs) o bundle ES module, NON incollare solo worker.mjs nell'editor.
7. Route precise aggiuntive `/api/auth/*` e `/api/gold-data/*` sul dominio corretto;
   lasciare le due route Stripe esistenti. Non usare una route generica `/api/*`
   perché esiste un altro Worker per i risultati live. Non aggiungere cache alle API Gold.
8. Programmare cron giornaliero per pulizia record OTP, sessioni e limiti scaduti.

## Collaudo e cutover

- AUTH_ENABLED resta assente/false in produzione. La nuova dashboard dipende da OTP:
  non pubblicarla prima di backend, R2 e invio email verificati in staging.
- Provare ricezione reale del codice, tentativi errati, reinvio, doppio uso, logout,
  scadenza sessione, VIP Bruno, utente non pagante e abbonamento revocato.
- Dopo verifica email positiva senza piano: messaggio di acquisto; nessuna sessione Gold.
- Non controllare l'abbonamento prima dell'invio, per non rivelare quali indirizzi sono iscritti.
- Codici 6 cifre CSPRNG, 10 minuti, 5 tentativi, reinvio non prima di 60 secondi.
  Limiti D1: 5 invii/email/ora, 10/IP/ora, 100 globali/ora; 60 richieste/IP/10 minuti.
  Le soglie sono conservative e vanno tarate sul traffico reale prima delle campagne.
- Cookie Secure HttpOnly SameSite Strict, host-only, token 256 bit memorizzato solo come hash.
  Durata 7 giorni; verifica Stripe/D1 a ogni richiesta dati. Non promette anti-condivisione assoluta.
- AUTH_ENABLED=true disabilita il vecchio endpoint email-only anche per i VIP.
- Mantenere attivi i pagamenti. Fermare il cutover se le email o la pubblicazione dati non funzionano.

## Verifica locale

`node --test tests/test_gold_auth.mjs tests/test_stripe_access.mjs`

14 test con SQLite e servizi esterni simulati superati. Questi test non attestano
consegna email, D1/R2 cloud, configurazione DNS o migrazione dei feed pubblici.
