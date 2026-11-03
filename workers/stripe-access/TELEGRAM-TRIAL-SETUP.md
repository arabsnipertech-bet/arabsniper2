# Prova Gold gratuita di 7 giorni via Telegram — attivazione controllata

**Stato:** codice predisposto, **NON attivo** finché la migrazione e i controlli sandbox non sono completati.

## Flusso

1. L'utente avvia una **chat privata** con il bot `@ArabSniperBot` e scrive `/prova`.
2. Il bot raccoglie l'email e invia un codice di sei cifre tramite Resend, valido dieci minuti.
3. Solo dopo il codice corretto, cerca/crea il cliente Stripe e attiva una sottoscrizione **trialing** sul prezzo Gold mensile esistente.
4. `trial_end` e `cancel_at` coincidono a **7×24 ore** dall'attivazione, con `trial_settings[end_behavior][missing_payment_method]=cancel`. Nessuna carta richiesta; Stripe non rinnova automaticamente questa prova.
5. L'utente accede all'Area Gold con la stessa email e il login OTP esistente. I controlli di accesso Stripe già considerano le sottoscrizioni `trialing` valide fino alla scadenza.
6. L'account Telegram e l'email verificata vengono registrati in D1 per impedire ulteriori prove. Le offerte da 30 giorni, i pass pagati e i motori Gold/Platinum rimangono invariati.

Le chat del canale non sono usate come richieste di iscrizione: il bot risponde **solo a messaggi privati** verificati dal token del webhook Telegram.

## Prerequisiti di sicurezza, in ordine

1. Testare su un Worker Cloudflare **staging**, con D1 e bot **di test** separati. Non registrare subito il webhook del bot di produzione.
2. Eseguire `telegram-trial-schema.sql` sul database D1 che sarà collegato come `ACCESS_DB`.
3. Sul Worker staging configurare: `STRIPE_MODE=test`, `STRIPE_SECRET_KEY` di test, `TEST_PRICE_MONTHLY`, `RESEND_API_KEY`, `EMAIL_FROM`, `ACCESS_DB`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_WEBHOOK_SECRET`, `TELEGRAM_TRIAL_HMAC_SECRET`. Usare un webhook secret lungo e casuale; non pubblicare le chiavi in GitHub.
4. Compilare `python3 workers/stripe-access/build_production.py` e distribuire il Worker generato/compilato su Cloudflare; configurare la route HTTPS **POST `https://arabsniperbet.com/api/telegram-webhook`** verso il **medesimo Worker degli accessi**, senza alterare le route Stripe e OTP già operative.
5. Impostare `TELEGRAM_TRIAL_ENABLED=true` **solo nel test**; chiamare Telegram `setWebhook` con URL, `secret_token` corrispondente alla variabile e `allowed_updates=["message"]`. Il canale può continuare a usare `sendPhoto` in uscita; testare anche la pubblicazione automatica già attiva.
6. Verificare in Stripe sandbox: richiesta `/prova`, codice errato, codice corretto, duplicati per email/Telegram, nessuna carta, nessuna fattura pagata, `trialing`, `cancel_at=trial_end`, accesso Gold, scadenza automatica e assenza di rinnovo. Ispezionare in Stripe eventuali sottoscrizioni create con parametri inattesi e annullarle.
7. Solo dopo i test e un backup delle impostazioni di produzione, applicare la migrazione D1 produzione, creare i secret sul Worker, distribuire, e attivare il **webhook del bot già esistente**. Convalidare una prova controllata su un indirizzo di test autorizzato.
8. Fino al completamento di tutti i passaggi lasciare `TELEGRAM_TRIAL_ENABLED=false`: pubblicizzare la prova automatica come attiva solo dopo il collaudo live.

## Rischi controllati

- Nessun utente non verificato può attivare una sottoscrizione.
- Un solo uso per email e per Telegram ID, con unicità D1.
- Retry Stripe idempotenti basati sulla richiesta verificata, senza estendere la durata.
- Se Stripe non conferma la cancellazione programmata, il Worker prova a cancellare subito la sottoscrizione invece di attivare l'accesso.
- L'email è richiesta in chat privata, non nel canale.
- **Importante:** non attivare `TELEGRAM_TRIAL_ENABLED` prima di configurare il webhook e il database. Il Worker non distribuisce sé stesso: è necessario un deployment Cloudflare autorizzato.

## Test repository

```sh
node --test tests/test_telegram_trial.mjs
python3 workers/stripe-access/build_production.py
node --check workers/stripe-access/production-bundle.mjs
```
