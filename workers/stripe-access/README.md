# ArabSniper Stripe: migrazione pagamenti, non ancora in produzione

Codice destinato al Worker esistente `stripe-checker`, account Stripe `acct_1T6rVoH763HaPm5U` (Aransniperbet). Non eseguire deploy automatici prima della configurazione e dei test Stripe sandbox. Nessuna chiave nel repository.

## Cosa cambia

- POST `/api/create-checkout-session`: Checkout ospitato Stripe, prezzo controllato dal server, pagamento singolo Daily/Weekly e ricorrenza mensile. Ripara l'endpoint già chiamato dalla homepage. Usa Checkout invece di creare tre Payment Links: permette di riusare i pulsanti esistenti e mantenere i prezzi sul server.
- POST `/api/stripe-webhook`: firma HMAC verificata sul corpo originale con tolleranza 5 minuti. `checkout.session.completed` e `checkout.session.async_payment_succeeded` attivano solo pagamenti confermati per prezzi consentiti. ID Checkout univoco, mai estensione da duplicati.
- Archivio D1: Daily 24h / Weekly 168h dalla conferma di pagamento, non dalla creazione del checkout. Gli acquisti sovrapposti hanno ciascuno la propria scadenza; non si sommano.
- GET `/?email=...` oppure `/api/check-access?email=...`: mantiene la compatibilità della dashboard; legge i pass e verifica lo stato delle transazioni Stripe. Per mensili controlla tutti i customer con la stessa email, solo prezzi Gold conosciuti, stato e scadenza, ultima fattura pagata (oppure trial in corso).
- Revoca pass per rimborso anche parziale o contestazione. Le verifiche al login controllano anche Stripe, per coprire un webhook in ritardo. La revoca mensile segue stato/scadenza della subscription: un rimborso di fattura, da solo, non cancella un abbonamento.
- VIP Bruno/Manuel preservati. Rimosso il falso token condiviso basato sull'orario.

## Limite dell'accesso esistente

Questa patch corregge il billing, NON completa autenticazione o protezione dati: la dashboard attuale accetta una semplice email, compresi i VIP, e scarica i dati da sorgenti GitHub pubbliche. CORS non sostituisce un login. Non descrivere questa versione come protezione anti-condivisione o autenticazione sicura. Prima di dichiarare l'area realmente riservata occorrono verifica email, sessione firmata e feed Gold protetti server-side. Non è necessario riscrivere il billing per aggiungerli.

## Configurazione Cloudflare

1. Salvare versione/codice del Worker corrente per rollback. Creare un database D1 dedicato (es. `arabsniper-access`). Eseguire `schema.sql` nella console del database.
2. Sul Worker `stripe-checker`, aggiungere binding D1 `ACCESS_DB` a quel database.
3. Conservare `STRIPE_SECRET_KEY` come secret già presente (oppure usare una chiave ristretta con autorizzazioni necessarie). Non incollarla in chat, HTML, git o variabili pubbliche.
4. Variabili: `STRIPE_MODE=live` e `CHECKOUT_ENABLED=false` durante preparazione. Secret `STRIPE_WEBHOOK_SECRET` verrà configurato con il segreto dell'endpoint corretto. Il Worker richiede la modalità esplicita.
5. Distribuire `worker.mjs` nel Worker ES module. Aggiungere solo le route esatte `arabsniperbet.com/api/create-checkout-session` e `arabsniperbet.com/api/stripe-webhook` al medesimo Worker. Non usare `/api/*`: altre API del sito sono gestite da Worker diversi. Se il sito serve anche www senza redirect, configurare le corrispondenti due route esatte, senza modificare il dominio Pages. Il dominio `stripe-checker.arabsnipertech.workers.dev` continua a servire il login esistente.
6. Su Stripe configurare l'endpoint HTTPS `https://arabsniperbet.com/api/stripe-webhook`: eventi `checkout.session.completed`, `checkout.session.async_payment_succeeded`, `charge.refunded`, `charge.dispute.created`. Per questa versione le subscription vengono verificate direttamente su Stripe al login: non vengono memorizzate in D1. Il vecchio endpoint è disabilitato: non riabilitarlo prima del deployment e dell'inserimento del suo segreto esatto. API usata dal Worker: `2026-08-26.dahlia`.
7. Chiave webhook in Cloudflare come secret `STRIPE_WEBHOOK_SECRET`. Nessun bisogno di inviarla in chat.

## Prima prova in sandbox

Usare Worker e D1 separati, modalità `STRIPE_MODE=test`, chiave e webhook della sandbox, variabili `TEST_PRICE_DAILY`, `TEST_PRICE_WEEKLY`, `TEST_PRICE_MONTHLY` con prezzi sandbox 500/1500/2900 centesimi. Nessun ID live funziona in sandbox. Configurare dominio staging e origini/URL di ritorno appropriati nel clone (il file usa il dominio produzione); non instradare gli API produzione al Worker di prova. Abilitare checkout soltanto nel clone di prova. Verificare completamento, annullamento, pagamento differito, doppio evento, rimborso, scadenza e accesso al mensile. Nessun pagamento reale è stato effettuato durante lo sviluppo.

## Attivazione produzione

Dopo la prova completa: abilitare i tre nuovi prezzi Stripe; aggiornare la homepage con prezzi 5/15/29 e redirect checkout nella stessa scheda; verificare route e webhook firmato, poi impostare `CHECKOUT_ENABLED=true`. Fino ad allora risponde 503 con messaggio di preparazione. Non cambiare il prezzo degli abbonati già esistenti automaticamente. Valutare separatamente l'archiviazione dei vecchi prezzi per nuove vendite.

| Piano | Nuovo prezzo Stripe | EUR | Rinnovo |
|---|---|---:|---|
| Daily | `price_1UTM5fH763HaPm5Ufge4HJC5` | 5 | nessuno, 24h |
| Weekly | `price_1UTM5mH763HaPm5UwzU6S0LG` | 15 | nessuno, 168h |
| Monthly | `price_1UTM5yH763HaPm5Uo2hm0Ped` | 29 | mensile |

I vecchi mensili 19 e 19,90 EUR sono riconosciuti. I vecchi pass singoli precedenti all'installazione NON sono ricostruiti automaticamente: prima del rilascio controllare acquisti recenti ed eventualmente importarli tramite eventi verificati, senza rinnovare la durata scaduta.

## Test eseguiti

`node --test tests/test_stripe_access.mjs` (Node 24, SQLite in-memory): nove test con API Stripe simulate. Firma/tamper/replay, duplicati, durata e scadenza esatta, pagamento asincrono, rimborso prima/dopo fulfillment, prezzo/quantità/valuta, più customer stessa email, fattura mensile non pagata, VIP, blocco checkout non configurato, prezzo non controllabile dal client e origine. Questi test non sostituiscono le prove end-to-end su Stripe/Cloudflare.

## Verifica sandbox 22 ottobre 2026

Checkout reali in modalità test completati per Daily 5 EUR, Weekly 15 EUR e Monthly 29 EUR. Il Worker di test ha restituito accesso daily, weekly e stripe_subscription/monthly per email non VIP. Rimborso simulato del Daily riuscito: il successivo controllo ha riconosciuto il mensile anziché il pass rimborsato. I test automatici su duplicati e scadenze passano. Nessun addebito reale; la consegna firmata del webhook di produzione resta da verificare.
