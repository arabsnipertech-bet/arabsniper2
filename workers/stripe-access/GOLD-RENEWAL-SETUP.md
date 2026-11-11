# Gold · Stato accesso e rinnovo manuale

## Dati e comportamento

La sessione OTP prova la titolarità dell’email e mantiene la durata di 24 ore.
`GET /api/auth/session` restituisce l’ora del server e la scadenza dell’accesso
della sola email autenticata. Il Worker legge i pass pagati di D1 e gli
abbonamenti/prove Stripe già riconosciuti. La scadenza include i pass futuri
che iniziano senza interruzioni; per prove e proroghe vale il limite Stripe
effettivo. Per il VIP senza termine viene mostrato «Senza scadenza».

Lo stato del riquadro ha una cache D1 di massimo cinque minuti, limitata
dalla scadenza e invalidata da acquisti e revoche dei pass. Aggiorna stato
rilegge la fonte. I feed privati continuano a verificare l’autorizzazione
indipendentemente dalla cache. L’OTP accetta una sessione senza Gold attivo
quando il rinnovo è abilitato, così l’utente può acquistare un pass; non può
leggere i feed.

`POST /api/auth/renew` accetta solo piano e scelta dello sconto, ricava l’email
dalla sessione e usa i tre prezzi una tantum già esistenti. Il Checkout dura
un’ora, non imposta rinnovi automatici. Il webhook firmato controlla sessione,
importo, valuta e PaymentIntent. I pass partono dalla maggiore tra la
scadenza corrente e l’ora del pagamento confermato. Il mensile conserva
l’ora di Roma e limita il giorno al termine del mese. Il ritorno da Stripe
mostra lo stato; non scrive alcun pass.

Lo sconto richiede accesso attivo con al massimo 48 ore residue e prova di
pagamento Stripe non rimborsato o contestato. Per il mensile storico conta
un pagamento Stripe verificabile collegato alla fattura; una prova gratuita
o una fattura a zero non basta. Il Checkout prenota lo sconto per la
scadenza corrente. Un solo Checkout pendente per email, una sola promozione
consumata per scadenza, chiave di idempotenza stabile e registro D1. La
prenotazione si libera dopo scadenza o fallimento verificati. Il prezzo
promozionale resta bloccato per l’ora del Checkout anche se nel frattempo
finisce l’accesso. Con un metodo differito, il pass scatta quando Stripe
conferma il pagamento.

## Preparazione del rilascio

1. Su un Worker e un D1 separati per la prova, applicare gli schemi D1
   esistenti, poi `renewal-schema.sql`. In produzione applicare soltanto
   la migrazione additiva sul database collegato come `ACCESS_DB`.
2. Generare e distribuire `production-bundle.mjs` sul Worker di accesso
   già esistente. Conservare route `/api/auth/*` e `/api/gold-data/*`,
   binding, `AUTH_ORIGIN`, modalità Stripe e segreti. Usare in sandbox
   i tre `TEST_PRICE_PASS3D`, `TEST_PRICE_PASS14D`, `TEST_PRICE_PASS1M`.
3. Mantenere il webhook esistente `/api/stripe-webhook` e il suo segreto.
   Oltre a `checkout.session.completed`, `checkout.session.async_payment_succeeded`,
   `charge.refunded` e `charge.dispute.created`, aggiungere allo stesso endpoint
   `checkout.session.expired` e `checkout.session.async_payment_failed`.
   Non creare un secondo endpoint in produzione.
4. Attivare `GOLD_RENEWAL_ENABLED=true` dopo la migrazione e il collaudo.
   `CHECKOUT_ENABLED=true` e le credenziali Stripe esistenti sono necessari.
   Il riquadro supporta il rilascio graduale senza sconto.
5. Per lo sconto, configurare un coupon Stripe del **10%**, durata **once**,
   valido e senza restrizioni prodotto (usato solo dal Worker). Impostare
   `GOLD_RENEWAL_COUPON_ID` e poi `GOLD_PROMO_ENABLED=true`. Non pubblicare
   un codice promozionale. Lasciare vuoto `GOLD_PROMO_ALLOW_EMAILS` salvo
   autorizzazione esplicita per una lista JSON di email specifiche.
6. Coordinare la pubblicazione del Worker e dei tre asset Gold con la
   prossima versione di Pages, dopo la revisione del PR. La pagina gestisce
   anche le risposte del Worker precedente durante il passaggio.

La chiave Stripe resta tra i segreti del Worker. Se è ristretta, verificare
che copra le operazioni già usate e in aggiunta lettura coupon e creazione,
lettura e scadenza delle sessioni Checkout. Nessun prezzo ordinario cambia.

## Collaudo e rollback

- `node --test tests/test_gold_renewal.mjs tests/test_gold_auth.mjs tests/test_stripe_access.mjs`
  usa SQLite in memoria e risposte Stripe/Resend simulate.
- `node tests/browser_gold_access.cjs` usa Playwright e un sito simulato.
  La suite è eseguita anche dal controllo browser esistente.
- Prima di abilitare la promozione in produzione, verificare coupon, importi,
  Checkout completato e abbandonato, webhook e rimborso in una sandbox Stripe.
  Con una risposta di creazione ambigua la stessa chiave viene riutilizzata;
  una prenotazione senza esito oltre 23 ore richiede verifica operativa.
- Per sospendere nuovi rinnovi, disattivare le due variabili Gold. Lasciare
  attivo il Worker aggiornato e il webhook per elaborare i Checkout già
  emessi e gli eventuali pagamenti differiti. Non rimuovere tabelle, coupon
  o segreti durante la riconciliazione. I pass già concessi rimangono in D1.
