# ArabSniperBet V7.1 · Publisher

Il Publisher legge i board Gold già pubblicati in R2 e usa il primo ruolo V7 con una quota eseguibile per le BEST. Probabile, Plausibile e Colpaccio seguono l’ordine della triade. Ogni gruppo BEST ha al massimo una proposta completa e fino a due anteprime con partita, competizione, kickoff e categoria. La homepage mostra anche fino a due triadi complete delle prossime partite in Gold, con le quote osservate e l’indicazione delle stime in attesa di quota. Il calendario Live alterna le prossime gare e mostra squadre, competizione e orario. Il ramo pubblico `publisher-public-v71` contiene un solo file JSON.

## Orari Europe/Rome

| Contenuto | Quando | Destinazioni |
| --- | --- | --- |
| BEST | Verifica ogni 15 minuti e dopo Fast Scan. Gruppi di 2–3 gare entro 90 minuti. Primo kickoff entro 60 minuti e almeno 20 minuti di anticipo all’invio. | Telegram e homepage. Meta riceve al massimo una BEST Probabile Gold o Platinum per giornata. |
| Spunto Studio | Una gara con analisi Studio già pubblicata in Gold per ciascuna fascia di kickoff 16–18, 18–20 e 20–24, da 60 a 20 minuti prima del via, quando la fascia non ha già una BEST sullo stesso canale. Fino a due anteprime con lucchetto. | Telegram e Facebook. |
| Live | Una copertura per giornata di kickoff. Il backup avvia il monitor della gara annunciata e il monitor legge il feed ogni minuto circa dal calcio d’inizio. | Telegram aggiorna il post originale con le GIF di gol, rigore segnato e rigore sbagliato, poi con il parziale e il finale. Homepage e riepilogo finale su Facebook. |
| Riscontri | Dal primo ciclo utile dopo le 10:45, con certificato Auditor della giornata precedente e coorti conciliabili. | Telegram, Meta, homepage. |
| Dentro i Numeri | Dalle 16:45, con rinvio in presenza di una BEST da inviare. | Telegram, Meta, homepage. |
| Gold | Martedì e sabato dalle 12:15, se il webhook della prova risulta disponibile. | Telegram, Meta, homepage. |

Il recupero editoriale già avviato dal timer esterno riattiva lo stesso Publisher quando manca un ciclo recente. La verifica avviene dopo il marcatore pubblico e usa il medesimo registro degli invii. BEST e Studio su Telegram e Facebook riportano il link a Gold, al bot e alle promozioni attive. Dal 12 al 18 novembre 2026 il Publisher recupera anche i richiami rimasti senza ricevuta per canale a partire dalle 13:15 e dalle 21:15, nelle finestre della campagna.

Una singola richiede una proposta Probabile di livello Gold o Platinum e una finestra fra 20 e 45 minuti. Nei giorni con più gare, il Publisher può creare ulteriori gruppi se superano gli stessi controlli. L’ora UTC identifica gli eventi nel registro, l’ora di Roma determina giorno e orari editoriali.

## Ricevute e riscontri

- Il registro è nel prefisso privato `publisher/v7.1/`. Le evidenze congelate usano creazione condizionale. Il registro usa scritture condizionali sul suo ETag.
- I tentativi hanno un massimo di tre invii, con attese crescenti per i rifiuti transitori. Le risposte ambigue richiedono riscontro positivo dal canale. Un invio incerto resta sospeso in assenza di ricevuta.
- Il recap di Telegram, Facebook e homepage mette in evidenza fino a quattro partite con una proposta corretta tra i ruoli della triade prematch certificata dall’Auditor. Il bilancio distingue BEST gratuite, BEST Gold documentate e ricerca interna. La homepage conserva tutte le metriche del report. Successo su esiti corretti ed errati. ROI teorico su una unità per esito chiuso, rimborsate incluse a zero.
- Il monitor richiede la ricevuta Telegram del post live. API-Football fornisce punteggio ed eventi della singola gara. Un rigore segnato richiede la corrispondenza con il punteggio ufficiale. Un rigore sbagliato può aggiornare la scheda a punteggio invariato. Il post conserva gli ultimi quattro eventi e usa il minuto e la squadra riportati dal feed. Le rettifiche del punteggio in gioco richiedono due osservazioni indipendenti.
- Il monitor ha un tetto di 195 richieste giornaliere e conserva 1.000 richieste di riserva. Il Publisher ordinario riprende dalla cache dei punteggi se il monitor si ferma. Se anche la cache è indisponibile, il budget di recupero è di 26 richieste giornaliere con la stessa riserva. Il risultato finale riporta il parziale quando disponibile.

## Passaggio e rollback

1. La produzione V7.1 attende che il normale produttore Fast/Night abbia rilasciato il marcatore del feed pubblico. Il Publisher importa gli identificativi esposti nei due giorni precedenti e attende la fine degli invii legacy. I workflow statistici continuano a usare i propri blocchi di concorrenza.
2. In emergenza disabilitare **Publisher ArabSniperBet V7.1** in Actions oppure impostare la variabile di repository `PUBLISHER_V71_DISABLED=true`. Controllare eventuali run già in corso. `PUBLISHER_V71_FACEBOOK_DISABLED` e `PUBLISHER_V71_TRIAL_DISABLED` sospendono soltanto le rispettive funzioni editoriali.
3. Per tornare all’assetto precedente, fare revert su `main` delle PR del Publisher in ordine inverso, partendo dai correttivi e chiudendo con la PR di rilascio e verificare la nuova pubblicazione Cloudflare. Il riferimento `backup/before-v7-1-publisher-20261110` identifica la configurazione iniziale. Fare revert, evitando il reset di `main` che contiene anche pubblicazioni statistiche intervenute nel frattempo.
4. Attendere la fine dei job V7.1 prima di riattivare eventuali invii precedenti. Consultare le ricevute prima di riaprire una fascia, mantenendo separati i job Meta e la coda Metricool. Conservare il registro R2 e il ramo pubblico per la riconciliazione.
