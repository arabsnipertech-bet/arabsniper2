# Homepage giornaliera

La homepage legge esclusivamente `data/arab_quote_homepage.json`, una proiezione pubblica di massimo tre partite. Non carica payload, storico o dettagli Gold e non usa blur per nascondere dati riservati.

`src/publication_storage.py` genera il feed prima dell'impacchettamento. Night e Fast già copiano e pubblicano `arab_quote_*.json`, quindi includono automaticamente il nuovo file. Nessuna modifica ai criteri della dashboard o al modello. La prima selezione non vuota del giorno resta fissa, quote comprese, per evitare di esporre altre partite a ogni refresh. Il nuovo giorno è calcolato in Europe/Rome. Il browser nasconde subito i dati di ieri anche se lo scanner è fermo e riprova ogni minuto.

Filtro della sola vetrina: proposta principale con quota preferita 1,5, partita NS futura nella giornata italiana, primary_eligible, copertura >=75%, statistica e book >=50%, distanza <=10 punti, quota >=1,5, esclusi context_conflict e fragile_halftime. Ordine: minore delle due probabilità decrescente, support_score decrescente, orario e ID. Massimo tre fixture distinte. Se mancano candidati non si forza il numero. Soglie sperimentali, non validate come tasso di successo.

I link Gold aprono la dashboard esistente senza cancellare lo storage o forzare logout. Dashboard, Worker Stripe e funzione checkout sono conservati. I prezzi e plan ID restano Daily 3 €, Weekly 7 €, Monthly 19 €. Il backend `/api/create-checkout-session` non è nel repository: l'esistenza di questi pulsanti non certifica il pagamento end-to-end. La protezione preesistente della dashboard e delle sorgenti non è stata modificata o rafforzata da questo intervento.

Verifiche: `PYTHONPATH=src python -m unittest discover -s tests -p test_homepage_preview.py`; `node tests/test_homepage.js`; `node --check web/homepage.js`. Il test JS usa dati sintetici e un orologio fisso. Nessun pagamento reale eseguito. Browser locale non disponibile nell'ambiente di lavoro: controllo visivo da completare in anteprima Cloudflare.
