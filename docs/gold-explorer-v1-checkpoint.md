# Gold Explorer V1 — checkpoint di sviluppo, 17 novembre 2026

## Stato reale
**Sviluppo incompleto per disconnessione dell'ambiente locale. Non pronto al merge o alla produzione.**

Il branch feature/gold-explorer-v1 è stato creato da eb9abe79e391a4e4eaf158fd43449dceb3ab8921.
Questo checkpoint contiene soltanto documentazione. Il codice implementato e gli screenshot sono ancora nella copia locale indicata sotto: non sono stati trasferiti sul branch prima della disconnessione. Non confondere questa PR draft con una consegna del software.

Nessun merge, deploy, migrazione D1, modifica di configurazione remota, pubblicazione organica o campagna a pagamento è stato eseguito.

## Dove riprendere
Ambiente Windows dell'utente:
C:\Users\utente\Documents\ChatGPT\Script arab sniper\gold-explorer-v1

La copia preesistente arabsniper2 era ferma a 8ef0657 e aveva quattro workflow modificati; è stata preservata.
La nuova copia è sul branch locale feature/gold-explorer-v1. I sorgenti remoti aggiornati sono stati recuperati attraverso il connettore GitHub perché git fetch non raggiungeva GitHub.
**Il diff locale rispetto al vecchio HEAD comprende anche aggiornamenti già presenti su main: non committarlo integralmente.** Confrontare i file con il commit remoto di base e trasferire soltanto le modifiche Explorer elencate sotto.

## Audit e matrice di compatibilità

| Requisito | Fonte verificata nel codice | Limiti |
|---|---|---|
| Universo mercati | matches[].multimarket.matrix.v7.markets | Solo mercati realmente presenti; nessuna nuova probabilità |
| Triade | v7.triad / triad_commitments | Commitment ufficiali invariati |
| Quote e fonte | matrix.rows e v7.markets[].quote | Mancanti/scadute esplicite |
| Ranking | TRIAD_LIMITS, TRIAD_MARKETS e criteri lessicografici di v7_decision.route | Nessun peso numerico inventato |
| Forecast | journal arab_quote_scan_journal_* e auditordata/prematch_* | Checksum e timestamp; nessun backfill di classifiche Explorer |
| Risultati | auditarchive/prematch_DATE_v7.json | Esiti mancanti rimangono non verificati |
| Persistenza | R2: feed Gold, state/current.json e oggetti verificati | Explorer scrive soltanto sotto explorer/ |
| Autenticazione | Worker stripe-access, sessionEmail, access, D1 ACCESS_DB | Cookie OTP e diritto Gold/prova verificati server-side |
| Personali | Nuove tabelle D1 explorer_searches e explorer_favorites | Identità derivata dalla sessione |
| Interfaccia | web/gold-dashboard-live.html | Pannello e pagina nella stessa origine |

Il modello comprende risultati FT, totali PT/ST/FT, GG PT/ST/FT e combinazioni definite in market_scope.COMBO_SPECS. Doppia chance e altri mercati non vengono inventati se assenti dal singolo snapshot.

All'audit iniziale Fast Scan era in corso; i recenti job Fast Backup e recovery erano conclusi con successo. Non è stata avviata alcuna scansione. Questo controllo non certifica la completezza dei dati R2.

## Implementazione locale da recuperare

Nuovi file:
- src/gold_explorer.py
- workers/stripe-access/explorer.mjs
- workers/stripe-access/explorer-schema.sql
- web/explorer-core.mjs
- web/gold-explorer.html
- web/gold-explorer.css
- web/gold-explorer.mjs
- web/gold-explorer-panel.js
- tests/test_gold_explorer.py
- tests/test_explorer_core.mjs
- tests/test_explorer_api.mjs
- tests/browser_gold_explorer.cjs
- docs/gold-explorer-v1.md (audit iniziale, aggiornamento finale interrotto)

File modificati:
- src/gold_storage.py
- workers/stripe-access/worker.mjs
- workers/stripe-access/build_production.py
- workers/stripe-access/production-bundle.mjs
- web/gold-dashboard-live.html
- .github/workflows/night_scan.yml
- .github/workflows/fast_scan.yml
- .github/workflows/publish-site.yml
- .github/workflows/policy_tests.yml

Screenshot QA locali:
test-artifacts/gold-explorer-desktop.png
test-artifacts/gold-explorer-tablet.png
test-artifacts/gold-explorer-mobile.png

Non sono screenshot autentici della produzione: contengono un banner di collaudo e dati sintetici. Non usarli nella promozione. Il tentativo di copiarli in docs/qa non è stato eseguito, perché la chiamata si è interrotta prima.

## Funzioni implementate localmente, da ricontrollare
Pagina completa e pannello Gold; cinque classifiche; filtri combinabili; caricamento di 10 righe; preferiti persistenti e ricerche salvate; controllo server e CSRF; archivi Night/Fast; storico e statistiche per mercato/campionato; confronto ufficiali/alternative e riferimenti temporali; archivio V7 precedente distinto; conservazione delle quote ufficiali originarie e assenza esplicita delle probabilità originarie non documentate.

La proiezione viene eseguita facoltativamente dopo il commit della pubblicazione Gold; gli errori Explorer sono contenuti e non fanno fallire Gold. Nessun nuovo workflow, client calcistico, collector o motore.

I workflow esistenti registrano l'inizio della scansione. I forecast più vecchi non vengono falsamente attribuiti a una nuova Fast. Il congelamento è per kickoff meno 300 secondi; gli episodi con kickoff diverso sono distinti. Le classifiche conservano versioni temporali. Un timer locale rimuove quote scadute/partite iniziate, senza polling.

Gli endpoint leggono soltanto R2 Explorer e D1 personali. Il server verifica SHA256 degli snapshot. I filtri operano in memoria nel browser.
Il seed storico usa esclusivamente gli archivi già ripristinati: python src/gold_explorer.py --seed-current --complete-backfill.
Il comando non è stato eseguito su R2.

## Test realmente eseguiti
- Ultimo blocco completato: 12 test Python Explorer e 23 test Node (15 Explorer + 8 OTP esistenti), tutti superati.
- 10 test storage Gold esistenti superati.
- Suite V7 locale: 33 test riusciti e uno in errore per dipendenza requests assente. Installazione locale tentata senza successo; non dichiarare la suite completamente superata. La CI esistente installa requests.
- Browser con 1.250 e successivamente 10.000 righe sintetiche: cinque classifiche, filtri senza richieste di rete, paginazione, ricerche salvate, preferiti, storico, dettaglio, viewport desktop/tablet/mobile, errore rete, blocco 401. Nessun errore JavaScript.
- Con 10.000 righe il primo caricamento locale misurato era 2,831 secondi. Non è un benchmark Cloudflare.
- Test di proiezione con socket vietati e test endpoint con fetch esterni vietati; test di errore Explorer con pubblicazione Gold che termina correttamente.
- Sintassi e generazione bundle verificate in versioni intermedie.

**Gli ultimi interventi sulle referenze Night/Fast, sul seed e su parti dello storico sono successivi ad alcuni test. Occorre rieseguire tutti i controlli dopo il recupero.** Nessun collaudo con account reali, nessun benchmark R2 completo e nessuna CI di implementazione sono stati conclusi.

## Verifiche ancora necessarie
1. Ripristinare l'accesso all'ambiente locale e verificare che tutti i file siano presenti.
2. Rivedere il diff completo Explorer, in particolare idempotenza, quote originali, import storico e limiti di memoria su grandi archivi.
3. Ricostruire production-bundle.mjs e rieseguire unit test, regressioni e browser test. Usare il Python del runtime primario per tzdata.
4. Trasferire i soli file Explorer sul branch remoto, mantenendo questo checkpoint. Aprire/aggiornare la PR completa con screenshot QA e lista esatta dei file.
5. Attendere CI completa; verificare che non compaiano nuove chiamate API-Football o fonti quote e che il motore e il Publisher rimangano invariati.
6. Collaudo staging con archivi autentici, account attivo/prova/scaduto e isolamento tra dispositivi; verificare memoria/prestazioni e completezza dell'import.
7. Verificare il routing Cloudflare di /api/explorer/* verso lo stesso Worker Gold. Nessun routing è stato modificato.
8. Solo dopo autorizzazione esplicita: merge e deploy, migrazione D1 additiva, flag GOLD_EXPLORER_ENABLED nel Worker e repository, smoke test reali. Non lanciare scansioni aggiuntive per Explorer: attendere le normali Night/Fast.

## Migrazione e rollback
Due tabelle D1 nuove e un indice, nessuna modifica ai piani Stripe o alle tabelle Gold. R2 Explorer separato.
Rollback: disabilitare i flag Worker/repository, rimuovere l'accesso web o ripristinare gli asset e il bundle precedenti; conservare dati personali e snapshot per un rilascio successivo. Non cancellare gli archivi Gold.

## Promozione
Nessun messaggio inviato. Grafica promozionale, screenshot autentici, testi finali e proposta Meta devono essere preparati dopo verifica online. Nessuna promessa di vincita o statistica non verificata. Inclusione nella prova gratuita significa prova valida: non promettere una nuova prova a chi non ne ha diritto. Pubblicazione organica e budget Meta richiedono le conferme indicate nel mandato.
