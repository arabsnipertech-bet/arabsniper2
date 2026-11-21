# Triade: scelta recente prima del calcio d’inizio

21 novembre 2026. `TRIAD_RECENT_SELECTION_V1`, implementata in
`src/v7_recent_selection.py` e applicata da `v7_decision.capture` alle pubblicazioni
effettive della bacheca, sostituisce il blocco precoce delle prime scelte quotate.

Ogni Fast prima del limite di un’ora dal kickoff rielegge **tutti e tre i ruoli**
con le stime e i prezzi disponibili nella scansione. Un precedente miglioramento
in un altro ruolo non impedisce la nuova elezione. Anche una scelta sana può
lasciare posto alla candidata che vince il ranking corrente. Se lo stesso mercato
rimane primo, viene registrata la conferma effettiva della nuova scansione, con la
relativa quota: non resta una finta data di fissaggio di diversi giorni prima.

Fino al limite le scelte sono esplicitamente provvisorie. Esattamente a T−60 è
ancora ammessa una nuova elezione; dopo il limite si conservano mercato, quota e
data dell’ultima selezione valida. I numeri della matrice possono continuare ad
aggiornarsi, mentre la quota alla scelta rimane nello storico della conferma.

La selezione finale deve provenire dalle **ultime tre ore prima del kickoff**;
sono verificate anche le date reali di osservazione e aggiornamento del provider.
Una cache riletta oggi non rende recente un prezzo vecchio. La finestra copre due
slot Fast ordinari da 90 minuti, ma non promette una scansione esattamente a T−60.
Non si aggiungono calendari, workflow, richieste API-Football o servizi.

Nella finestra recente, i prezzi più vecchi restano consultabili nella matrice
completa e nei journal, ma non possono vincere la nuova elezione. Le stime senza
quota restano esplicitamente Studio e non costituiscono una conferma quotata.
Se prima del limite manca una selezione recente, o la prima analisi arriva
nell’ultima ora, la pubblicazione lo dichiara e rimane Studio: non attribuisce una
conferma al passato né promuove quella scelta ai segnali Gold/Platinum o alle
offerte operative. Gli slot mancanti non vengono riempiti con nuovi mercati
dopo la scadenza. I primi snapshot legacy già nell’ultima ora rimangono leggibili,
senza diventare conferme retroattive della nuova politica.

Le pubblicazioni precedenti sono immutabili nei journal. Quando un mercato
cambia, `replaces` conserva il precedente impegno completo; l’avvio della nuova
politica conserva anche l’originale legacy. Confermare lo stesso mercato non
allunga inutilmente la catena. Il kickoff diverso invalida la vecchia prova.
Le partite iniziate non vengono ricalcolate da questa modifica.

La Gold distingue «Scelta provvisoria rivalutata», «Mercato confermato», «Scelta
precedente» e «Studio tardivo». Il bundle ha un nuovo hash per aggiornare anche
i browser con risorse in cache. I vecchi snapshot mantengono la loro lettura.

Le formule, lambda, probabilità, EV, EV prudente e il ranking dei ruoli restano
identici. Rimangono i controlli di compatibilità, le alternative qualificate di
#247, il recupero EV–aggio e i requisiti Gold/Platinum. Il ROI non entra nella
scelta. Le etichette «Ritmo», «Trend» e «Valore +EV» restano spiegazioni degli
indicatori; questa patch riguarda il momento di elezione, non introduce pesi
appresi dagli esiti degli screenshot.

## Verifica e rilascio

I test verificano rielezioni ripetute, conservazione dell’originale, conferme senza
crescita della catena, T−60 incluso e minuto successivo escluso, prezzi vecchi o
futuri, provider non aggiornato, scansioni mancanti, primo arrivo tardivo,
posticipi, formule invarianti, blocco delle offerte tardive e testi della Gold.
Sono prove cronologiche e strutturali, non un backtest di rendimento.

Il collaudo PR usa la CI esistente. Il merge con `[night-before-fast]` evita una
scansione aggiuntiva da push; la normale CI main e la pubblicazione del sito
rimangono attive. Non usare `[skip ci]` su questo rilascio, perché salterebbe anche
la pubblicazione automatica dell’interfaccia. La nuova elezione viene prodotta
dal primo Fast ordinario che utilizza il nuovo main, senza cambiare lo storico.

Rollback: revert di questi file, conservando `[night-before-fast]` nel commit.
In alternativa `POLICY['enabled'] = False` ripristina il percorso di elezione
precedente; i nuovi campi archiviati restano prove leggibili, senza migrazioni.
