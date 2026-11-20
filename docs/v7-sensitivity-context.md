# V7: coerenza delle prove prudenziali

## Difetto e correzione

Le prove di sensibilità eliminavano una gara dalla stima dei lambda grezzi,
ma V7 riutilizzava le medie ST dell'intero campione e la proiezione di mercato
calcolata con quelle statistiche. La gara esclusa poteva quindi contribuire
ancora alla probabilità dello scenario prudente.

Ogni prova ora conserva il legame fra lambda e osservazione esclusa. V7
ricalcola le medie PT/ST/FT dalla stessa finestra residua e rifà la proiezione
con le **stesse quote osservate e lo stesso peso calibrato**. Non vengono
aggiunti scenari: resta il confronto senza H2H e l'esclusione di una gara
storica alla volta. Le probabilità centrali, le quote e gli EV centrali non
cambiano; possono cambiare i limiti prudenziali e le decisioni che li usano.

I contesti verificati e gli eventuali fallback sono registrati nel nuovo
campo `sensitivity` dello snapshot. Evidenze precedenti prive dell'identità
degli scenari mantengono il comportamento precedente e sono dichiarate
incomplete. Non vengono inventati né ricostruiti risultati mancanti.

## Caso segnalato il 20 novembre 2026

La pagina Gold mostrava 79 fixture e una sola scheda Gold. La Consigliata
di Sumqayıt–Sabah FA era GG ST: probabilità 34,3%, quota 3,25, EV +11,5%,
aggio 6,0%, OCI +71,3 e quattro osservazioni ST per squadra. Il risultato
mostrato era 1–5 al 90', con PT 1–2. Il GG FT risultava già riuscito.

La policy vigente ammette GG ST da 32% e quota 2,15–3,50 con EV positivo e
EV prudente almeno +2%. La maggiorazione per saturazione OCI si applica
solo agli Over FT. Questi sono vincoli preesistenti, non una prova che il
34,3% sia la probabilità reale. Una quota 3,25 con probabilità stimata 34,3%
può avere valore pur restando un evento meno probabile del suo contrario.

Questa patch non sostiene che il nuovo controllo avrebbe escluso quel GG ST:
la sola schermata non contiene i lambda di tutte le esclusioni storiche.
Non cambia retroattivamente la previsione della partita iniziata. Il singolo
esito non viene usato per scegliere soglie, pesi o mercati da vietare.

## Collaudo e limiti

I test verificano il confronto con una nuova analisi completa dopo la stessa
esclusione, la conservazione dei valori centrali di tutti i mercati, il
legame fra scenario e gara, i dati ambigui o mancanti e il fallback esplicito.
Un caso sintetico dimostra il superamento improprio della soglia prudente
Gold; non è un risultato economico né una ricostruzione della partita reale.

Collaudo locale: suite completa di 947 test Python superata in 98,202 secondi;
7 test mirati superati anche dopo il rafforzamento della verifica di ammissione
Gold; tutti i 20 file di test Node superati. Nel caso completo GG ST a quota
2,75 il solo nuovo controllo di sensibilità revoca l'idoneità Gold, lasciando
identici probabilità centrale e prezzo.

Il benchmark locale conserva il numero di modelli di sensibilità. I tempi
sono una misura indicativa su un caso sintetico, non una previsione dei
minuti totali del Fast. Nessuna chiamata di rete viene aggiunta al motore.

Restano invariati ROI come solo controllo successivo, EV_RECOVERY_AGGIO_V1,
probabilità ufficiali V7, soglie e ranking dei ruoli, requisiti Platinum e
gate di apprendimento. Nessuna modifica a raccolta quote, workflow,
Homepage, social, Publisher o infrastruttura.

## Rilascio e rollback

Usare il marcatore esistente `[night-before-fast]` nel commit e nel merge:
il Fast da push viene saltato e il Publisher conseguente non viene eseguito.
Sono previste solo le CI di PR e main, circa 5–6 minuti runner complessivi
sulla base delle due esecuzioni del rilascio precedente; saldo di budget
non leggibile tramite i permessi disponibili. Nessun dispatch manuale.

La correzione si applica alle nuove analisi dei normali cicli programmati.
Una scansione già avviata può usare il codice precedente. Le fotografie
prematch già congelate restano tali. Rollback tramite revert della patch,
con lo stesso marcatore: nessuna migrazione o ripubblicazione storica.

La correzione chiude un difetto di robustezza; non certifica un aumento delle
vincite e non conclude la validazione della selezione per il weekend.
