# V7 — relazione statistica, prezzo e margine

`V7_MARKET_RELATION_V1` aggiunge alle analisi le identità verificate nello studio del 20 novembre. Quando la quota scende, separa la variazione del riferimento fair proporzionale dalla variazione dell'aggio; quando il modello supera il fair ma non copre l'aggio, lo spiega senza trasformare l'EV negativo in positivo.

## Comportamento

Per mercati binari con riferimento coerente dello stesso operatore:

```text
f = 1 / [Q × (1 + m)]
EV = p_cal × Q − 1
EV + 1 = (p_cal / f) / (1 + m)
```

Il vantaggio relativo `p_cal / f − 1` deve coprire `m` prima che l'EV diventi positivo. L'aggio non viene sottratto una seconda volta dall'EV. Il fair è una normalizzazione di mercato, non una probabilità vera certificata. Rimborsi, vincite/perdite parziali mantengono il calcolo EV a pesi di liquidazione e non ricevono una soglia binaria fittizia.

Per due osservazioni comparabili dello stesso operatore:

```text
log(Q_prima / Q_dopo)
    = log(f_dopo / f_prima)
    + log[(1 + m_dopo) / (1 + m_prima)]
```

I riferimenti sono preservati da `context_analysis.movement`, che ha già calcolato fair e margini da partizioni complete. `matrix_quotes` conserva i sei valori iniziali/finali e l'identità dell'operatore all'interno di `selected_movement.price_reference`. Raccolta, OCI e scelta della quota usano la logica esistente; non vengono effettuate nuove richieste né nuove letture remote.

La finestra completa è distinta dall'ultimo tratto successivo a un'inversione: `selected_movement.delta_pct` mantiene il suo significato precedente; la nuova scomposizione usa prezzi e fair iniziali/finali della stessa finestra. Prima di mostrarla, V7 verifica identità, operatore, ordine temporale e corrispondenza dell'ultimo riferimento con la quota selezionata. Non ricostruisce dati mancanti da margini mediani, OCI, operatori diversi o quote di apertura scollegate. Le quote stabili/da cache non riattivano conferme di flusso storico.

Le intensità già calibrate espongono inoltre, per PT/ST/FT, volume totale, contributi delle due squadre, intensità del lato più debole e simmetria. Sono descrizioni dello stesso modello: medie, lambda e Poisson non diventano tre voti indipendenti.

## Impatto sulle analisi e sulle selezioni

Le spiegazioni estese vengono aggiunte **dopo la scelta della Triade**, usando stime già calcolate, e sono concentrate sui tre mercati selezionati per contenere dimensione e lavoro. I tooltip di origine già utilizzati dalla Gold mostrano fair dello stesso operatore, soglia di pareggio economico e, quando verificabile, scomposizione del movimento. I riferimenti delle altre quote restano disponibili nei dettagli della matrice per studi successivi.

Probabilità ufficiale, EV centrale, EV prudente, soglie, ranking, `EV_RECOVERY_AGGIO_V1`, Platinum, gate di apprendimento e impegni prematch non vengono modificati. Il ROI non entra nel calcolo. I vecchi snapshot restano leggibili senza i nuovi campi; la loro assenza non viene trasformata in una conferma. La normale scansione successiva aggiunge i dati alle analisi prematch che rigenera; lo storico non viene ripubblicato.

Lo studio ha verificato un'identità matematica; non ha validato un nuovo bonus predittivo. Questo rilascio migliora interpretazione e tracciabilità, senza certificare un aumento delle vincite.

## Validazione

- 19 nuovi test dedicati: identità fair–EV, margine non ancora coperto, nessun doppio addebito, movimento solo da margine, movimento fair puro, prezzo invariato con componenti opposte, inversione, operatori/finestre incoerenti, cache neutrale, liquidazioni non binarie e integrazione della matrice.
- Griglia riproducibile di 150 coppie di prezzi, senza dati futuri o rete.
- Verifica su tutte le 31 coppie storiche disponibili con operatore identificato, appartenenti a 3 partite; non rappresentano 31 osservazioni indipendenti. L'archivio originale non viene modificato.
- Confronto completo prima/dopo su 96 casi sintetici, distinto da un backtest storico: probabilità, prezzi, EV, EV prudente, livelli e selezioni della Triade confrontati per ogni mercato. Nessuna simulazione viene presentata come risultato sportivo reale.
- Controllo AST di 12 funzioni decisionali/statistiche e confronto integrale di Gold, Platinum, apprendimento e statistica di base: invariati.

Il primo tentativo di suite locale ha rilevato dipendenze mancanti nel runtime; sono state installate esclusivamente nell'ambiente locale, senza modificare dipendenze del progetto o workflow. L'esito finale della suite completa e del confronto è riportato nella PR.

Collaudo locale finale: **966 test Python superati** in 115,804 secondi, **20 file di test Node superati**. Le 31 coppie storiche sono tutte verificabili, con errore massimo dell'identità inferiore a `5,6e−16`. Nei 96 casi sintetici il confronto semantico prima/dopo è identico; le 288 selezioni ricevono i dettagli estesi, senza cambi di ruolo o di livello. L'aggiunta media al solo snapshot V7 è circa 4,4 kB per partita nel campione, pari al 4,35%; nessuna nuova stima Poisson o chiamata remota viene aggiunta.

Benchmark locale sequenziale, due ripetizioni per versione sui 96 casi: mediana 10,753 s prima e 10,106 s dopo. È un controllo di sovraccarico, non una dimostrazione di accelerazione o una previsione dei tempi di scansione in produzione.

## Costo, rilascio e rollback

Base verificata: `721a292721a3b794a9510daff2d6e86bddde41c5`, successiva a #241–243; rispetto a #243 aggiunge solo le tre proiezioni pubbliche aggiornate. Nessuna modifica ai workflow, ai budget o a servizi esterni.

Su questi percorsi la PR attiva solo `Selection policy tests`; il merge attiva gli stessi test e registra l'evento Fast. Il titolo del merge contiene `[night-before-fast]`: il guard esistente esclude il job Fast su questo push. Night, Auditor, Publisher e deploy del sito non hanno percorsi compatibili con questa modifica. Non viene eseguito alcun dispatch. La previsione è circa 6–8 minuti runner fra PR e merge, basata sui test precedenti da circa 3 minuti ciascuno; il saldo di fatturazione non è esposto dalla connessione disponibile.

Rollback: revert della sola PR, mantenendo `[night-before-fast]` nel commit di revert per rispettare il guard esistente. Il calcolo precedente resta operativo e i nuovi campi eventualmente già archiviati restano dati descrittivi; non occorrono cancellazioni, ripubblicazioni o modifiche a R2. Tutte le correzioni precedenti restano nel ramo principale.
