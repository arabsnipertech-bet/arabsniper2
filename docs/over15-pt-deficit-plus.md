# Preferenza limitata per Over 1,5 PT

`OVER15_HT_SINGLE_DEFICIT_V1` riguarda solo il mercato semplice Over 1,5 PT,
nel ruolo Consigliata. Confronta le ultime due gare documentate di ciascuna
squadra con le precedenti 4–6, esclusivamente sui gol segnati nel primo tempo.
Sono necessari almeno sei precedenti per entrambe, punteggi PT coerenti con FT,
date note e input chiusi prima del riferimento statistico. Duplicati non creano
nuove gare. L'eventuale inserimento della partita target o di dati successivi
disattiva la preferenza. Le nazionali sono escluse come nello studio storico.

La preferenza è disponibile soltanto se esattamente una squadra è sotto media
e l'Over 1,5 PT è già idoneo con quota documentata, supporto statistico, EV
positivo ed EV prudente non negativo, nelle fasce correnti della Consigliata.
Non modifica nessun criterio di ammissione e non rende idoneo un mercato scartato.

Il limite è `0.005` nella sola componente EV dell'indice di ordinamento:
al massimo mezzo punto di indice e solo tra candidati con la stessa priorità
di nicchia. Non è un aumento di probabilità, EV, EV_low, lambda, quota equa o
qualifica. Non si applica a GG, altri Over, combo, studi senza quota o recuperi
con EV negativo. I valori originali restano quelli mostrati, salvati e valutati.
`selection_context` conserva medie, date, identificativi dei precedenti e policy;
la motivazione Gold/Explorer espone «Plus PT» e il confronto osservato.

Il cap è una scelta prudenziale d'implementazione richiesta dall'utente, non una
stima dell'effetto né una soglia ottimizzata sul ROI. Lo studio del 22 novembre
trova un beneficio esplorativo minimo della distinzione nessuno/uno/entrambi
e incertezza compatibile con zero. La policy resta separatamente disattivabile
con `POLICY['enabled'] = False` per i confronti successivi.

Rilascio: usare il normale ciclo prematch, con il marcatore esistente
`[night-before-fast]` per evitare un ricalcolo aggiuntivo da push. Nessuna nuova
scansione, chiamata provider o modifica dei calendari per attivare questo plus.
I test verificano l'elezione a piccolo scarto e l'invarianza dei prezzi e delle
probabilità; gli snapshot di partite iniziate restano quelli già registrati.
