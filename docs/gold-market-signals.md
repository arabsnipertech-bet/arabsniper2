# Segnali di Mercato — monitoraggio visivo

La categoria blu identifica esclusivamente partite prepartita in whitelist, con
regole `gold-goals-v6.1`, senza una proposta Gold ufficiale. È un'indicazione di
studio, non una nuova giocata. La policy, le probabilità, `eligible`,
`strict_eligible`, i rank, gli export e i conteggi delle giocate restano invariati.

## Condizioni simultanee

- Uno dei cinque mercati gol v6.1, comprese le linee asiatiche equivalenti.
- Motivo di esclusione del backend: `probability`, con eventuale `margin`
  esclusivamente per aggio fra 8% e 9%. Probabilità sotto il
  minimo Gold del mercato di non più di **5 punti percentuali**.
- Tutti gli altri controlli Gold superati: qualità e storico PT, struttura,
  simmetria/frequenze, anti-picco, quote nel range, EV e scenario prudente,
  divergenza. Non si recuperano mercati con storico o dati mancanti.
- **OCI >= +25**, variazione dall'apertura dello stesso operatore **<= -2%**,
  aggio dello stesso operatore **<= 9%**. Da 6,5% resta richiesto il
  supplemento di 1,5 punti percentuali di EV prudente, esteso anche alla
  fascia 8–9%. Il limite delle giocate Gold rimane 8%.
- Movimento confermato e probabilità senza margine concorde; almeno due
  operatori concordi e maggioranza stretta degli operatori confrontabili.
- Anche la traccia dell'operatore selezionato deve mostrare calo almeno del 2%,
  aumento della probabilità senza margine, persistenza >= 0,5 e nessuna inversione.
- Prezzo recente, oppure cache valida entro le 12 ore previste dalla v6.1.
  Apertura, variazione e quota corrente devono essere coerenti; dati mancanti
  o incoerenti non attivano la categoria.

Queste soglie di monitoraggio sono scelte di prodotto esplicite, non una
calibrazione o una prova di redditività. OCI e variazioni sono indicatori di
prezzo, non misure dei volumi puntati.

## Visualizzazione

Il box esistente diventa blu e mostra:

> Segnale di mercato forte (Statistica insufficiente / Mercato concorde)

con «Solo studio · esclusa dalle giocate Gold ufficiali», probabilità/minimo,
quota osservata, OCI, variazione, aggio e operatori concordi. Il filtro
«Segnali di Mercato» conta soltanto queste partite. La matrice conserva lo
stato di scarto e aggiunge il badge blu «Segnale di mercato · Solo studio».

Una proposta Gold valida ha sempre precedenza. Le partite iniziate non vengono
riclassificate retroattivamente. La Combo mantiene il proprio stato indipendente.
Griglia, dimensioni e contenitori della vista restano gli stessi.

Il calcolo è una funzione pura del client: usa le decisioni del backend senza
scriverle. L'ordine dei soli segnali è OCI decrescente, calo quota più marcato e
ID stabile; non modifica l'ordine delle giocate ufficiali. Gli equivalenti
asiatici della stessa linea non raddoppiano i mercati in monitoraggio.

## Risultato del primo tempo

Nell'intestazione della scheda compare il punteggio PT ricevuto dal feed live:
durante `1H` è indicato come «in corso», dall'intervallo come «concluso».
Il punteggio del secondo tempo non sostituisce quello del PT. Se manca un
risultato PT confermato, si mostra «dato non disponibile», senza dedurlo dal
punteggio totale o trasformare un valore mancante in 0–0.

Un risultato PT confermato resta disponibile se una successiva rilevazione lo
omette; eventuali correzioni successive del fornitore hanno precedenza. La riga
si aggiorna con il polling live esistente, senza nuove chiamate al fornitore,
ricaricare la scheda o modificare le stime e le quote prematch.
