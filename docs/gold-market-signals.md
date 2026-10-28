# Radar di Mercato — monitoraggio dei prezzi

La categoria blu identifica partite prepartita in whitelist senza una proposta
Gold ufficiale, sulla sola base dei movimenti delle quote. È un'indicazione di
studio, non una nuova giocata. La policy Gold v6.1, le probabilità, `eligible`,
`strict_eligible`, i rank, gli export e i conteggi delle giocate restano invariati.

## Condizioni simultanee

- Uno dei cinque mercati gol v6.1, comprese le linee asiatiche equivalenti.
- **OCI >= +30**, variazione dall'apertura dello stesso operatore **<= -2%**,
  aggio dello stesso operatore fra **0% e 9%**. L'aggio controlla la qualità
  del prezzo; non impone requisiti di EV al Radar.
- Movimento confermato e probabilità senza margine concorde; almeno due
  operatori concordi e maggioranza stretta degli operatori confrontabili.
- Anche la traccia dell'operatore selezionato deve mostrare calo almeno del 2%,
  aumento della probabilità senza margine, persistenza >= 0,5 e nessuna inversione.
- Prezzo recente, oppure cache valida entro le 12 ore previste dalla v6.1.
  Apertura, variazione e quota corrente devono essere coerenti; dati mancanti
  o incoerenti del mercato non attivano la categoria. La quota deve essere
  numerica, finita e maggiore di 1, senza i limiti di rendimento Gold.

**Nessun requisito statistico o di valore**: probabilità distante dalla soglia
Gold, EV negativo, EV prudente insufficiente, storico breve/assente, simmetria
PT, frequenze, gol attesi e anti-picco non bloccano il Radar. Anche l'assenza
completa della valutazione statistica o della decisione Gold è ammessa, se
i dati delle quote consentono di confermare il movimento. Non si applicano
né il precedente limite di 5 punti percentuali né il supplemento di EV prudente
legato all'aggio. Tutti questi vincoli rimangono invariati per le giocate Gold.

La whitelist, i cinque mercati gol e la distinzione prepartita/live restano
invariati: uno storico mancante non blocca una partita già presente nel
palinsesto autorizzato, ma il Radar non aggiunge competizioni escluse.

Queste soglie di monitoraggio sono scelte di prodotto esplicite, non una
calibrazione o una prova di redditività. OCI e variazioni sono indicatori di
prezzo, non misure dei volumi puntati.

## Visualizzazione

Il box esistente diventa blu e mostra:

> Radar di Mercato · Statistica debole o assente · Solo studio

con «Solo studio · esclusa dalle giocate Gold ufficiali», quota osservata,
persistenza, OCI, variazione, aggio e operatori concordi. Il filtro
«Radar di Mercato» conta soltanto queste partite. La matrice conserva la
decisione Gold, quando disponibile, e aggiunge il badge blu
«Radar di Mercato · Solo studio». Non presenta una soglia statistica da
raggiungere per il Radar.

Una proposta Gold valida ha sempre precedenza. Le partite iniziate non vengono
riclassificate retroattivamente. La Combo mantiene il proprio stato indipendente.
Griglia, dimensioni e contenitori della vista restano gli stessi.

Il calcolo è una funzione pura del client: legge i dati del mercato e dà
precedenza a una proposta Gold valida senza modificarla. L'ordine dei soli
segnali è OCI decrescente, calo quota più marcato e
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
