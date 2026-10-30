# Copertura supplementare e cache degli storici — 30 ottobre 2026

## Cambiamento

`odds_collection.coverage()` normalizza esplicitamente il payload prima sia di
`collect_quotes()` sia di `direct_combo_groups()`. Tutte le verifiche di inclusione
usano la copertura completa: un supplemento parziale non può cancellare una linea
già utilizzabile. La copertura del risultato accettato è riusata fra tentativi;
`merge_payload()` copia una sola volta i sottoalberi selezionati, senza duplicare
interi bookmaker e l'intera risposta per ogni mercato. Gli input restano intatti.

Precisazione sulla base precedente: `collect_quotes()` normalizzava già le quote
al proprio interno e il lettore Combo applicava già il Discovery. L'allineamento
esplicito, da solo, non dimostra un aumento di copertura asiatica. Il cambiamento
funzionale aggiuntivo è l'ammissione dei qualificatori neutri nei nomi dei mercati.

Discovery v2 accetta `main`, `alternative`, `alt`, `regular`, `regulation`,
`time`, `standard`, `market(s)`, `full` e la dicitura `2-way`/`2 way`/`2way`.
Quest'ultima viene rimossa prima di estrarre una linea numerica: il numero 2 non
diventa una falsa linea gol. I blocchi EXCLUDED, i controlli sul periodo e sui
totali di squadra restano attivi. Le combo con `and`/`or`, corner, cartellini,
minuti parziali, supplementari e mercati ambigui continuano a essere respinti.
Anche i due estrattori di base usano il payload normalizzato, senza cambiare le
regole `usable_for_signals` o il minimo di bookmaker. Al passaggio da Discovery
v1 a v2 si azzerano solo le pause/cooldown delle interrogazioni supplementari;
quote, prime osservazioni, ledger, filtri e budget API restano preservati.

## Cache persistente

Il nuovo `src/history_cache.py` è collegato a `APIClient.get()` in `3appdays.py`.
In questo modo comprende anche `get_team_recent_matches()` installata dal motore
attivo in `statistics_integration.py`, oltre a H2H e baseline di campionato.

- File: `data/arab_quote_history_cache.json`, incluso nel checkpoint privato e
  ripristinato sui successivi runner. La rappresentazione supporta gli archivi
  suddivisi in parti già usati dallo storage.
- TTL: 12 ore dalla risposta HTTP riuscita. Le letture non prolungano il TTL.
- Chiavi: endpoint e parametri esatti, compresi squadra, coppia H2H,
  campionato/stagione e numerosità del campione.
- Si conservano le risposte grezze; modello, filtri temporali, statistiche e
  probabilità sono ricalcolati a ogni ciclo.
- Il Night aggiorna gli storici da HTTP e prepara la cache; Fast e Today
  riusano le voci valide. Anche il primo Fast a cache vuota la prepara.
- Prima del riuso si interrogano 2–3 date di calendario italiano. Il giorno
  precedente è incluso per le partite iniziate prima di mezzanotte e terminate
  dopo la creazione della cache. Non si eseguono controlli separati per squadra.
- Una nuova partita terminata (`FT`, `AET`, `PEN`) o una correzione del risultato
  invalida solo le squadre interessate, le loro coppie H2H e il campionato/stagione
  corrispondente. Il calendario aggiornato letto dal ciclo alimenta gli stessi
  controlli. Partite in programma, in corso o rinviate non invalidano gli storici.
- Se il calendario non è verificabile, si torna agli scarichi storici HTTP.
  Risposte in errore o incomplete non diventano cache valida.
- Le quote e le statistiche delle singole fixture non entrano in questa cache.

I contatori `api_health.history_cache` distinguono hit, miss, scadenze,
invalidazioni, richieste storiche e richieste/errori di verifica del calendario.
Sono scritti anche nel log e nella cronologia privata dei consumi degli scan.

## Riscontri

Benchmark locale simulato, senza nuove partite terminate fra i due cicli:

| Misura | Primo ciclo | Ciclo successivo |
| --- | ---: | ---: |
| Richieste storiche: 960 squadre + 480 H2H + 60 campionati | 1.500 | 0 |
| Verifiche del calendario | 2 | 2 |
| Totale per la sola acquisizione degli storici | 1.502 | 2 |

Sul payload simulato di una partita con 8 bookmaker e 20 mercati ciascuno,
15 ripetizioni del merge restituiscono payload identici: mediana precedente
26,345 ms, nuova 2,370 ms, circa 11,12 volte più veloce. È una misura locale
del solo merge, non dell'intero scan o della latenza del provider.

La regressione include i 430 test preesistenti e 21 nuovi casi: recupero asiatico
e ID alternativi, qualificatori neutri ed esclusioni, protezione dei mercati
completi, immutabilità, cache fra processi/runner, invalidazione selettiva,
correzioni risultati, scadenza, mezzanotte/ora legale, errori e passaggio
Night → Fast. Totale: 451 test Python.

Riferimento di produzione precedente a questa modifica:
[Fast 41086606504](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/41086606504),
30/10/2026, job 17:11:47–17:38:50 Europe/Rome: 27m03s complessivi,
di cui 19m32s nel passo `Run Fast Scan`. Il distacco dall'Auditor aveva dunque
già ridotto la durata rispetto ai vecchi cicli da circa 66 minuti.

Non è ancora misurato il tempo di un Fast con questa cache in produzione.
Il primo ciclo deve popolarla; il confronto utile parte da un ciclo successivo
che ripristina il checkpoint e riporta hit. Le 1.500 chiamate evitate sono il
risultato del benchmark descritto, non una misura già acquisita sul feed live.
