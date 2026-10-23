# Audit delle fasce probabilistiche — 21 e 22 ottobre 2026

Analisi retrospettiva delle fasce, congelata il 23 ottobre. Modello context-goals-v3.4,
ultima osservazione prematch disponibile per partita. Tutti i mercati delle partite
archiviate, anche quando non erano la proposta principale. Non tutte le partite
mondiali. Due risultati restano pendenti e non sono perdite. Le fasce sono state
esaminate DOPO entrambi i giorni: nessuna è un ottimo validato. ROI teorico a quota
registrata e puntata uniforme; quote assenti escluse. Non sommare gruppi sovrapposti.

Riproduzione:

```sh
python auditor/range_review.py 2026-10-21 2026-10-22 --output docs/research/range-review-2026-10-21-22.json
```

Il JSON conserva provenienza/hash dei report, conteggi per giorno, quote utilizzabili,
fasce bidimensionali, esiti positivi e negativi delle partite prossime al filtro.

## Le due fonti

Book: per ogni operatore con mercato completo e aggiornato, probabilità fair
Over = (1/quota Over) / [(1/quota Over) + (1/quota Under)]. Si prende la mediana
fra operatori per ogni esito e si rinormalizza. Il modello context usa quote con
timestamp verificabile non più vecchie di sei ore. La quota esposta è una quota
osservata, non necessariamente il reciproco della probabilità fair aggregata.

Statistica: fino a otto gare ufficiali della stagione corrente per FT, quattro primi
tempi validi per PT. Medie fatti/subiti e casa/trasferta pesate per copertura; xG fino
al 25% del componente FT in proporzione ai dati. Correzione contestuale FT limitata
al 5%. Il modello Poisson trasforma i gol attesi in probabilità. Le quote non entrano
nel calcolo statistico. I tiri in porta sono un riscontro qualitativo, non un bonus.
Drop, inversioni e OCI descrivono il mercato: non sono prove indipendenti né un
incremento automatico della probabilità. La vicinanza fra fonti non le rende due
esperimenti indipendenti.

## Over 2,5: gruppi confrontabili

| Gruppo | 21 ottobre | 22 ottobre | Totale | ROI teorico totale |
|---|---:|---:|---:|---:|
| Tutte le partite valutabili | 37/63 | 36/58 | 73/121 (60,3%) | +6,79% su 117 quote |
| Oro: entrambe >50%, distanza ≤10 punti | 18/24 | 23/32 | 41/56 (73,2%) | +15,13% |
| Statistica 60–<70%, book 50–<60% | 5/5 | 7/8 | 12/13 (92,3%) | +55,85% |
| Statistica 50–<60%, book 60–<70% | 2/2 | 6/7 | 8/9 (88,9%) | +30,67% |
| Entrambe 50–<60% | 7/11 | 5/8 | 12/19 (63,2%) | +7,63% |
| Statistica 50–<60%, book 40–<50% | 2/6 | 1/2 | 3/8 (37,5%) | −23,25% |
| Entrambe 40–<50% | 1/6 | 1/3 | 2/9 (22,2%) | −49,67% |

Sono le fasce osservate, con campioni piccoli, non probabilità future. La statistica
60–<70% da sola produce 19/22; ≥70% soltanto 10/19. Non c'è una relazione monotona
validata fra percentuale stimata e risultati. Non alzare artificialmente le stime.

Il precedente 30/38 del filtro oro riguardava SOLO Over 2,5 scelti come primari.
Il 41/56 comprende anche Over 2,5 presenti come alternative. Prima/ultima previsione
non devono essere sommate. Serve registrare separatamente la nuova vista del sito
rispetto alla proposta originaria del motore.

## Escluse vicine: verificare anche gli errori

Fascia di osservazione, definita a posteriori: entrambe ≥47%, distanza ≤13 punti,
fuori dall'oro attuale. Risultato 4/6 e 2/3, totale 6/9, ROI +17,33%.

| Partita | Statistica | Book | Over 2,5 |
|---|---:|---:|---|
| FC Gifu – Sagamihara | 47,462% | 53,817% | Riuscito |
| Roasso Kumamoto – Gainare Tottori | 49,105% | 54,606% | Riuscito |
| Avispa Fukuoka – Gamba Osaka | 55,021% | 47,154% | Riuscito |
| Mixco – Municipal | 57,885% | 49,591% | Non riuscito |
| Shonan Bellmare – Iwaki | 51,169% | 47,735% | Non riuscito |
| Al Ahly – Olympic El Qanah | 48,954% | 55,060% | Riuscito |
| Jagiellonia – Anderlecht | 49,415% | 54,145% | Riuscito |
| Olympiakos – Sparta Praha | 49,602% | 54,451% | Non riuscito |
| Plzen – Levski Sofia | 47,044% | 54,781% | Riuscito |

Il limite rigido separa valori quasi uguali: è una ragione per osservare una zona
vicina alla soglia, non per promuoverla dopo aver visto i risultati. Una previsione
40% può riuscire senza che manchi un parametro; per dimostrare una sottostima serve
un errore sistematico su più giornate con dati anteriori al calcio d'inizio.

Limiti verificati nel codice: forza degli avversari storici non normalizzata;
campioni PT brevi; xG/tiri spesso mancanti; indipendenza Poisson fra squadre/tempi.
Non è dimostrato quale limite causi le singole esclusioni. Formazioni, assenze e
contesto tattico richiederebbero fonti prematch storiche prima di poterli testare.

## Over 1,5 PT: problema di selezione e calibrazione

Su tutte le partite: 17/63 e 22/58, totale 39/121. Nelle proposte principali: 0/9.
Il modello riconosce male quali partite scegliere in questo campione. La soglia
>50% su entrambe le fonti, adatta allo studio FT, non trasferisce automaticamente
la sua utilità al PT: nessun caso supera l'oro PT in questi due giorni.

Senza l'avviso archiviato di media sensibile alla gara più prolifica: 5/11 e 5/10,
totale 10/21 (47,6%). Con avviso: 29/100 (29%). È un candidato da verificare, non
una regola pronta. L'assenza dell'avviso in cons non prova stabilità: il report
conserva fino a tre controindicazioni; prima di un filtro operativo serve ricalcolare
la caratteristica dai profili prematch congelati. Non eliminare a posteriori la gara
più prolifica dallo storico, né utilizzare tiri della partita stessa come input prematch.

## Decisione sulla pagina

Vista principale Over 2,5 per ogni partita, con pallino oro invariato; se il mercato
manca, indicazione esplicita. Vista Consigliate Over 3,5 solo quando il motore lo ha
già selezionato per la preferenza quota scelta. Nessuna sostituzione arbitraria di
una proposta PT con un Over 3,5. PT fuori dalle proposte principali; dati e dettaglio
rimangono per ricerca. Nessuna modifica a modello, probabilità o archivi congelati.
