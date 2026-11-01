# Architettura V5.1 — ciclo di validazione congelato

Configurazione: `validation_freeze_v5.1_2026-11-01`. Inizio 1 novembre 2026;
check intermedio dopo 7 giorni, verifica completa dopo 14 giorni. Il freeze
riguarda codice, mercati, soglie e formule: gli aggiornamenti notturni dei pesi
previsti da queste formule restano attivi. Non garantisce un miglioramento
entro una data: dipende da esiti chiusi, copertura e numerosità indipendente.

## Ciclo e certificazione

Fast/Night → matrice e decisioni → freeze prematch immutabile → Auditor nella
finestra **00:30–05:30 Europe/Rome** → pesi pubblicati → scan successivo.
Nessuna previsione viene ricalcolata a posteriori per migliorare il benchmark.
I batch di pubblicazione congelano anche la matrice finale dopo il refresh,
così Gold, Platinum, Radar e probabilità corrispondono alla proposta pubblicata.

`data/arab_calibration_weights.json` viene conservato sia in Git sia nel
checkpoint privato. Il ripristino conserva la pubblicazione certificata più
recente in base a `trained_at`, inclusa la migrazione dai vecchi checkpoint.
Il loader controlla modifica e dimensione del file prima di riusare la cache:
una sostituzione atomica viene letta anche nello stesso processo. Non elimina
le coorti sotto soglia. Versioni sconosciute/file illeggibili usano lo stato
neutro; dati futuri o più vecchi di due date di Roma non attivano pesi appresi.

Matrici, day feed e dashboard espongono `calibration_status`: versione, data,
coorti utilizzabili (anche tramite fallback), coorti micro in accumulo,
`mismatch_shrinkage_factor`, ciclo e `certified_current`. In assenza di pesi
correnti rimane solo il prior prudente di shrinkage 0,15, dichiarato come non
certificato: non viene spacciato per apprendimento. `sub_threshold_progress`
conserva n, Brier, log-loss, ROI, CLV e numero di coppie anche durante il cold start.
Il marcatore dell'Auditor include lo stato della calibrazione dopo la fase pesi.

## Correzione dei lambda

Il Poisson originale rimane in `pure_poisson_rates` e nei campi `pure_poisson`
delle righe modificate. Con un intero 1X2 dello stesso operatore, fresco,
aggio fra 0 e 9%, favorita fair ≥58% oppure quota ≤1,60, e divergenza assoluta
oltre 15 pp fra Poisson e fair sulla favorita, si stima per bisezione una
ripartizione dei lambda compatibile con il prior di mercato.

Si conserva λ_home + λ_away: il solo 1X2 non identifica un nuovo totale gol.
La quota di gol della favorita viene spostata verso quella stimata con fattore
α ∈ [0;0,35], inizialmente 0,15. Le frazioni PT/FT di ciascuna squadra vengono
conservate e PT ≤ FT. Si rigenera la matrice congiunta, comprese le sensibilità,
prima di applicare il blend di probabilità e le policy. Nessuna moltiplicazione
successiva dei lambda già corretti: ogni refresh parte dagli originali.
Assenza di quote, quote incomplete/stale o match iniziati non attivano nuovi
ancoraggi. Il Poisson grezzo resta separato dalla previsione calibrata nel freeze.

Le trappole soppresse con `model_market_divergence` alimentano un apprendimento
separato: primo contratto per fixture, solo risultati chiusi prematch. Con almeno
25 fixture, ROI negativo oppure Brier grezzo peggiore del fair, α cresce di
`0,02 * n/(n+50)` fino a 0,35. L'impronta dell'evidenza impedisce incrementi
ripetuti nello stesso batch; senza nuove fixture non cresce neppure quando
vecchie osservazioni escono dalla finestra. La diagnostica include anche le trappole vincenti.
Questa regola apprende l'intensità dell'ancoraggio; non è ancora una stima diretta
della forza di ogni avversario o della qualità relativa di ogni campionato.

## Fallback gerarchico e blend

Finestra mobile di 28 date chiuse di Roma, emivita 14 giorni. Le osservazioni
ripetute e i tre esiti 1X2 nella stessa cella non moltiplicano il numero di gare.
Le fasce di training restano [1;1,8), [1,8;2,5), [2,5;4), [4;+∞).

1. Micro-coorte `market|band|tier`: piena attivazione da **50 fixture**.
2. Sotto soglia: macro-coorte `market|band`, con **25 fixture distinte** anche
   attraversando i tier; shrinkage verso zero con **PRIOR=50** e pesi temporali.
3. Macro sotto 25: blend neutro. Il progresso rimane sempre visibile.

Le fonti empirica e fair guadagnano peso solo se migliorano il Brier sullo stesso
campione rispetto al Poisson congelato. Budget condiviso ordinario 0,03. Sale a
0,06 se il miglioramento del Brier supera il suo errore standard e il ROI supera
0,5% e il proprio errore standard, oppure vi sono almeno 25/50 coppie CLV (secondo
il livello), CLV >0,1% e sopra il proprio errore standard. La variazione di
probabilità complessiva, bonus inclusi, è limitata a 3/6 pp. Le soglie operative
restano adattabili solo con 50 coppie micro: il fallback macro non rilassa i gate.
Somma 1 per 1X2, complementarità binaria e ordine degli eventi nidificati sono
preservati. Il riferimento Over 0,5 PT resta fuori dalle 58 righe UI.

ROI: rendimento teorico di un'unità alla quota congelata, non profitto eseguito.
CLV: quota iniziale/ultima quota prepartita dello stesso operatore e contratto −1;
richiede timestamp successivi e chiusura nelle ultime tre ore. Un CLV assente
resta nullo. Quote sintetiche e campioni retrospettivi non allenano i pesi.

## four_level_benchmark

I JSON giornalieri `prematch_YYYY-MM-DD_daily.json`, il Markdown relativo e il
cumulativo `arab_quote_prematch_report.json` espongono questa sezione:

- Fair bookmaker: Brier e log-loss.
- Poisson base congelato: Brier, log-loss, ROI teorico.
- V5.1 calibrato congelato: Brier, log-loss, delta Brier vs base e CLV.
- Operatività distinta: Gold Certificate, Platinum Certificate, Radar stella,
  tutti gli spunti visibili, trappole soppresse. Numero contratti e fixture,
  hit rate, quota media, ROI e copertura CLV; Gold 1,70–1,85 isolato.

I primi tre livelli usano esattamente gli stessi contratti, fixture, timestamp,
quote e risultati; probabilità mancanti sono escluse esplicitamente. I mercati
con rimborsi non vengono forzati in un Brier binario. Le categorie operative
usano le loro prime scelte congelate sulle gare chiuse: possono sovrapporsi,
non vanno sommate e non sono campioni causalmente comparabili. Sono riportati
mercati e fasce quota 1,40–1,69; 1,70–2,09; 2,10–3,50; 3,51–4,50. Gli esiti fuori
fascia restano nel totale e nelle diagnostiche, mai eliminati per abbellirlo.

Le trappole mostrano scarti chiusi, perdite evitate, tasso di salvataggio, ROI
ipotetico e saldo evitato in unità **comprendendo le vincite escluse**. Gli spunti
senza quota hanno conteggio e hit rate ma non ROI. Giorni pendenti non sono
perdite. I gruppi per mercato comprendono GG FT/PT, linee gol PT/FT e combo.
Il confronto di metà mese deve distinguere disponibilità di evidenza,
calibrazione probabilistica e rendimento: un test verde non dimostra profitto.
