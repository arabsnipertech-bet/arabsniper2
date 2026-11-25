# V7 — misurazione prospettica del 25 novembre 2026

Questa release registra prove aggiuntive. Non modifica probabilità, intensità,
peso base mercato 0,80, soglie, ordinamento, ruoli o selezioni ufficiali. Non
contiene chiamate al provider o invii social. Il rilascio usa il Fast ordinario;
il marker `[night-before-fast]` impedisce un Fast aggiuntivo al push.

## Fotografia e certificazione

Il protocollo `V7_RESEARCH_TRACE_20261125_V1` viene aggiunto soltanto dentro una
nuova cattura prematch dal 25 novembre. Nessun archivio viene ricostruito o
riscritto per aggiungere il protocollo a fotografie esistenti.

Una scelta appena fissata conserva P V7, quota, `captured_at`, fixture, kickoff,
quote e le due probabilità precedenti al mercato. Il suo
`prematch_evidence_sha256` certifica il corpo elencato in
`measurement.evidence_fields`, prima dell'aggiunta dei metadati. Il normale
`sha256` certifica l'intera fotografia, inclusi questi metadati. Il relativo
`forecast_sha256` viene inserito nella scelta esportata e trasportato nelle
successive osservazioni, senza creare un hash che contiene se stesso.
Le scelte precedenti prive di P restano prive di P: niente riempimenti dal futuro.

| Evidenza | Posizione nella nuova fotografia |
| --- | --- |
| Poisson grezzo / vettore statistico / V7 finale | `measurement.probability_origins` |
| P delle tre origini, fair, campioni, tenuta senza picco | `measurement.markets[market]` |
| Sei campi di flusso e provenienza | `flow_fields`, `flow_origins`, `flow_states` |
| Dati assenti / zero / test falso | `missing` / `neutral` / `failed` |
| Quote complete, fair, margini e orari | `partitions.opening` e `partitions.current` |
| Definizione apertura | prima osservazione completa dello stesso operatore |
| Movimento dall'apertura / ultimo tratto | `opening_delta_pct` / `last_leg_delta_pct` |
| Stato OCI | `current`, `neutral`, `missing` |

Le partizioni sono copiate dalla traccia già acquisita, anche per Over/Under e
GG. Una quota da cache senza la relativa traccia resta senza partizione; nessuna
ricostruzione da margini mediani o bookmaker diversi. Una tenuta ST assente è
`missing`, mai un test fallito. Il flag `reversal=False` è `no_reversal`, non
un fallimento: `reversal=True` è `reversal_detected`.

I risultati normalizzati `home_ft/away_ft` sono esclusivamente regolamentari.
`score_periods` separa 90 minuti, supplementari e rigori. Se manca il risultato
regolamentare non si usano i gol aggregati o i rigori per liquidare il contratto.

## Confronti isolati

1. **GG FT squadra debole:** almeno una lambda FT finale V7 inferiore a 1.
   Le lambda statistiche native sono conservate separatamente. Un timestamp
   statistico assente o futuro rende l'osservazione non disponibile.
2. **2 FT grezzo/mercato:** P Poisson grezzo meno P V7 almeno 0,10 (10 punti
   percentuali), con entrambe le probabilità originali disponibili.

Sono osservatori di coorte: non rieseguono il selettore, non bloccano mercati,
non cambiano ruoli e non promuovono nulla. Il controllo e le precedenti varianti
shadow restano identici. Configurazione e soglie sono certificate nel prematch.

Il controllo dei prezzi ammette solo 1/2 FT e il corrispondente AH −0,5 FT del
catalogo verificato, adapter v2, stessa squadra, stesso bookmaker, entrambe le
quote eseguibili e timestamp di osservazione/provider entro 60 secondi.
P V7 deve coincidere esattamente: differenza zero. Interi, quarti, rimborsi,
parziali, periodi differenti, identificativi ambigui e adapter precedenti sono
esclusi. Le opportunità riportano il vero mercato AH, operatore, orari ed
esclusioni. Nessun prezzo viene sostituito nelle selezioni ufficiali.

GG FT entra nella griglia **di audit** 0,50–0,80 soltanto per nuove fotografie
del protocollo. `audit_weight_benchmark=True` abilita il report;
`benchmark=False` mantiene GG FT fuori dall'apprendimento produttivo esistente.
La griglia calcolata alla liquidazione è controfattuale sui vettori congelati,
non una previsione alternativa pubblicata prima della partita.

## Report e verifica successiva

Il report esistente `movement_shadow` include `research_trace`, calcolato solo
su prove nuove. Il certificato di configurazione include soglie di ricerca,
versione, peso, stato di calibrazione e policy del controllo e configurazione
shadow precedente. Le configurazioni sono separate, i kickoff conflittuali esclusi,
le fotografie deduplicate per fixture e i timestamp successivi al cutoff esclusi.
Sono pubblicate coorti corrispondenti e complementari, scelte ufficiali e
candidati non scelti, anche sfavorevoli, e celle per giornata.

Le metriche comprendono calibrazione, Brier e log-loss appaiati contro il fair
del medesimo evento, bootstrap separati per fixture e giornata, copertura,
composizione, vincite dei candidati non scelti e qualità del prezzo equivalente.
I cambi di ruolo sono zero perché gli osservatori non cambiano scelte. La griglia
GG FT ha un report dedicato. ROI unitario è soltanto controllo. Con meno di due
cluster l'intervallo è esplicitamente non disponibile. Gli intervalli sono
descrittivi: nessuna selezione retrospettiva della cella migliore o promozione.

Il report ordinario di apprendimento acquisirà le nuove giornate concluse;
prima degli esiti la raccolta è disponibile, l'efficacia resta da verificare.
La funzione offline `v7_shadow_metrics.research_report(archives, cutoff, draws)`
permette un'analisi separata sugli stessi archivi, senza provider o scritture.
Per il rapporto finale prospettico usare almeno 4.000 repliche e mantenere tutte
le celle esplorate. Non usare questo report per modificare automaticamente V7.

Validazione tecnica: suite Python completa, tutti i file Node, suite browser CI
e test dedicati all'invarianza numerica, immutabilità, identità dei pagamenti,
separazione 90'/AET/PEN, dati mancanti, griglia GG FT e report appaiato.
