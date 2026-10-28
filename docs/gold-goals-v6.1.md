# Gold goals v6.1 — implementazione e audit retroattivo

Richiesta applicata al motore lateral-matrix-v5.0 e alla politica gold-goals-v6.1. Layout, griglia CSS e file CSS invariati. Nessun ordine di scommessa eseguito.

## Regole

| Mercato | Quota | P minima | EV prudente minimo | Altro |
|---|---:|---:|---:|---|
| Over 2,5 FT | 1,85–2,60 | 48% | +2% | Sotto 1,85: soglia rendimento |
| Over 3,5 FT | 2,10–4,50 | 35% | +1% | Prima alternativa al 2,5 FT sotto quota |
| Over 1,5 PT | 2,10–3,50 | 40% | +2% | Lambda PT ≥1,35, frequenza ≥75% |
| Over 2,5 PT | 4,50–7,50 | 18% | 0% | Lambda PT ≥1,70 e OCI ≥15 |
| GG PT | 3,50–5,50 | 26% | −1% | EV centrale ≥6%, simmetria individuale |

Le equivalenze asiatiche ammettono soltanto la stessa mezza linea. I rimborsi delle linee intere o quarti non sono equiparati a un evento binario. Le soglie sono inclusive. Il mantenimento della proposta non può derogare ai limiti della v6.1.

Blocchi: OCI <−20; salita quota dello stesso operatore >2%; divergenza statistica >15 punti senza entrambi calo quota e OCI positivo; aggio negativo, mancante o >8%. Tra 6,5% e 8% inclusi, EV prudente minimo aumentato di 1,5 punti. P book è depurata dal margine dello stesso operatore quando disponibile. OCI mancante non viene trasformato in zero; impedisce Over 2,5 PT e Combo, e non può confermare una divergenza oltre 15 punti.

La media combinata anti-picco è la somma delle medie dei gol fatti delle due squadre, nel periodo del mercato; la stessa somma senza ultima gara deve raggiungerne l’80%. Per il PT servono quattro osservazioni effettive per squadra. GG PT richiede, per ciascuna, lambda ≥0,55, media fatti ≥0,50 e subiti ≥0,50. La frequenza PT riguarda almeno un gol segnato dalla squadra.

## Cache e identità squadre

La quota conserva operatore, apertura, margine e timestamp originali. La finestra è di 12 ore, inclusa, prima del calcio d’inizio; usa la più vecchia fra data provider e data originale di osservazione disponibili. Rileggere o riscaricare la stessa osservazione non rinnova la scadenza. EV e sensibilità sono ricalcolati con i pesi statistici correnti. Fuori whitelist o oltre il limite non c’è recupero. Quote senza partition completa possono essere visibili ma non superano il requisito di aggio.

Willem II è una identità senior esplicita ammessa nella whitelist. I suffissi di vere riserve (Ajax II, Bayern III, Barcelona B) e le marcature giovanili restano esclusi: il solo campionato autorizzato non basta a identificarle come prima squadra.

## Combo e interfaccia

P = (1−exp(−lambda PT)) × (1−exp(−max(0,30; lambda FT−lambda PT))). Attiva con P ≥56%, lambda PT ≥1,15, lambda ST ≥1,40, non più di una gara senza segnare nell’intera finestra FT per squadra e OCI dell’Over 2,5 FT ≥−10. La somma di due tempi distinti usa l’ipotesi di indipendenza degli incrementi.

Una riga standard nel popup mostra stima, quota equa e stato. La raccomandazione testuale usa il paragrafo già esistente. Nessun nuovo contenitore o CSS. La Combo resta una indicazione statistica senza quota offerta: non sostituisce un mercato quotato nel box e non genera un ROI inventato.

## Copertura del ricalcolo

| Data 2026 | Partite archiviate | Replay V5 completo | Non ricostruibili integralmente |
|---|---:|---:|---:|
| 2026-10-23 | 66 | 0 | 66 |
| 2026-10-24 | 258 | 0 | 258 |
| 2026-10-25 | 287 | 0 | 287 |
| 2026-10-27 | 329 | 329 | 0 |

Le 611 partite del 23–25 ottobre provengono da context-goals-v3.4. Mancano gli input V5 originali e gli scenari di sensibilità V5. Sono incluse nel controllo di copertura, ma i relativi risultati v6/v6.1 sono N/D, non zero. Non sono stati inventati precedenti o prezzi né raccolti dati post-partita per ricostruire previsioni. Il confronto causale completo sulle quattro giornate resta quindi non disponibile.

## Confronto verificabile: 27 ottobre, 329 partite

| Politica | Proposte | Vinte | Perse | Strike rate | Quota media | Saldo unità | ROI netto teorico |
|---|---:|---:|---:|---:|---:|---:|---:|
| v6 effettivamente registrata | 13 | 5 | 8 | 38.46% | 2.138 | -1.99 | -15.31% |
| v6 ricalcolata | 13 | 5 | 8 | 38.46% | 2.145 | -1.89 | -14.54% |
| v6.1 simulata | 14 | 8 | 6 | 57.14% | 2.614 | +6.64 | +47.43% |

Recupero di almeno una quota gol in 63 partite, di cui 7 della Serie B italiana. 5 proposte finali usano la cache; recupero quota e idoneità non coincidono.

Combo attiva in 3 partite: 3 vinte e 0 perse; ROI non calcolabile senza quota effettiva.

Verificati 329 hash degli input e 4346 probabilità rispetto ai tassi salvati: zero discrepanze. Una sola ultima osservazione prematch per partita; esiti regolamentari ufficiali acquisiti nell’export precedente.

Il ricalcolo v6 può scegliere un diverso operatore/alias equivalente rispetto al box conservato; per questo sono riportati entrambi i riferimenti. ROI a una unità fissa per proposta, al netto delle puntate perse, senza commissioni/tasse/slippage non osservati. Queste giornate sono state usate per definire la nuova politica: il risultato è diagnostico sul campione, non validazione indipendente o garanzia di rendimento.

## Riproduzione e controlli

```bash
python -m unittest discover -s tests -v
node tests/test_lateral_matrix.js
python auditor/replay_gold_v61.py --export PRIVATE_EXPORT_23_24.json --export PRIVATE_EXPORT_25_26_27.json --manifest PRIVATE_WORKBOOK_DATA.json --output PRIVATE_REPORT.json
```

Baseline v6 e whitelist v2 conservate in auditor/baselines dal commit 4c16a09f790dd0a8f93620e4ab8f8d2eadd4e328. Gli export privati e i dettagli delle previsioni non vengono pubblicati nel repository. I controlli browser della CI verificano desktop 1280 px e telefoni 390/360 px, apertura popup, navigazione, focus, scorrimento e nuovi stati.
