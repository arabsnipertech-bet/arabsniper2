# Gold: perimetro e priorità dei quattro mercati gol

La pipeline attiva è `3appdays.py` → `statistics_integration(matrix_mode=True)` → `matrix_pipeline`/`matrix_quotes` → `opportunity_policy`. Le regole non sono un calcolo separato dalla scansione: ogni Fast le applica alle partite future. La vista Gold e l’anteprima pubblica consumano gli stessi dati certificati.

## Competizioni

`src/gold_competitions.py`, policy `gold-senior-whitelist-v1`, ammette le massime divisioni senior dei paesi richiesti e le coppe esplicitamente autorizzate. **Romania inclusa**, Liga I/Superliga e coppe ufficiali. Le nazioni sono abbinate al nome esatto normalizzato della competizione: un nome generico come “Premier League” da solo non basta. Giovanili, riserve, divisioni inferiori e competizioni sconosciute sono escluse. Le coppe nazionali ammesse sono quelle dei paesi europei elencati, Cina e Giappone; le coppe internazionali sono le tre UEFA, AFC Champions League, Libertadores, Sudamericana e Concacaf Champions Cup.

Il controllo precede le chiamate dettagliate per fixture. Dentro la whitelist, tutte le partite restano osservabili: dati insufficienti comportano stime di riferimento dichiarate, non cancellazioni. Anche i dati salvati, gli export delle selezioni, il palinsesto live e l’anteprima pubblica vengono riallineati. Gli archivi storici rimangono integri, con previsioni iniziate immutabili. La copertura distingue il palinsesto restituito dal provider da quello ripristinato da cache.

## Proposta e aggio

`gold-goals-v2` consente nel box soltanto Over 1,5 PT, Goal PT, Over 2,5 FT e Over 3,5 FT, con probabilità centrale almeno 35%, quota reale recente 1,40–3,50 inclusi e storico supportato. Gli altri esiti restano nella matrice ma non diventano la proposta. Over 3,5 FT non può essere più probabile di Over 2,5 FT; Goal PT non può superare Over 1,5 PT.

L’aggio è quello della partizione completa dello **stesso operatore che offre la quota scelta**. Sotto 5% FT / 7% PT sono richiesti EV almeno 2% e scenario prudente positivo; fascia media almeno 4% / 1%; sopra 9%, EV almeno max(8%, aggio−1) e scenario prudente almeno 2%. Un’espansione di almeno un punto aggiunge rispettivamente 2 e 1 punti alle soglie. Aggio sconosciuto richiede 10% / 3% e non consente conferme di flusso; aggio negativo o sopra 30% esclude la proposta.

L’OCI deve avere modulo almeno 40, almeno due operatori concordi e maggioranza stretta, direzione depurata dal margine coerente e movimento dello stesso operatore persistente (almeno 0,5), senza inversioni. Con valore statistico e OCI positivo: **Convergenza di Trend**. Con OCI negativo: **Contrarian sano** soltanto se il margine è al massimo 9% e non in espansione di almeno un punto. Negli altri casi: **flusso non confermato**. Margine e variazioni non dimostrano volumi, liquidità o smart money.

## Digiuno, campo e primo tempo

Il confronto usa gli ultimi tre risultati contro i precedenti disponibili, fino a otto gare validate. Per il campo effettivo, confronta le ultime due gare in casa della squadra di casa (in trasferta per l’ospite) con almeno due precedenti nello stesso contesto. Non è una media stagionale completa: numerosità, media grezza e riferimento robusto sono salvati. Un picco storico è limitato nella media di riferimento; nessun gol futuro è “dovuto”.

Scarto attivo: media recente al massimo al 70% del riferimento e differenza almeno 0,5 gol FT / 0,25 PT, oppure almeno due gare consecutive senza segnare con riferimento sufficiente. Senza riscontro nel campo effettivo o con campione insufficiente, il digiuno generale non attiva la priorità.

Il contesto 1X2 richiede una terna completa dello stesso operatore, flusso confermato come sopra e aggio 0–9% non in espansione. Favorita netta: quota al massimo 1,75 e probabilità normalizzata almeno 55%. Quote 1 e 2 distanti al massimo **0,40**: partita bilanciata, senza imporre una favorita.

| Condizione con scarto nel campo effettivo e flusso valido | Priorità tra candidati già ammissibili |
| --- | --- |
| Favorita in digiuno, casa o trasferta | Over 1,5 PT, poi Goal PT, poi FT |
| Sfavorita in digiuno, casa o trasferta | Goal PT, poi Over 1,5 PT, poi FT |
| Quote bilanciate | I due PT competono per valore/giocabilità, poi FT |
| Dati o flusso insufficienti | Confronto ordinario dei quattro mercati |

Se entrambe sono sotto media con favorita netta, prevale la regola della favorita. La priorità modifica soltanto l’ordine dei candidati: **bonus alla probabilità = 0**. Non aggira alcun filtro; se i PT non passano, valuta i FT, altrimenti lascia il box senza proposta. Il flusso 1X2 non conferma automaticamente un mercato gol: l’etichetta della proposta dipende dal suo flusso esatto.

A parità di priorità si usa il punteggio ordinale `p × max(0, EV_prudente − soglia_prudente) / (quota − 1)`, poi vantaggio prudente, EV centrale e probabilità. Non è una probabilità calibrata né un suggerimento di puntata. Le soglie e la priorità temporale sono regole sperimentali da verificare sui risultati futuri.

## Pubblicazione

Le modifiche compatibili di policy e perimetro rigenerano dati e classificazioni dal checkpoint privato con zero chiamate al provider; i normali Fast continuano a raccogliere dati e rivalutare le partite future. Il browser attende dati con la policy corrente e non ripropone vecchie selezioni. Il quadro 1X2 mantiene le tre colonne anche a 360 px, apertura osservata, quota attuale e frecce; il box espone quota, probabilità, OCI, aggio e motivazione della priorità PT.
