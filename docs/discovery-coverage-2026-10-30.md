# Discovery quote: verifica del 30 e 31 ottobre 2026

Il confronto sul feed reale recupera **10 mercati con quota valida**, da **1.513 a 1.523 su 3.456**, pari a **+0,29 punti percentuali**. Il miglioramento proviene dalle interrogazioni supplementari. Sul medesimo payload il nuovo parser Discovery e quello pubblicato riconoscono lo stesso numero di prezzi validi: nel campione attuale le etichette sono già compatibili.

## Copertura misurata

| Giorno (Europe/Rome) | Partite da iniziare | Mercati esaminati | Prima | Dopo acquisizione e refresh | Variazione |
|---|---:|---:|---:|---:|---:|
| 2026-10-30 | 111 | 666 | 406 (60,96%) | 408 (61,26%) | +0,30 pp |
| 2026-10-31 | 465 | 2790 | 1107 (39,68%) | 1115 (39,96%) | +0,29 pp |
| **Totale** | **576** | **3456** | **1513 (43,78%)** | **1523 (44,07%)** | **+0,29 pp** |

Le quote effettive dei bookmaker passano da **1.511 a 1.521**; restano **2 Combo sintetiche**, entrambe per domani, conteggiate separatamente come stime. Non sono offerte eseguibili. Non ci sono aumenti dovuti al solo refresh nelle ultime quattro ore.

| Mercato | Prima | Dopo | Recuperati |
|---|---:|---:|---:|
| Over 1,5 PT | 256 | 256 | +0 |
| Over 2,5 PT | 256 | 256 | +0 |
| Over 2,5 FT | 258 | 258 | +0 |
| Over 3,5 FT | 248 | 248 | +0 |
| GG PT | 248 | 252 | +4 |
| Over 0,5 PT + Over 0,5 ST | 247 | 253 | +6 |

Le combinazioni mercato–operatore con prezzo completo, fresco e aggio valido passano da **6.114 a 6.132**. I mercati con almeno due bookmaker validi passano da **1.312 a 1.316**: questa disponibilità non dimostra ancora un movimento OCI concorde.

## Calendario e assenze

Il calendario ufficiale contiene 125 partite in whitelist oggi e 473 domani. Sono state esaminate **tutte le 576 partite NS/TBD ancora da iniziare** (111 + 465), senza errori API né esclusioni per budget. I 22 eventi con altri stati non entrano nel confronto prepartita. Tutte le 576 partite erano già presenti nei feed pubblicati: nessun buco del calendario rilevato.

Dopo il recupero restano **1.151 mercati senza una quota riconosciuta**, **748 scaduti senza alternativa valida** e **34 con altri impedimenti alla validità**, su 3.456 mercati. Le partite restano in osservazione: “In attesa di quote / flusso · Solo studio” è distinto da un segnale Radar confermato. Le priorità delle proposte Gold e Platinum rimangono intatte.

Il catalogo espone **33 bookmaker**. Nel campione ne risultano validi **9 oggi e 10 domani**, già prima della modifica. Il parser legge tutti gli operatori disponibili; l'ampliamento non rende valide coppie incomplete o quote troppo vecchie. Non è possibile azzerare le quote assenti nel feed solo cambiando i nomi riconosciuti o ripetendo le chiamate.

## Modifiche e controlli

- Discovery semantico su nomi, periodo e linea, con normalizzazione di punteggiatura, decimali e sinonimi. Gli ID del feed restano nella provenienza; gli adattatori interni conservano gli identificatori storici.
- Le ricerche supplementari usano il catalogo API e i mercati osservati. Nessun bookmaker di riferimento è obbligatorio. Il consenso conserva almeno due operatori distinti e la maggioranza concorde già prevista.
- Fallback asiatici esclusivamente equivalenti alle linee `.5`; nessuna conversione arbitraria di linee intere o quarti.
- Combo sintetica dello stesso operatore e timestamp, con entrambe le coppie Over/Under e aggio di ciascuna origine tra 0% e 9%. Stima esplicita, storico separato per fonte, nessuna falsa partizione completa o conferma OCI.
- Refresh prepartita entro quattro ore solo se esiste una prima quota salvata: massimo **40 chiamate HTTP per ciclo**, inclusi retry/status/pagine, e **30 minuti** di cooldown persistente per fixture.
- Se falliscono le statistiche, la partita ufficiale viene mantenuta e le quote vengono comunque interrogate. Nessuno storico statistico inventato; nessuna ricostruzione a posteriori di previsioni iniziate.
- Invariati i file delle politiche Gold v6.1, Platinum, movimento OCI e ambito dei mercati. Il pavimento EV Platinum resta **−15%** e gli altri filtri rimangono attivi.

## Metodo e consumo API

Misurazione valida: **30 ottobre 2026, 13:34–13:38 Europe/Rome**, in sola lettura. Confronto con il commit pubblicato `4bcff1b1c2cba61f55f0f732e9c08678978aae76`; audit sul commit `5a0a0b982baaf94c7cb5ec4258a3188faef654da`.

Per ciascuna partita le quattro fasi vengono valutate allo **stesso identico istante**: parser precedente sul payload base, Discovery sul medesimo payload, acquisizione supplementare, refresh prepartita. Questo evita di attribuire al recupero l'effetto del solo trascorrere del tempo. La prima verifica esplorativa è sostituita da questa misurazione.

La validità di copertura richiede prezzo fresco, coppia completa dello stesso operatore e aggio 0–9%. La Combo sintetica valida i margini delle due origini separatamente. Escluse quote da cache; equivalenti asiatici contati una volta sola. La copertura di prezzo non è l'idoneità Gold: i suoi limiti, incluso l'aggio più restrittivo, sono invariati.

- Audit valido: **835 chiamate**, tetto complessivo 1.600, con riserva API verificata.
- Acquisizione supplementare: **240 chiamate su 31 fixture**; il limite dell'audit non impedisce la lettura base di tutte le 576 partite.
- Refresh prepartita: **11 fixture, 13 chiamate HTTP, nessun recupero aggiuntivo**; limite 40 rispettato.
- Verifica preliminare: 832 chiamate. Totale delle due verifiche di questa modifica: **1.667 chiamate**.

Il fornitore indica aggiornamenti delle quote prepartita indicativamente ogni tre ore. Un Fast orario può interrogare regolarmente il feed senza ricevere un nuovo prezzo a ogni ciclo: [documentazione API-Football sugli endpoint quote](https://www.api-football.com/news/post/how-to-get-started-with-api-football-the-complete-beginners-guide).

## Verifica e tracciabilità

**400 test Python verdi**, rispetto ai 382 presenti all'inizio del lavoro (18 nuovi test); controlli JavaScript e browser desktop/390/360 superati. Due casi precedenti che imponevano un ID fisso sono adeguati al contratto semantico, mantenendo il rifiuto dei mercati di significato diverso. Le verifiche includono nomi simili non equivalenti, periodo e linea, cambio di fonte, operatori duplicati, margini Combo, limiti API, whitelist e previsioni immutabili.

Dopo la misurazione è stata ulteriormente rifiutata la presenza di frammenti di periodo ambigui, come “Total Goals Half”. Il confronto di classificazione sulle **134 etichette effettivamente osservate** (20 riconosciute, 114 fuori ambito) è identico alla revisione misurata; nessun dato di copertura è alterato da questa protezione.

- [Modifica completa: PR #93](https://github.com/arabsnipertech-bet/arabsniper2/pull/93)
- [Audit live con orario uniforme](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/41056275791)
- [CI sul codice dell'audit](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/41056284618)
- [Aggregati JSON e impronte dei payload](discovery-coverage-2026-10-30.json)

L'audit non pubblica nomi delle partite, scelte private o payload di quote. La pubblicazione della modifica è tracciata separatamente nella PR.
