> **Esito aggiornato:** Explorer è attivo e il collaudo online finale è passato. Vedere il [report di rilascio del 17 novembre](gold-explorer-release-2026-11-17.md) per risultati, tempi, limitazioni e rollback. Le indicazioni di stato pendente riportate sotto descrivono la fase precedente al rilascio.

# Gold Explorer V1 — architettura, collaudo e rilascio

Aggiornamento: 17 novembre 2026. URL canonico previsto: https://arabsniperbet.com/gold-explorer.

Bruno ha autorizzato merge e deploy. [PR #220](https://github.com/arabsnipertech-bet/arabsniper2/pull/220) e [PR #221](https://github.com/arabsnipertech-bet/arabsniper2/pull/221) integrate; [PR #222](https://github.com/arabsnipertech-bet/arabsniper2/pull/222) con test verdi, in attesa del completamento del backend. Nessuna modifica al cron Night.

## Architettura e audit

Lettura dei cinque feed Gold già prodotti da Night/Fast e degli archivi Auditor, proiezione in R2 `explorer/`, API privata nel Worker `stripe-checker`, interfaccia sulla stessa origine della Gold. Il modello V7 e i suoi collector non sono modificati.

Fonti individuate nel codice:

- `src/3appdays_runner.py` e workflow Night/Fast esistenti: produzione delle analisi, quota originale, successivi aggiornamenti. Nessun secondo avvio di questi processi.
- `matches[].multimarket.matrix.v7.markets`: universo analizzato e probabilità V7; `matrix.rows`: prove temporali/eseguibilità delle quote; `triad_commitments`/`triad`: identificazione ufficiali.
- `src/v7_archive.py`: validità degli snapshot tramite versione, fixture, kickoff, data pubblicazione e SHA256.
- `auditordata/prematch_DATE.json`, inclusi contenitori a parti verificati da `audit_storage`: pubblicazioni prematch originali in `analysis_updates`.
- `auditarchive/prematch_DATE_v7.json`: risultati già acquisiti, stato del match e pesi di liquidazione per mercato. Explorer non chiama il live né il provider risultati.
- `src/gold_storage.py`: feed privati R2 e checkpoint `state/current.json`. Explorer parte solo dopo il commit Gold; la sua eccezione viene contenuta.
- `workers/stripe-access/auth.mjs`, `worker.mjs`, `renewal.mjs`: cookie OTP, sessione di 24 ore e diritto Gold/prova esistente. `ACCESS_DB` e `GOLD_DATA` sono riutilizzati. Nessun prezzo, piano o durata Stripe cambia.

L'audit reale ha verificato R2, D1 e il Worker di produzione. Il Worker online contiene correzioni non presenti nel bundle del repository: il rilascio applica una patch additiva al codice effettivamente scaricato e verifica che, rimuovendo solo le aggiunte Explorer, si ottenga il codice originale byte per byte. Le suite autenticazione, rinnovi ed Explorer sono state eseguite anche sul Worker reale modificato.

## Matrice di compatibilità

| Funzione | Dato effettivamente utilizzato | Comportamento/limite |
|---|---|---|
| Tutti i mercati | `v7.markets` dello snapshot verificato | Nessun mercato aggiunto artificialmente |
| 1/X/2 FT | `result` nel catalogo V7 | Disponibile se presente nel feed |
| Over/Under PT, ST, FT | `total` e relativi periodi | Solo linee già calcolate e archiviate |
| GG/No Goal PT, ST, FT | `btts` | Probabilità originale, nessuna moltiplicazione |
| DNB, Asian totals, handicap | Catalogo `lateral_matrix.base_specs` | Visualizzati se presenti; liquidazioni parziali supportate |
| Combo | `market_scope.COMBO_SPECS` e altri mercati già nello snapshot | Probabilità congiunta V7 conservata, mai ricostruita |
| Doppia chance | Non inclusa nel catalogo base corrente | Nessuna promessa di disponibilità; leggibile solo se già archiviata |
| Apertura/prime quote/fonte | `v7.markets[].quote` e `matrix.rows` | Valori mancanti restano assenti; mai ricavati dal prezzo attuale |
| EV, EV_low, OCI, affidabilità | Indicatori V7 originali | Supporto statistico distinto da validazione e valore |
| Risultati | Archivio V7 `fixtures[].outcomes` | Solo stessa fixture e stesso kickoff; assenze non conteggiate come perse |
| Ricerche salvate | D1, account autenticato | Filtri riapplicati ai dati correnti |
| Preferiti | Snapshot canonico recuperato dal server | Quota al salvataggio immutabile, Fast successivo separato |
| Night/finale | Nuovi snapshot Explorer prospettici | Il passato non diventa retroattivamente una classifica Explorer |
| Vecchio storico | Pubblicazioni originali V7 verificabili | Sezione separata; nessuna attribuzione dei risultati a Explorer |

La copertura effettiva per partita dipende dal feed: un mercato previsto nel catalogo non implica che esistano quota utilizzabile, campione adeguato o risultato completo. La proiezione non risolve lacune della raccolta V7 aggiungendo chiamate.

## Funzioni implementate

Pagina `web/gold-explorer.html` e pannello laterale lazy-load in Gold, a tutta altezza su mobile. Dashboard; cinque classifiche; ricerca avanzata anche su mercati non raccomandati; filtri combinabili (campionato, squadra, mercato, quota, probabilità, EV/EV_low, OCI, supporto, categoria, ufficiali, tempo); Europe/Rome e cinque date di calendario. Il pulsante mostra altri dieci risultati. Classifiche diversificate per fixture, con accesso alle altre analisi dello stesso incontro.

Ricerche personali: creazione, nome, richiamo sui dati aggiornati, modifica e cancellazione. Preferiti persistenti e privati: massimo 500 per account, pagine da 20; 50 ricerche per account. Dettaglio: quote, fonte/date, Night/Fast, impronta forecast, motivi di attenzione, esito e punteggi PT/FT disponibili.

Storico 7/30/90 giorni o completo, universi distinti (tutti, validate, Top 10 certificate, ufficiali, alternative, preferiti). Statistiche: N, verificati, copertura, vinte/perse, hit rate, ROI teorico, EV prematch, Brier e calibrazione; aggregazione per campionato/mercato e confronto Night/finale. I filtri degli indicatori e del mercato si applicano anche ai campioni storici. Le tabelle indicano campioni separati, non dimostrano una superiorità causale.

## Criteri delle classifiche

Nessun modello o peso stimato nuovo. Si riutilizza l'ordinamento lessicografico V7: probabilità/EV_low/EV per Più Probabili; presenza della nicchia V7/EV/probabilità per le altre classifiche. Parità risolte deterministicamente per mercato e ID. Consigliate e Colpacci applicano `TRIAD_MARKETS` e `TRIAD_LIMITS` esistenti, inclusa la condizione Away del Colpaccio.

L'ammissione Explorer richiede quota utilizzabile, `supported=true`, tier V7, EV positivo ed EV_low non negativo. È un filtro prudenziale di valore e supporto già usato dalla logica V7; può escludere una selezione ufficiale di studio o una nicchia meno robusta, senza modificarla. Non è una nuova ottimizzazione statistica e non si rivendica un rendimento dimostrato. Le etichette Ufficiale, Alternativa, In osservazione e Non qualificata restano separate.

## Doppia certificazione e integrità

Il limite è **kickoff della singola partita meno 300 secondi**, confrontato in UTC. La tolleranza di cinque minuti è un margine operativo esplicito contro aggiornamenti a ridosso dell'avvio, non una soglia ottimizzata su esiti. Il congelamento viene materializzato dalla prima pubblicazione esistente successiva alla soglia. Nessun job aggiuntivo si sveglia al minuto del kickoff. Anche dopo un'interruzione di più giorni il catalogo conserva i riferimenti ancora da congelare.

Il primo Night verificato resta originale; l'ultimo Fast pubblicato entro il limite e con quota utilizzabile può diventare riferimento finale. Senza Fast valido si conserva Night con indicazione esplicita. Un forecast precedente all'inizio del job non viene falsamente certificato come nuova scansione Fast. Stato avviato, rinviato o cancellato e risultati già presenti escludono la partita dalla proiezione prematch. Un kickoff diverso crea un episodio distinto; i risultati non sono trasferiti tra episodi.

Oggetti e pagine certificati hanno chiave SHA256 del contenuto, sono riutilizzabili idempotentemente e verificati dall'API. Gli indici giornalieri possono avanzare per appendere nuove versioni/risultati; gli oggetti certificati e i dati Gold originali non vengono modificati. I Top 10 conservano gli ID nell'ordine registrato per ogni versione temporale. Il campione Top 10 aggregato usa l'appartenenza della partita alla versione del suo riferimento; le singole versioni restano consultabili separatamente.

Le quote recenti rispettano le finestre V7 (6 ore, 1 ora nelle ultime 3 ore); le quote stabili rispettano il limite originale di 120 ore. La rilettura non rinnova la scadenza. Timer locali rimuovono partite iniziate e quote scadute senza polling.

Per le ufficiali lo storico usa la quota del commitment. Se il commitment non conserva la probabilità originale e il timestamp differisce dallo snapshot, probabilità/EV/Brier originali sono indisponibili: non si sostituiscono con valori successivi. ROI a puntata unitaria solo su quote documentate; Brier/calibrazione solo su esiti binari con probabilità. Rimborso e liquidazioni parziali non diventano vittorie binarie.

## Sicurezza, chiamate e prestazioni

Ogni `/api/explorer/*` verifica feature flag, origine, sessione OTP e diritto Gold tramite la funzione di accesso esistente. POST richiede stessa origine, JSON entro 8 KiB e limite 30 scritture/minuto. Le query personali sono parametrizzate e account-scoped. Il client invia soltanto ID e hash per il preferito: quota/probabilità/email fornite dal client non possono sostituire il dato canonico.

R2: letture/scritture del projector confinate a `explorer/`; nessuna scrittura nei feed/calibrazioni/archivi originali. API Worker legge soltanto gli oggetti derivati e D1. Il codice Explorer non importa client calcistici né esegue fetch a provider. Il controllo dell'abbonamento riutilizza l'infrastruttura Stripe/cache già esistente; non è una chiamata calcistica.

Pagine da 250 righe e caricamento di quattro pagine per gruppo; rendering progressivo. Filtri/ranking locali senza richieste di rete. Preferiti paginati. Checksum degli archivi riutilizzati dal manifest Gold già calcolato; import storico ordinario limitato a una giornata modificata per pubblicazione. L'import completo opzionale legge soltanto gli archivi restaurati. Gli errori Explorer non fanno fallire la pubblicazione Gold già conclusa.

Limite: caricando lo storico completo il browser conserva in memoria le pagine richieste; il costo cresce con l'archivio. La verifica reale prima del rilascio ha elaborato 419 snapshot V7 e 30.149 righe (413 partite, 73 mercati), con 2.741 righe validate nel campione. Proiezione circa 64 secondi e 42,7 MB di output; 639 letture R2 per circa 299,6 MB. Sono misure del campione verificato, non del caricamento browser o dello storico completo. La prima importazione completa viene misurata separatamente dal job di rilascio.

## Collaudo ed evidenze

- CI PR #221 e main: **884 test Python**, suite Node e browser Gold/Publisher/Explorer superati. [Run main 44054034194](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44054034194).
- CI PR #222 superata: [run 44055883996](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44055883996).
- Test dedicati Python: validità/impronte, cutoff, stati, idempotenza, congelamento dopo interruzione, fallback, Fast inutilizzabile, originali immutati, paging, scope storage e storico senza classifiche retroattive. Socket outbound negati durante i test dedicati.
- Cinque test del lease: contesa, gara atomica, recupero dopo crash, scadenza e rilascio condizionale, contenimento degli errori. Il lease protegge soltanto Explorer e non blocca Gold.
- Test Node: sessione D1/cookie, diritto attivo/prova, revoca/scadenza, isolamento CRUD, CSRF, limiti, preferiti canonici, checksum, filtri, ranking e statistiche. Questi test usano diritti controllati: non equivalgono a login con abbonamenti reali di terzi.
- Browser sintetico: 1.250 righe, quattro viewport, CRUD, storico, pannello, errori/rete, zero errori JavaScript e zero richieste calcistiche; filtri senza richieste. Screenshot in `docs/qa/` esplicitamente sintetici, non schermate autentiche di produzione.
- Preflight reale: [run 44049819676](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44049819676). Lettura archivi autenticata; nessuna scrittura remota né chiamata calcistica. Patch sul Worker reale e suite di accesso superate.
- Collaudo online: il rilascio esegue CRUD personale e browser con sessione temporanea del proprietario; sessione e soli record QA vengono eliminati. Non invia OTP, non modifica abbonamenti, non usa sessioni di terzi. Il controllo aggiuntivo verifica cinque classifiche e storico reale di sette giorni.

**Stato al 17 novembre 2026, durante l'importazione:** il job di deploy è ancora in esecuzione; il collaudo autenticato online non è ancora dichiarato superato. Non dichiarare il servizio pronto sulla sola base della presenza degli asset Pages.

## Procedura effettiva di rilascio

1. CI verde prima della distribuzione. Il job isolato `explorer-authorized-release` nell'esistente workflow di test si attiva dal messaggio di merge `[release-explorer-v1]`; non avvia Night/Fast/Auditor.
2. Lease R2 esclusivo su `explorer/writer-lease.json`, precondizioni atomiche e scadenza di tre ore. Se occupato, l'hook Gold salta soltanto Explorer senza attendere. Il precedente rilascio in coda è stato annullato verificando run, SHA e stato; nessun job calcistico annullato.
3. Backup privato del Worker e dei binding sotto `ops-backups/gold-explorer/`; verifica SHA del codice online, patch additiva e suite sul codice reale. Non distribuire ciecamente il bundle del repository.
4. Due tabelle personali e un indice D1 con istruzioni idempotenti. Lettura dei feed e degli archivi già esistenti, importazione sotto `explorer/`; nessun originale modificato. Rimozione dei soli chunk temporanei locali dopo ciascuna giornata.
5. Aggiornamento del solo contenuto Worker, conservazione dei binding, aggiunta del flag `GOLD_EXPLORER_ENABLED`. Il client usa `/api/auth/explorer/*`, coperto dalla route Gold esistente: nessuna nuova route necessaria.
6. Rifiuto anonimi HTTP 401, controllo autenticato API e browser, report e screenshot autentici negli artifact Actions. L'ulteriore verifica `[explorer-verify]` non ripete il deploy.
7. I workflow ordinari proiettano Explorer dopo il commit Gold. La variabile GitHub esplicita `GOLD_EXPLORER_ENABLED=false` disabilita questo passaggio. Nessuna nuova raccolta né credenziale calcistica nel job di rilascio.

Il deploy una tantum verifica il codice originale e l'assenza del flag: dopo un deploy riuscito non va rilanciato indiscriminatamente per correggere un eventuale solo errore del browser. Usare il job di verifica separato.

## Rollback

Disabilitare il flag Worker e impostare la variabile GitHub Explorer a `false`. Ripristinare il contenuto Worker dal backup privato verificando prima che non siano intervenute modifiche concorrenti; ripristinare gli asset Pages precedenti se necessario. Conservare tabelle personali e oggetti Explorer. Non modificare archivi Gold, pagamenti, probabilità o risultati originali. Il deploy ripristina automaticamente il codice precedente se fallisce durante caricamento/attivazione, salvo modifiche concorrenti.

## Limiti e attività residue

- Copertura di quote e risultati determinata dagli archivi esistenti. Dati mancanti indicati, non ricostruiti con nuove chiamate.
- Certificazione Explorer solo prospettica: lo storico V7 preesistente resta un universo separato.
- Verifica online autenticata e misure del primo import completo pendenti fino all'esito del job; nessuna dichiarazione anticipata di completezza.
- Nessuna campagna o pubblicazione promozionale avviata da questa consegna. Le immagini di test non costituiscono screenshot autentici del servizio.
