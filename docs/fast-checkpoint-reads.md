# Fast — letture del checkpoint private, deduplicate e interrompibili

20 novembre 2026. Correzione circoscritta al ripristino iniziale del Fast; nessuna modifica alle analisi V7, ai workflow o all'infrastruttura R2.

## Evidenza dell'incidente

- Fast ordinario `44821836591`, avviato dal calendario alle 22:36:33 italiane: il job termina alle 22:46:34 con `ReadTimeoutError` nello step `Restore authoritative private checkpoint`. Lo step di raccolta e analisi non è stato eseguito. I log iniziali non identificano quale oggetto fosse in lettura.
- Il Fast automatico precedente `44809751660` era riuscito, sul codice successivo alla PR #244. Il ripristino di 327 puntatori era durato **10 min 37 s**, mentre `Run Fast Scan` era durato **4 min 19 s**. La notifica di errore non dimostra un errore del modello di selezione.
- Il ripristino precedente usava il client comune con connessione 15 s, lettura 60 s e tre retry SDK; il lettore riprova inoltre una volta gli errori di streaming. `restore_subset` accodava tutte le letture e attendeva il completamento della coda anche dopo un errore.

## Modifica

Solo `python src/gold_storage.py restore-fast` crea un client con connessione **5 s**, lettura **20 s** e **due tentativi SDK complessivi**, compresa la prima richiesta. Rimane il singolo retry già esistente per letture di streaming interrotte. I timeout riguardano operazioni di connessione/lettura: non sono un limite globale al tempo necessario per scaricare l'intero archivio.

Per il Fast, il ripristino raggruppa per SHA-256 i percorsi che riferiscono lo stesso oggetto immutabile. Scarica quell'oggetto una volta e conserva tutti i percorsi locali richiesti, anche se i medesimi frammenti appartengono a directory diverse. Non mantiene una cache globale di tutti i contenuti in RAM: i dati sono riutilizzati soltanto dal worker che sta materializzando quel gruppo.

Lo scheduler mantiene al massimo **otto trasferimenti in corso**. Prima di aggiungerne altri, controlla tutti i risultati appena completati. Se uno fallisce, annulla quelli ancora in coda e attende la chiusura delle letture già attive prima di eliminare l'area temporanea. Il checkout operativo viene aggiornato soltanto dopo la verifica di tutti gli oggetti e frammenti richiesti, come prima della correzione.

I log mostrano manifest in lettura, numero di percorsi/oggetti unici, progressi e percorso relativo dell'oggetto fallito. Non stampano messaggi SDK, intestazioni o credenziali.

## Ambito e limiti

- Stessi file richiesti, stessa verifica di dimensione e SHA-256, stessi dati storici e medesima politica di conservazione. Nessun fallback a dati obsoleti e nessuna raccolta avviata con un checkpoint incompleto.
- Il profilo preesistente resta il default per Night, Auditor, Publisher e gli altri chiamanti. Il download ordinario di `restore_subset` resta invariato; il nuovo percorso viene richiesto esclusivamente da `restore_fast`.
- Nessuna chiamata API-Football aggiuntiva, scrittura remota durante il ripristino o ripubblicazione dello storico. Nessuna modifica a budget, limiti, secret, bucket, autorizzazioni o workflow.
- La deduplicazione riduce le richieste solo quando esistono riferimenti ripetuti. Non garantisce la disponibilità di R2 né un tempo totale fisso per un archivio di dimensioni variabili.
- I test locali e la CI verificano logica e integrità con storage simulato. La conferma end-to-end deve arrivare da un successivo Fast ordinario con il nuovo codice; non si rilancia una scansione manualmente per il collaudo.

## Collaudo

Otto test dedicati coprono file identici, frammenti condivisi fra directory, retry del solo oggetto interrotto, corruzione senza sostituzione dei file locali, cancellazione della coda, dimensioni contraddittorie, compatibilità del percorso precedente e isolamento dei nuovi timeout al Fast. Il test della coda usa 100 oggetti e verifica che il primo errore non faccia partire più degli otto trasferimenti già ammessi.

Si esegue la suite Python completa e i 20 file di test Node. Le funzioni comuni di pubblicazione, restore completo, checkpoint e lettura retry, insieme ai moduli decisionali V7 e ai workflow, vengono confrontate con la base e restano invariate. I risultati esatti sono riportati nella PR.

Collaudo locale finale: **974 test Python superati** in 93,035 s e **20 file di test Node superati**. Nel caso simulato di sette percorsi con contenuto identico, gli oggetti vengono letti una volta invece di sette; questo non è una misura del risparmio reale sui 327 puntatori di produzione, che richiede i contatori del prossimo ripristino.

## Rilascio senza attività superflue

Base: `948a707401208dc42dd4562f61307107d580e290`, successiva alle PR #241–244. La PR esegue normalmente tutti i controlli, senza istruzioni di skip nel suo commit HEAD.

`src/gold_storage.py` compare nei percorsi push di `Gold yesterday integration`, che ripubblicherebbe lo storico. Dopo il successo della CI della PR, il commit di squash su main usa **`[skip ci] [night-before-fast]`**. Il meccanismo GitHub applicato al solo commit di integrazione evita tale pubblicazione e il secondo giro di test sul push; non modifica né disabilita workflow e non interrompe i calendari. Nessun check della PR viene saltato o forzato.

Previsione: un solo job CI di circa 3 minuti, più i test locali. Nessun dispatch e nessuna scansione extra. Se il repository impedisce questa procedura, non si forza il merge e non si avvia la pubblicazione storica.

Rollback: revert della sola correzione dopo verifica locale, usando gli stessi marker sul commit di integrazione per evitare la pubblicazione storica. Le regole V7 e i dati già pubblicati non richiedono rollback.

Riferimenti tecnici: [configurazione botocore](https://docs.aws.amazon.com/botocore/latest/reference/config.html), [skip dei workflow per singolo commit](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/skip-workflow-runs).
