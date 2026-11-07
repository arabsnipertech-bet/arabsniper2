# Ripristino apprendimento e Gold Matchroom — 7 novembre 2026

Base tecnica: 60bbc051bddfc6ac521f2e5b283d5e5233ce1561. Le revisioni successive verificate fino ad ad8c480 contengono aggiornamenti dati, preservati nella pubblicazione.

## Correzioni abilitate

- Recupero completo per data: archivio originale più osservazioni autentiche dei journal, risultati ufficiali, report, evidenze e ricevuta. Ogni previsione già congelata mantiene la propria impronta; si possono aggiungere osservazioni originali prima mancanti. Verifica del checkpoint corrente prima del cambio puntatore.
- Report senza evidenze o risultati: certificazione incompleta. Evidenza vuota esplicita distinta da file assente. Training con date e SHA-256 esatti, più `trained_at`.
- Over 2.5 PT incluso nella mappa e nella monotonicità 0.5/1.5/2.5 PT.
- Stella Radar solo con qualità supportata, quota utilizzabile, EV centrale positivo ed EV prudente non negativo. Scarti conservati. Contratti e gare distinte separati nei report.
- Benchmark decisionale su quattro percorsi degli stessi snapshot; prime scelte, eleggibilità e rango. Replay retrospettivo dichiarato e distinto dalle decisioni congelate; pubblicazioni social non dedotte dall'eleggibilità.
- Lettura incrementale verificata; `read_store` rifiuta il master storico oltre 64 MiB, mantenendo compatibili i feed operativi più grandi, per impedire la materializzazione del master da 27,8 GB.
- Gold Matchroom: nuova gerarchia visiva, schede adattive, controlli leggibili su mobile, focus tastiera. Percorsi di autenticazione e dati invariati.

## Esperimenti separati, non promossi automaticamente

Il tracking prematch confronta Platinum con EV centrale/prudente non negativi e Gold senza promozione artificiale quando Over 2.5 FT ha quota bassa. Il fattore alpha simmetrico è registrato in shadow; il valore pubblicato resta invariato. Nessuna riscrittura delle previsioni storiche.

Non vengono promossi i blend 70/30 o 55/45, l'espansione lambda +10%, le nuove soglie OCI o MIN_N/PRIOR 35. Le evidenze fornite non validano questi numeri. Le 3.008 righe del file OCI contengono 877 vinte, 2.004 perse e 127 pendenti: tutte le 20 fasce con vincite al 100% sono riproducibili convertendo erroneamente in booleano le stringhe `won`, `lost`, `pending`. Dopo deduplicazione, inclusa l’equivalenza delle linee Asian .5, rimangono 2.406 contratti su 383 gare; 1.619 contratti chiusi quotati hanno ROI teorico −14,25%. Il miglioramento della probabilità fair non implica automaticamente un miglioramento del rendimento delle proposte.

## Workflow

`Selection policy tests` esegue suite Python, Node e browser desktop/mobile. `Pubblica sito` viene attivato dalle modifiche Gold su main. `Repair learning chain` ripristina il 5 e 6 novembre con credenziali già configurate, prepara fuori dal mutex, pubblica sotto il mutex condiviso e ricalibra solo dopo la pubblicazione della catena completa. Date chiuse entro 14 giorni; in mancanza di esiti ufficiali verificabili non vengono inventati risultati. I pending possono restare tali se il provider non dispone ancora di esiti completi.

Nessuna modifica ai contenuti, moduli o workflow Telegram/Facebook. Nessun invio social manuale associato a questo rilascio.

## Verifica locale

616 test Python e 13 file di test Node superati prima del rilascio. L'esecuzione browser locale è impedita dal runtime (`socket() Operation not permitted`); i test browser obbligatori vengono verificati nel runner GitHub prima del merge. I risultati remoti, il completamento del recupero R2 e l'effettiva pubblicazione vanno riportati separatamente: un deploy hook accettato non prova che il sito sia già aggiornato.
