# Anteprima locale del Publisher Telegram

Eseguire `node --test prototypes/telegram-preview/worker.test.mjs` dalla radice. Il prototipo legge una copia del registro Python attraverso la sola operazione `get` sul binding simulato GOLD. Il test usa dati sintetici e non richiede credenziali.

Il formato d'ingresso è quello di `publisher/v7.1/state.json`. Il risultato conserva didascalia, marker, destinatario logico ed eventuale ID del messaggio da modificare. Le proposte richiedono ancora la verifica della selezione e della scadenza contro i dati Gold, come in `delivery.can_send`. La vista non assegna né registra consegne.

Il Publisher Python legge in streaming i board Gold, genera JPEG 1080×1350 con Pillow e può generare GIF per gli aggiornamenti live. Il prototipo descrive il media richiesto. Prima di un rilascio servono confronti di didascalie e immagini con il rendering Python, incluse animazioni e font. Il percorso di prova non richiede token Telegram, Facebook o API-Football.

Per un eventuale passaggio della consegna occorre conservare il proprietario unico, la scrittura condizionale del registro, lo stato sending prima dell'invio e la riconciliazione tramite marker dopo risposte incerte. Il rollback deve sospendere il nuovo trasporto e riconciliare le ricevute prima di restituire il controllo a Growth.
