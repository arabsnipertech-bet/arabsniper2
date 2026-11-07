ARABSNIPER — ARCHIVIO PRIVATO R2 COMPLETO DEL CHECKPOINT
Tutti i file referenziati dalla fotografia corrente sono conservati byte per byte.
Gli oggetti identici compaiono una sola volta; CHECKPOINT_MANIFEST.json mantiene
ogni percorso originale e il suo hash. INDICE_FILE.csv elenca tutti i file.

Non occorrono API, password o credenziali. Con Python 3:
1. Estrarre LEGGI_ARCHIVIO.py dallo ZIP, lasciando lo ZIP intero.
2. Elencare i documenti:
   python LEGGI_ARCHIVIO.py ArabSniper_Archivio_R2.zip --list
3. Estrarre un documento JSON completo:
   python LEGGI_ARCHIVIO.py ArabSniper_Archivio_R2.zip --document PERCORSO_DALL_ELENCO --output documento.json
4. Oppure ripristinare tutti i file nel formato originale del software:
   python LEGGI_ARCHIVIO.py ArabSniper_Archivio_R2.zip --restore cartella_originali

Il programma verifica gli hash e ricostruisce anche i JSON multipart, con
memoria limitata. Alcuni JSON ricostruiti possono occupare decine di GB:
preferire il documento della data o del mercato da confrontare.

Le schede mantengono tutti i campi salvati: categorie, quote, movimenti,
probabilità, lambda, statistiche, risultati e osservazioni quando presenti.
Usare fixture_id per unire la stessa partita, mantenendo separati data di
osservazione, versione, mercato, periodo e bookmaker. Dati assenti non sono
zeri e non vengono inventati. Una partita può avere molte osservazioni.
Gold/Platinum/Studio/Radar vanno letti dai campi originali, senza dedurli
retroattivamente dal risultato. Non tutte le partite osservate sono selezioni.
L'archivio può contenere anche partite future o ancora pendenti.

Esportazione non equivale a certificazione degli esiti: controllare punteggi,
stato finale e provenienza dell'audit prima di calcolare il rendimento.
Non comprende versioni R2 orfane/non referenziate né altri prefissi del bucket.
Il dossier software separato contiene la revisione sorgente per il confronto.
