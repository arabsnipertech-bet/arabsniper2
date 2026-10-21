# Pubblicazione manuale del sito

Workflow: **Actions → Pubblica sito → Run workflow → main → Run workflow**.
È un comando manuale di riserva per Cloudflare Pages; non esegue scansioni,
non cambia le previsioni e non sostituisce le distribuzioni automatiche.

Creare in Cloudflare Pages, progetto `arabsniperbet`, un Deploy Hook sul ramo
`main`. Conservare il suo URL esclusivamente nel repository secret Actions
`CLOUDFLARE_PAGES_DEPLOY_HOOK`. Non copiarlo nei file, nei log o nelle chat.
Il hook resta utilizzabile finché non viene eliminato/revocato.

Il workflow invia una sola richiesta POST, senza checkout, dipendenze esterne,
redirect o permessi sul contenuto del repository. Non stampa URL o risposta
integrale. Rifiuta avvii su rami diversi da main. Il ramo effettivamente costruito
è quello configurato nel hook: verificare main durante la sua creazione.

Un job verde significa **richiesta accettata**, non build completata.
Il riepilogo contiene l'identificativo restituito da Cloudflare. Controllare la
relativa distribuzione nel progetto e verificare il sito dopo il completamento.
In caso di timeout controllare prima Cloudflare, evitando avvii duplicati.

Per ruotare il hook: crearne uno nuovo su main, aggiornare il repository secret,
provarlo e revocare il precedente. Eliminare un hook non elimina il sito pubblicato.
