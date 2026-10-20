# Partite dopo il calcio d'inizio

Il dossier V3 conserva le partite del giorno già analizzate. Filtri: tutte,
da iniziare, in corso/aggiornamento atteso, terminate, altri stati.
Il semplice trascorrere del kickoff non certifica che la partita sia iniziata.

Il backend usa la risposta fixtures già richiesta dalla scansione, senza chiamate
aggiuntive, per aggiornare status, minuto, gol, PT e FT. Le analisi e quote
prematch rimangono inalterate. Un errore conserva l'ultimo timestamp riuscito.
Il minuto è quello osservato dal provider, non un cronometro stimato nel browser.

PT verificato soltanto dall'intervallo; FT soltanto a fine gara e sui 90 minuti,
senza sommare supplementari/rigori. Dati mancanti non equivalgono a 0-0.
Rinvii, annullamenti e abbandoni non vengono classificati come sconfitte.

Questa modifica non aggiunge quote live né probabilità live. La pagina lo indica
esplicitamente. Gli aggiornamenti seguono le scansioni periodiche: il pulsante
ricarica legge il JSON disponibile, non avvia una chiamata live al provider.
La permanenza riguarda le partite già conservate nel JSON del giorno selezionato;
l'archivio storico audit rimane separato dalla finestra giornaliera del sito.
