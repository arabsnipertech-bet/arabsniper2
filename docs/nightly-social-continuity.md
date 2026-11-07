# Continuità Auditor e social — 7 novembre 2026

Tutti gli orari indicati sono Europe/Rome. I trigger GitHub possono arrivare in ritardo: gli orari sono finestre, non garanzie al minuto.

## Auditor

- Trigger principale alle 00:30 e 04:30.
- Backup interno ogni 20 minuti tra 00:00 e 05:40; il controllo ammette solo 00:40–01:29 e 04:00–05:09.
- Fast Backup e Night Backup richiamano lo stesso controllo prima dei loro controlli ordinari. La modalità dry_run resta senza dispatch.
- Le ammissioni del runner rimangono 00:30–01:29 e 04:00–05:09; scadenze rispettivamente 01:50 e 05:30. L'intervallo centrale resta riservato al Night Scan.
- Nessun avvio con un altro data writer attivo; nessuna chiamata ai dati privati o al provider sportivo fuori finestra.
- Una workflow verde con job saltato non è completamento. Il backup richiede la prova del passo di checkpoint completo; un audit sospeso deve riprendere. Due tentativi effettivamente falliti fermano il recupero automatico e segnalano errore.

## Social

| Ora | Contenuto |
| --- | --- |
| 00:30 | Selezioni notturne |
| 04:30 | Selezioni mattino |
| 10:45 | Riscontri verificati del giorno precedente |
| 12:30 | Selezioni pomeriggio |
| 12:45 | Promo |
| 13:00 | Fino a tre nuove selezioni; tripla solo con tre valide |
| 16:45 | Educativo |
| 17:15 | Promo |
| 17:30 | Singola |
| 18:30 | Singola |
| 20:00 | Singola |
| 22:00 | Ultima selezione o promo |

Il report dei risultati è già programmato alle 09:30 Europe/Rome. I riscontri delle 10:45 includono vinte, perse e pendenti; un report assente, incoerente o di altra data non genera statistiche pubblicabili. La ricerca degli artefatti copre anche le pagine successive alla prima.

I nuovi slot usano le stesse verifiche di freschezza, eleggibilità, ricevute e deduplicazione delle partite. Facebook segue la ricevuta Telegram e mantiene il recupero separato. Nessuna pubblicazione manuale di prova è necessaria.

## Verifica operativa

Controllare le ricevute tg-AAAA-MM-GG-HHMM-sent e Facebook, oltre allo stato del workflow. Per la notte controllare il completamento effettivo dell'Auditor e il checkpoint R2. Il passaggio dei test non certifica anticipatamente una futura esecuzione notturna.
