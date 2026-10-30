# Auditor notturno e Fast leggeri

Configurazione del 30 ottobre 2026, fuso `Europe/Rome`.

| Processo | Pianificazione | Protezioni |
| --- | --- | --- |
| Auditor storico | `30 0 * * *`, una volta al giorno alle 00:30 | Ammissione 00:30–00:59; stop assoluto 01:50; marcatore giornaliero nel checkpoint pubblicato |
| Night Scan | `0 2 * * *`, dalle 02:00 | Guardiano giornaliero esistente; backup esterno delle 02:20 invariato |
| Fast Scan | `0 6-23 * * *`, ogni ora dalle 06:00 alle 23:00 | Guardiano e backup esterno esistenti; nessuna elaborazione storica in coda allo scan |
| Freeze orario | Rimosso | Il vecchio ingresso manuale mostra solo una nota; nessuna scrittura |

GitHub Actions può consegnare un evento cron in ritardo. Le 00:30 sono l'orario
configurato, non una garanzia del provider al minuto. Un evento Auditor fuori
finestra viene scartato prima di accedere alla coda dei writer o ai dati privati.
Se un altro writer è già attivo, l'Auditor non si accoda. Un secondo controllo
all'avvio effettivo e il limite assoluto delle 01:50 proteggono il Night delle 02:00.
Un ciclo interrotto fallisce visibilmente, senza essere registrato come completato.

## Osservazioni prepartita preservate

Ogni scan conserva solo le nuove osservazioni in batch immutabili
`data/arab_quote_scan_journal_*`. Un secondo batch congela le decisioni Gold e
Platinum effettivamente predisposte per la pubblicazione. I controlli temporali,
le impronte e le funzioni di cattura esistenti restano attivi: nessuna previsione
viene ricostruita retroattivamente dopo il calcio d'inizio.

L'Auditor notturno importa i batch in ordine di osservazione, preserva la prima
previsione e distingue gli aggiornamenti successivi. Poi archivia, verifica i
risultati e ricalcola i rapporti. Le eliminazioni dei batch pendenti diventano
durature insieme agli archivi uniti nello stesso checkpoint; un upload fallito
lascia disponibile lo stato remoto precedente. La reimportazione è idempotente.

Il Fast non interroga più risultati storici e non ricostruisce i rapporti Gold,
Platinum o multimercato. I rapporti disponibili durante il giorno restano quelli
dell'ultimo Auditor completato. Le selezioni e le quote correnti continuano a
essere aggiornate dagli scan con le medesime regole di ammissione.

Il salvataggio privato confronta le impronte sia degli indici sia delle parti
compresse con il checkpoint già verificato: evita di analizzare nuovamente JSON
storici identici. Nuovi dati o file modificati/corrotti richiedono la verifica
completa e non possono sostituire il puntatore del checkpoint in caso di errore.
Resta il mutex finale fra writer per evitare perdite di aggiornamenti concorrenti.

## Verifica

- 400 test Python preesistenti invariati e verdi.
- 14 nuovi test: finestra italiana e ora legale, trigger tardivi, writer occupato,
  esecuzione giornaliera, arresto dei processi alla scadenza, conservazione di
  prima/ultima osservazione, retry, integrità delle scelte e dati corrotti.
- Totale locale: 414 test superati.
- Sintassi YAML verificata; filtri Gold, Platinum, Combo e Radar invariati.
- Il tempo del primo Fast e il primo Auditor delle 00:30 devono essere misurati
  in produzione; i test non dimostrano da soli la puntualità del servizio cron.
