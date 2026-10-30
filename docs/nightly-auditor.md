# Auditor notturno e Fast leggeri

Configurazione del 30 ottobre 2026, fuso `Europe/Rome`.

| Processo | Pianificazione | Protezioni |
| --- | --- | --- |
| Auditor storico | `30 0 * * *`, una volta al giorno alle 00:30 | Ammissione 00:30–00:59; stop assoluto 01:50; marcatore giornaliero nel checkpoint pubblicato |
| Backup Auditor esterno | cron-job.org: `40,50 0 * * *`, tutti i giorni | Avvio solo se assente, nessun writer attivo e nessun completamento verificato; configurazione del servizio esterno separata dal repository |
| Night Scan | `0 2 * * *`, dalle 02:00 | Guardiano giornaliero esistente; backup esterno delle 02:20 invariato |
| Fast Scan | `0 6-23 * * *`, ogni ora dalle 06:00 alle 23:00 | Guardiano e backup esterno esistenti; nessuna elaborazione storica in coda allo scan |
| Freeze orario | Rimosso | Il vecchio ingresso manuale mostra solo una nota; nessuna scrittura |

GitHub Actions può consegnare un evento cron in ritardo. Le 00:30 sono l'orario
configurato, non una garanzia del provider al minuto. Un evento Auditor fuori
finestra viene scartato prima di accedere alla coda dei writer o ai dati privati.
Se un altro writer è già attivo, l'Auditor non si accoda. Un secondo controllo
all'avvio effettivo e il limite assoluto delle 01:50 proteggono il Night delle 02:00.
Un ciclo interrotto fallisce visibilmente, senza essere registrato come completato.

## Risveglio esterno dell'Auditor

Il workflow `auditor_backup.yml` è richiamato da cron-job.org alle **00:40 e 00:50
Europe/Rome**, fine settimana inclusi. Lo scheduler principale resta alle 00:30.
Il controllo esterno non legge archivi, non usa API-Football e non entra nel mutex
dei writer. Fuori dalla finestra 00:40–00:59 termina prima di interrogare GitHub.

Se un Auditor o un altro writer è attivo/in coda, il backup non avvia nulla. Se
l'Auditor di oggi ha il passaggio `Record verified daily Auditor completion`
riuscito, il backup termina: un workflow verde con sola ammissione saltata non
vale come completamento. Il passaggio di prova è emesso soltanto dopo una
pubblicazione completa o la verifica del marcatore nel checkpoint già pubblicato.
Il marcatore giornaliero e il mutex restano la protezione finale contro le corse
fra scheduler principale e backup. Gli errori attendono almeno 10 minuti prima
del tentativo successivo e, dopo due fallimenti nella stessa notte, il controllo
si ferma con errore visibile. Non forza mai un avvio dopo le 01:00.

Configurazione del job esterno (da salvare nel pannello cron-job.org):

- Metodo `POST`, URL
  `https://api.github.com/repos/arabsnipertech-bet/arabsniper2/actions/workflows/auditor_backup.yml/dispatches`.
- Corpo `{"ref":"main","inputs":{"dry_run":false}}`.
- Stessi header GitHub e token Actions limitato già usati dal backup Fast;
  nessuna chiave o credenziale nel repository.
- Fuso `Europe/Rome`, minuti `40,50`, ora `0`, tutti i giorni/mesi/giorni settimana.
- Il test HTTP deve ricevere `204`; verificare anche il relativo workflow GitHub.
  Un test diurno deve riportare `outside_audit_backup_window` senza avviare Auditor.

Il backup protegge dal mancato trigger dello scheduler GitHub. L'esecuzione
continua a dipendere dalla disponibilità di GitHub Actions: in caso di indisponibilità
dei runner non può garantire la partenza al minuto, né oltrepassare i limiti notturni.

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
- Backup esterno: ulteriori 16 test su risveglio, duplicati, ammissioni saltate,
  errori e cooldown, fuso italiano, risposte incomplete e prova di completamento.
- Totale locale: 430 test superati.
- Sintassi YAML verificata; filtri Gold, Platinum, Combo e Radar invariati.
- Il tempo del primo Fast e il primo Auditor delle 00:30 devono essere misurati
  in produzione; i test non dimostrano da soli la puntualità del servizio cron.
