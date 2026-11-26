# Verifica V7 prima del weekend — 26 novembre 2026

La priorità emersa dai dati è completare i risultati dello storico recente.
Il confronto attuale non giustifica nuove soglie o sostituzioni automatiche di
mercato. V7 conserva il peso mercato 0,80, le lambda, i gate, i ruoli e il
selettore esistenti. Gold ed Explorer mantengono l'interfaccia attuale.

## Perimetro verificato

Sette giornate chiuse, 19–25 novembre, da un unico manifest privato verificato.
Il cutoff del rapporto è il 26 novembre alle 08:00 italiane. Ogni file e parte
compressa è verificato tramite checksum; nessuna fotografia viene riempita con
dati futuri. Il rapporto completo pubblica tutte le celle predefinite, comprese
quelle sfavorevoli, con 4.000 repliche bootstrap per fixture e giornata sulle
configurazioni registrate. Le configurazioni diverse restano separate.

| Data | Gare archiviate | Liquidate | In attesa | Rinvii | Void |
|---|---:|---:|---:|---:|---:|
| 19 novembre | 20 | 20 | 0 | 0 | 0 |
| 20 novembre | 84 | 75 | 1 | 8 | 0 |
| 21 novembre | 292 | 278 | 0 | 13 | 1 |
| 22 novembre | 187 | 172 | 0 | 15 | 0 |
| 23 novembre | 46 | 19 | 27 | 0 | 0 |
| 24 novembre | 60 | 20 | 40 | 0 | 0 |
| 25 novembre | 65 | 20 | 45 | 0 | 0 |
| Totale | **754** | **604** | **113** | **36** | **1** |

Le 112 attese del 23–25 novembre limitano la rappresentatività dei risultati
recenti e delle nuove coorti shadow. Una partita in attesa non è una persa.
Un risultato FT disponibile non implica automaticamente un PT disponibile.

Il log dell'Auditor `45723190302` registra il fallimento della lettura della
cache live, seguito dal recupero limitato a 20 ID. Il blocco V7 è stato salvato;
questo certifica gli output disponibili, non la completezza di tutti gli esiti.
Il successivo recupero ha elaborato il 9 novembre mentre restavano aperte
giornate molto più recenti: i blocchi mai tentati precedevano quelli già tentati.

## Risultati già presenti nella cache

Una verifica separata alle 08:28 italiane legge esclusivamente il Worker live,
senza richiamare API-Football e senza scritture. Non è un input retroattivo al
backtest delle 08:00.

| Data | Attese / PT mancante | Finali PT+FT già disponibili | Void o rinvio confermato | Ancora senza conferma |
|---|---:|---:|---:|---:|
| 22 novembre | 1 PT mancante | 1 | 0 | 0 |
| 23 novembre | 27 | 26 | 1 | 0 |
| 24 novembre | 40 | 36 | 4 | 0 |
| 25 novembre | 45 | 34 | 4 | 7 |

Sono quindi recuperabili dalla cache 96 finali delle gare in attesa, nove stati
amministrativi e un PT mancante. Gli altri sette casi restano non confermati.
Il singolo caso del 20 novembre è fuori dal perimetro della cache recente di
questa riconciliazione e resta distinto.

## Auto-miglioramento e qualità delle proposte

L'ultimo rapporto di apprendimento comprende 1.126 fixture. Il controllo
cronologico usa 427 fixture di training e 503 di holdout; le righe dei diversi
mercati sulla stessa partita non sono osservazioni indipendenti.

Il peso mantenuto è **0,80**. Il candidato scelto è ancora 0,80; la variante
0,75 non migliora il training. L'aggiornamento riporta `applied=false` e
`baseline_retained`. Sul holdout il Brier V7 è 0,177150 e quello fair 0,177688:
la piccola differenza puntuale non dimostra una superiorità stabile. Non si
introduce una modifica produttiva per inseguire l'ultima settimana.

Tra le scelte ufficiali liquidate nel campione, la Più Probabile centra
339/544, la Consigliata 199/531 e il Colpaccio 141/545. Sono ruoli, prezzi e
probabilità differenti: queste frequenze non ne stabiliscono una graduatoria
di valore economico. Il ROI resta un controllo, non sceglie soglie o mercati.

Le vecchie scelte ufficiali non conservano sempre la P originaria: nel campione
liquidato solo tre scelte per ruolo hanno la probabilità originale verificabile.
La probabilità dell'ultimo snapshot è invece disponibile per l'audit dei mercati.
I due campioni non vengono confusi. Il nuovo protocollo del 25 novembre
migliora la tracciabilità prospettica; i dati mancanti del passato restano mancanti.

## Over 1,5 PT e GG FT

Le coppie con entrambi i prezzi utilizzabili, supporto registrato e stesso
operatore restano piccole e appartengono a configurazioni diverse:

| Configurazione | Coppie liquidate | Over 1,5 PT centrati | GG FT centrati |
|---|---:|---:|---:|
| `435a2875…` | 12 | 6 | 6 |
| `bcc5b484…` | 6 | 4 | 4 |
| `233686dd…` | 2 | 0 | 0 |
| `71f30a67…` | 13 | 3 | 6 |

Le probabilità e i prezzi dei due eventi sono diversi. Il Brier appaiato è
confrontato solo con il fair dello stesso evento, mai sottraendo direttamente
il Brier del GG a quello dell'Over. Anche il braccio shadow GG FT nella
Consigliata mostra direzioni diverse tra configurazioni. Non emerge una regola
generale per sostituire l'Over con il GG o per forzare più Over in Explorer.

I nuovi osservatori «GG FT squadra debole» e «2 FT grezzo/mercato» hanno ancora
una sola giornata prospettica e pochissimi esiti valutabili nel rapporto
iniziale. Il bootstrap per giornata non è disponibile con un solo cluster.
Restano in shadow, con zero promozione automatica.

## Interventi mirati

1. La lettura della cache conserva il percorso esistente e aggiunge un unico
   fallback con lo stesso trasporto del watchdog live: GET sul medesimo endpoint,
   timeout e dimensione limitati, nessun refresh e nessuna richiesta al provider.
   Restano le verifiche di identità, kickoff, origine, istante e score a 90 minuti.
2. Il recupero dà precedenza alle giornate incomplete entro la finestra di
   quattro giorni della cache. Restano un solo blocco storico per ciclo,
   il limite di 20 ID, le finestre protette e il mutex già usato da Fast/Night.
3. Una riconciliazione una tantum riutilizza i risultati già disponibili per
   il 22–25 novembre. Non ha credenziali API-Football; protegge byte e SHA delle
   fotografie, kickoff, istanti, pesi e feed Gold prima di pubblicare gli esiti.
   Non addestra, non pubblica proposte, non aggiorna proiezioni promozionali
   e non invia messaggi. Gli esiti non verificabili restano in attesa.

La riparazione rende più completi i dati per il prossimo ciclo ordinario di
valutazione. Non promette un aumento immediato delle vincite e non certifica
nuove regole di selezione sulla base di campioni ancora incompleti.

## Evidenza riproducibile

- Primo rapporto in sola lettura: run `45773939454`.
- Rapporto con verifica della cache e correzioni: run `45775544346`.
- Il JSON completo conserva manifest, SHA dei file, tutte le celle, intervalli,
  calibrazione, Brier/log-loss, composizione e controlli ROI.
- L'esito della riconciliazione viene allegato al workflow del merge come
  `cached-v7-reconciliation`: i conteggi disponibili sopra non sono presentati
  come già pubblicati prima di quel controllo.
- Test dedicati verificano cache primaria/fallback, assenza di risultati
  inventati, PT zero contro PT mancante, timestamp futuri, priorità recente,
  integrità dei forecast e divieto di modificare pesi o feed.
