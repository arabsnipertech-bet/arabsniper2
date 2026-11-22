# V7: coerenza dei ruoli e delle proposte — 22 novembre 2026

## Difetto riprodotto

Il completamento Blu/Studio accettava qualsiasi mercato quotato e gli
attribuiva la stessa preferenza dei mercati previsti per il ruolo. Inoltre il
numero dei riquadri compilati precedeva la qualità della corrispondenza al
ruolo. Tre osservazioni Under/No Goal potevano così sostituire uno studio
previsto dal ruolo, anche quando il modello attribuiva più probabilità al suo
complementare Over.

Nello screenshot Häcken–Elfsborg, Under 2,5 FT al 41,3% implica Over 2,5 FT al
58,7%. Questo individua un problema di selezione, senza dimostrare un errore
nelle statistiche del campionato o garantire il successo dell'Over. Le quote
delle alternative usate nella regressione sono sintetiche: i quattro snapshot
privati delle 12:39 non erano disponibili nel checkout e la pagina live
richiedeva un nuovo accesso. La regressione non è un replay di quelle partite.

## Correzione in produzione

- Gli studi nuovi completano soltanto i ruoli del già esistente `TRIAD_MARKETS`.
  Le osservazioni fuori ruolo restano nella matrice completa. I candidati che
  rispettano i limiti esistenti del ruolo precedono i completamenti rilassati.
- Le alternative Under realmente qualificate e compatibili restano ammesse
  dalla politica esistente della Consigliata. Restano anche i recuperi EV
  documentati e il Plus PT. Non si introducono bonus Over per campionato,
  nuovi floor o variazioni delle probabilità, dei pesi e dell'EV.
- I lock espliciti di scelte precedenti continuano a essere leggibili. Gli
  snapshot congelati, le quote originali e le appartenenze alle classifiche
  storiche non vengono riscritti.
- Per ogni nuova proiezione Explorer si confronta il mercato con le sole
  proposte Gold confermate ed eseguibili dello stesso snapshot, registrandone
  l'hash. La compatibilità richiede almeno un punteggio PT/ST che consenta la
  vincita congiunta di tutte le scelte: non basta confrontare le parole
  “Over” e “Under”. Uno studio non confermato non può bloccare un'alternativa
  qualificata.
- Gli opposti rimangono nella ricerca completa con la spiegazione visibile
  “Scenario opposto alla triade Gold”. Sono esclusi dalle classifiche correnti,
  inclusa la vista dei ruoli originali Consigliata. Mercati compatibili su
  linee o periodi diversi restano ammessi. Le vecchie classifiche certificate
  conservano la loro composizione originale.
- Il lettore delle proposte confermate rifiuta una triade confermata
  internamente incompatibile, senza modificare la fonte. Home e Publisher
  usano già questo lettore; i test non inviano messaggi.

Le nuove catture riportano `study_selection_policy=TRIAD_ROLE_COHERENCE_V1`.
La stessa revisione entra nel digest della configurazione shadow, separando
i futuri controlli dalle catture precedenti. Le quattro ipotesi quantitative
del 22 novembre restano shadow, senza promozione automatica in produzione.

## Verifiche e limiti

Nove regressioni Python coprono priorità dei ruoli, assenza di riempimenti
fuori ruolo, conservazione delle alternative Under qualificate, compatibilità
tra periodi/linee, opposizioni Explorer e guardia di pubblicazione. I test dei
lock e del floor Under shadow riproducono esplicitamente una scelta legacy:
non richiedono più al nuovo selettore di crearla come studio fuori ruolo.
Il test Node distingue ricerca completa e classifiche; il browser verifica
anche la vista Consigliate e il messaggio mostrato in ricerca.

Un confronto di controllo su **96 casi dichiaratamente sintetici** mantiene
identici gli hash degli input (probabilità, quote, EV e dati statistici):

| Controllo | Prima | Dopo |
| --- | ---: | ---: |
| Riquadri selezionati | 288 | 288 |
| Studi fuori ruolo | 169 | 0 |
| Selezioni prive di quota reale | 18 | 18 |

Cambiano 171 riquadri; nessuna scelta già qualificata cambia nel campione.
Non sono partite storiche, un backtest o una misura dell'aumento di hit rate.
Il rendimento delle nuove selezioni richiede osservazione prospettica.

La patch non aggiunge chiamate API-Football, provider, cron, workflow di test
o invii Telegram/Facebook. Le nuove catture verranno prodotte dai normali
aggiornamenti prematch; le partite già iniziate restano nello storico.
