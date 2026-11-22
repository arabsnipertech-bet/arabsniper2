# V7 — qualifiche dei movimenti e validazione prospettica

Riferimento: `ArabSniper_Rialzi_Ribassi_Statistiche_Correzioni_2026-11-22.md`,
22 novembre 2026, e audit V5/V7 del 9–16 novembre. Le associazioni osservate
non dimostrano un nuovo bonus probabilistico generalizzabile. Questa patch
separa quindi la correttezza delle spiegazioni dalle ipotesi da misurare.

## A. Modifiche utilizzabili in produzione

Non cambiano kernel, pesi V7, calibrazione, probabilità, quote o formule EV.
Restano il piccolo Plus PT già approvato, le regole di compatibilità e la scelta
sull'ultima scansione prematch valida. Non viene introdotto un nuovo blocco
generico dei rialzi né un vincolo rigido T−60 minuti.

### Due confronti espliciti

| Campo | Significato |
| --- | --- |
| `opening_odd` / `first_odd` | Prima quota realmente osservata dello stesso operatore; non apertura ufficiale inventata |
| `opening_delta_pct` | Variazione tra quella prima osservazione e la quota corrente |
| `delta_pct`, `delta_basis=last_leg` | Movimento dell'ultimo tratto, anche dopo un'inversione |
| `delta_observed_from/to` | Finestra temporale del tratto, quando documentata |
| `selected_movement.price_reference` | Confronto completo stesso operatore, mantenuto separato dal tratto |

Esempio di regressione: **2,40 → 2,00 → 2,20** significa **−8,33% dalla
prima osservata**, ma **+10% nell'ultimo tratto**, iniziato alla seconda
osservazione. Anche il ricalcolo del prezzo conserva questa distinzione.

I criteri Gold/Platinum che già usavano l'apertura continuano a leggerla tramite
`opening_change`; non vengono applicati per errore al nuovo campo dell'ultimo
tratto. Per una quotazione riutilizzata da cache resta la neutralizzazione del
flusso già esistente, conservando separatamente l'apertura storica.

OCI ha tre rappresentazioni distinte: attuale mancante (`oci=None`, stato
`missing`), attuale neutro (`oci=0`, stato `neutral`), valore precedente in
`last_confirmed_oci` con il proprio timestamp. Uno zero non sostituisce l'ultimo
flusso confermato e un valore storico non conferma il flusso attuale.

### Motivo e qualifica coincidono

`v7_confirmation.py` spiega una qualifica già documentata, senza creare nuovi
voti statistici. Il supporto statistico resta quello V7; non viene aggiunta una
nuova soglia universale sul Poisson grezzo.

| Caso | Motivazione visibile | Uso |
| --- | --- | --- |
| Calo e valore/supporto qualificati | Calo con conferma statistica | Conserva la qualifica |
| Calo senza qualifica documentata | Quota in calo, conferma insufficiente | Osservazione |
| Rialzo e valore/supporto qualificati | Rialzo con supporto documentato | Conserva la qualifica |
| GG PT ≥5,50, rialzo ≥5%, OCI attuale <−50 e pattern statistico specifico | Studio GG PT contrarian | Studio, senza promozione automatica Gold |
| Altro rialzo | Rialzo senza conferma | Osservazione |

Il valore richiede un prezzo eseguibile, fair e aggio coerenti dello stesso
operatore, supporto V7, aggio sano e le verifiche prudenti esistenti. Un eventuale
**recupero statistico a EV negativo** resta una qualifica distinta: deve portare
il certificato `EV_RECOVERY_AGGIO_V1` coerente con prezzo, probabilità e statistiche
attuali. Non si somma mai l'aggio all'EV e non si rinomina il recupero valore
positivo. Se un GG PT è già qualificato per valore, mantiene questa motivazione;
lo studio contrarian non ne diventa la giustificazione alternativa.

Le osservazioni restano consultabili nella triade come **Osservazione · ruolo**
e in Explorer come **Osservazione V7**; non sono segnali confermati. Badge,
tooltip, motivazione, radar e qualifica usano lo stesso esito. Il fallback
generico `Trend Quota` è eliminato dalle nuove motivazioni.

Le spiegazioni già congelate restano quelle originali: non si riscrivono vecchi
snapshot per farli apparire conformi alla nuova regola.

## B. Quattro ipotesi esclusivamente shadow

Protocollo `V7_MOVEMENT_SHADOW_20261122_V1`, catturato solo durante una nuova
analisi prematch. `decision_shadow.py` congela configurazione, hash delle prove,
quote, probabilità, EV originali e proposte alternative. Non chiama provider o
publisher e non usa risultati o ROI per decidere.

Un controllo `nov22_control` usa gli stessi candidati quotati e statisticamente
supportati. Mantiene ranking ordinario, Plus PT, recuperi documentati e alternative
qualificate esistenti. Le varianti si confrontano con questo controllo;
le scelte effettivamente pubblicate restano una serie separata.

| Ipotesi | Serie congelata | Verifica |
| --- | --- | --- |
| Direzione, fascia, OCI | `nov22_oci_window` | Over 1,5 PT / Over 2,5 FT, 0<OCI<50, EV≥0; Over FT senza tratto ≤−5%; rialzi con `without_peak` valido e valore separati |
| Incroci della matrice | `nov22_cross_markets` | Un vettore 1X2 completo di un solo operatore; favorita iniziale 1,35–1,70, inversione, verso della favorita × verso del mercato gol; GG PT su + Over 2,5 FT giù a quota 1,80–2,50 |
| Attrito di prezzo | `nov22_margin_friction` | Aggio oltre 6,5% FT / 8% PT-ST; penalità solo sul punteggio shadow, mai sulla probabilità o sull'EV registrato |
| Ranking della triade | `nov22_triad_rank_21`, `nov22_triad_rank_25` | Floor Under PT/ST 1,75; preferenze di fascia; Colpaccio negativo escluso se esiste un candidato non negativo ammissibile; due soglie GG PT fisse 21% e 25% |

Il Colpaccio GG PT richiede inoltre quota ≥4,20 ed EV effettivamente positivo:
**21% a quota 4,20 produce EV −11,8%**, quindi non basta superare il floor.

L'attrito è un esperimento di prudenza supplementare:

```
quota_riferimento = 1 / (fair_stesso_operatore × (1 + aggio_riferimento))
costo = p_V7 × (quota_riferimento − quota_osservata)
punteggio_shadow = EV_originale − costo
```

Il costo è zero sotto la soglia. Il rendimento a margine di riferimento è
registrato come **controfattuale**, tenendo fissi fair e probabilità. Conta le
quote con EV negativo che diventerebbe non negativo a quel margine; non dimostra
che l'aggio fosse l'unica causa della perdita né sostituisce l'EV reale.

## Monitoraggio, integrità e risorse

- `settle_v7` aggiunge esiti in una copia separata dei dati shadow: l'evidenza
  prematch resta immutata. Gli snapshot privi del protocollo non vengono ricostruiti.
- Il report giornaliero esistente include Brier, log-loss, hit rate e ROI con
  bootstrap a cluster per `fixture_id` (400 repliche, intervallo 95%). Le tre
  posizioni della stessa partita non diventano tre partite indipendenti.
- Brier/log-loss richiedono eventi binari; push e pagamenti parziali mantengono
  il proprio EV/ROI con denominatori separati. Mancanti, copertura e conflitti
  sull'orario sono espliciti. `latest` e `before_hour` restano distinti.
- Un cambio di mercato è una diversa selezione, non un miglioramento della
  probabilità sullo stesso evento. I delta di calibrazione appaiati vengono
  calcolati soltanto quando il mercato coincide. ROI resta un controllo.
- Nessuna promozione automatica e nessun collegamento delle nuove serie ai pesi
  di apprendimento. Per cambiare produzione servirà una valutazione prospettica.
- Il report legge gli archivi già aperti dal ciclo giornaliero. I mercati senza
  prezzo non vengono duplicati nel ledger; la loro copertura è contata. Nessun
  nuovo workflow schedulato, richiesta API-Football o invio social nei test.

## Verifiche della patch

- Suite completa Python e tutti i file Node eseguiti localmente.
- Confronto locale con `e7a95d71ab21be0a23cf04f5e93fe78a13aefedf`: **438
  osservazioni di mercato su 6 casi sintetici**, con e senza quote, probabilità
  grezze/statistiche/calibrate, fair, quote, EV ed EV_low identici.
- Regressioni sui due esempi dell'utente, su OCI zero/mancante/storico, inversione
  di quota, riprezzamento, certificati di recupero, controparti 1X2 coerenti,
  risultati estranei alla decisione, floor GG ed esiti shadow senza mutazioni.
- CI Python/Node/browser: collaudo completo della PR, inclusi viewport desktop,
  390 e 360 pixel per le nuove qualifiche. Il test browser usa dati sintetici e
  blocca la rete. Le altre suite browser esistenti restano attive.

Questi controlli verificano la correttezza tecnica e l'isolamento delle varianti;
non attestano un miglioramento della percentuale di riuscita.
