# Triade V7 — alternative qualificate e revisione delle scelte deteriorate

21 novembre 2026. Politica isolata `TRIAD_QUALIFIED_REFRESH_V1` in `src/v7_decision.py`. Base del confronto: `c79075a9ce0d74235b9c430bf85089531f264f9c`; gli aggiornamenti successivi dei soli file pubblici non modificano questa base del motore.

## Problema e comportamento risultante

La Consigliata ordinaria considera un insieme preferito di mercati. Il ripiego Studio considera anche altri mercati, ma può etichettare come Studio un'alternativa già qualificabile oppure scegliere una probabilità maggiore con valore prudente insufficiente. Inoltre, una precedente sostituzione in un altro ruolo impedisce alla revisione ordinaria di riparare una scelta successivamente deteriorata.

La nuova politica interviene dopo le normali assegnazioni e il recupero `EV_RECOVERY_AGGIO_V1`, prima del ripiego Studio:

1. Se manca una Consigliata qualificata, confronta anche gli altri mercati binari supportati e le combo congiunte già calcolate. Richiede quota recente con bookmaker identificato, fair disponibile, supporto V7, qualifica già assegnata dalla pipeline, EV positivo e EV prudente non negativo. Verifica l'identità EV–probabilità–quota, senza riscrivere nessun valore.
2. Mantiene le fasce attuali dei ruoli, incluso `2.15–3.50` per Consigliata, i floor di probabilità e il ranking V7 esistente. Restano obbligatorie unicità dei mercati, compatibilità congiunta degli esiti e coerenza dell'ordine delle quote fra i ruoli.
3. Su partite già pubblicate, può sostituire una scelta che non supera più la qualifica corrente con una candidata positiva e prudente. Conserva il limite esistente di un'ora prima del kickoff e il miglioramento prudente minimo già previsto: 2 punti per club, 4 per nazionali. Esegue al massimo una riparazione per partita e passaggio; eventuali deterioramenti successivi conservano l'intera catena degli impegni.
4. Non interpreta quote o valori mancanti come prova di deterioramento. In questi casi conserva l'impegno storico. Una scelta ancora qualificata, inclusa una rivalutazione EV–aggio valida, non viene sostituita dalla nuova riparazione. La precedente revisione ordinaria mantiene le sue regole.
5. Se non esiste un'alternativa ammissibile, il completamento Studio resta disponibile: nessuna probabilità o promozione viene inventata per riempire uno slot.

La qualifica corrente appartiene al nuovo snapshot. La qualifica storica del primo impegno non viene riscritta. Quando cambia il mercato, il nuovo impegno contiene `replaces`, prezzo e data originali, oltre al motivo della riparazione. I record prematch precedenti restano immutati.

## Confini della modifica

Nessuna modifica a stime V7, medie, lambda, Poisson, EV, Gold/Platinum, calibrazione o gate di apprendimento. Nessuna soglia appresa dagli esiti o dal ROI. Il corpo di `ev_recovery_candidates`, il ranking, il completamento Studio e la revisione ordinaria restano identici alla base. Il recupero negativo conserva i suoi controlli e la fascia `[-10%, 0%)`; questa politica non recupera nuovi EV negativi.

Night, Fast, Auditor, quote, R2, Publisher, Homepage, Telegram, Facebook, Stripe e workflow non vengono modificati. L'integrazione avviene nel normale `capture` chiamato da `matrix_pipeline.build_board`: il primo ciclo automatico che utilizza il nuovo main applicherà la politica ai prematch, rispettando la finestra di congelamento esistente.

## Verifica riproducibile

Suite Python completa: **1.004 test in 96,309 secondi, zero errori o fallimenti**. Tutti i **20 file Node** superati. I **22 test dedicati** coprono alternative qualificate, prezzi e dati insufficienti, mercati incompatibili, mantenimento delle scelte sane e del recupero negativo, riparazione dei tre ruoli, storico originale, orario limite e assenza del ROI dalla scelta.

Confronto locale su **96 scenari sintetici**, dichiarati nello script; non sono partite storiche né una stima di rendimento:

| Ruolo | Mercati cambiati · nuove assegnazioni | Studio → qualifica corrente | Scelte già qualificate sostituite |
| --- | ---: | ---: | ---: |
| Più probabile | 0 | 0 | 0 |
| Consigliata | 10 | 11 | 0 |
| Colpaccio | 2 | 0 | 0 |

Le due differenze del Colpaccio riguardano soltanto ripieghi Studio compatibili con la nuova Consigliata. Tutte le Triadi rimangono complete e congiuntamente compatibili. Con impegni iniziali identici derivati dalla baseline e uguale orario di rivalutazione, si osservano **6 sostituzioni della Consigliata**, tutte con originale conservato; nessuna sostituzione di Più probabile o Colpaccio e nessuna scelta qualificata sottratta dalla nuova politica.

Sono confrontati **14.016 record di mercato**, con zero differenze nei campi di probabilità, prezzo, EV, EV prudente e tier. Una verifica separata contro il codice della base, su 96 casi e 14 impegni congelati, conferma che `enabled=False` riproduce esattamente l'output precedente.

Riproduzione senza rete:

```bash
python scripts/check_v7_qualified_refresh.py --output docs/evidence/v7-qualified-refresh-20261121.json
```

Il JSON allegato registra griglia e conteggi. Gli export storici già disponibili non contengono i limiti prudenti e l'intera matrice necessari a un replay completo delle 1.286 vecchie proposte. I risultati reali diagnosticati fino al 20 novembre non vengono usati per inventare rendimenti controfattuali o nuovi pesi.

## Rilascio e rollback

Previsto un solo collaudo CI della PR, circa 3–4 minuti Actions. La PR non attiva scansioni. Il push ordinario di `src/v7_decision.py` su main attiverebbe un Fast: dopo CI verde si usa `[skip ci] [night-before-fast]` sul solo commit di squash per evitare il giro CI duplicato e la scansione aggiuntiva. Nessun check della PR viene saltato e nessun workflow viene modificato. Calendari e backup continuano normalmente.

Rollback: disabilitare `TRIAD_QUALIFIED_REFRESH_V1['enabled']`, oppure revert della patch, applicando gli stessi marker al commit di integrazione. Nessuna cancellazione o riscrittura dello storico, nessuna ripubblicazione e nessun dispatch manuale.
