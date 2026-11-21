# Auditor — ripresa del congelamento prematch

21 novembre 2026. Base `e1215f87d094d30df6344b5f411d2ba5ea4c5621`, successiva alle PR #241–245. Correzione limitata a due moduli dell'Auditor, test e questa nota.

## Incidente verificato

I backup [Fast 44864347083](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44864347083) e [Auditor 44862298460](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44862298460) si sono fermati con `action=retry_limit`, dopo due tentativi Auditor falliti. Il backup Fast era fallito nel controllo dell'Auditor, senza avviare altre scansioni. Il limite di due tentativi ha impedito ulteriori recuperi costosi e resta invariato.

I tentativi [44838538111](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44838538111) e [44841469653](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44841469653) avevano entrambi ripristinato correttamente i dati in circa un minuto. Ciascuno ha poi riletto gli stessi **109 journal** per oltre sei minuti, scritto il congelamento locale e iniziato la liquidazione degli esiti. Il limite protettivo del sottoprocesso è arrivato prima della pubblicazione del blocco risultati. Il congelamento intermedio era quindi perso al termine del runner.

Il successivo [Auditor programmato 44864410185](https://github.com/arabsnipertech-bet/arabsniper2/actions/runs/44864410185) ha verificato e salvato risultati, rapporto e checkpoint di apprendimento V7 del **20 novembre**, più un recupero incrementale del 19 novembre. Job concluso alle **05:12:42 italiane**. I backup successivi sono tornati verdi sullo stesso codice: il ripristino del servizio precede questa correzione preventiva.

## Modifica e garanzie

- Il congelamento giornaliero registra gli SHA-256 dei journal già acquisiti nei propri metadati. I record prematch e i journal originali restano immutati.
- Il ciclo leggero abilita `--checkpoint-freeze`: dopo la scrittura atomica del congelamento, e prima della verifica degli esiti, viene pubblicato il solo file privato giornaliero con i suoi frammenti verificati. Viene riutilizzata la pubblicazione incrementale esistente, con puntatore remoto aggiornato per ultimo.
- Un runner successivo ripristina lo stesso checkpoint e rilegge solo journal nuovi. Il controllo SHA-256 rimane obbligatorio per quelli già acquisiti; un journal modificato produce un errore, non un salto silenzioso. Anche la costruzione dell'indice descrittivo della cronologia viene evitata quando non ci sono nuovi journal.
- Se un nuovo journal arriva dopo il salvataggio intermedio, viene integrato senza rielaborare quelli già inclusi. I journal rimangono disponibili all'ingestione storica ordinaria: non vengono cancellati o dichiarati consumati.
- Il salvataggio prematch **non certifica risultati o apprendimento** e non crea un indicatore di completamento. Le verifiche precedenti di risultati, rapporto, apprendimento e marker completo restano necessarie. Un errore di salvataggio interrompe la fase prima di eventuali richieste risultati.

Non cambiano budget temporale, riserva di salvataggio, massimo di 20 fixture per recupero, limite di due tentativi, guardie di concorrenza, workflow o calendari. Nessuna modifica a probabilità, EV, recupero aggio, Triade, Gold, Platinum, apprendimento, pubblicazione Gold o canali social.

Il primo tentativo richiede una scrittura privata intermedia aggiuntiva. Se il processo termina prima di completarla, resta valido il checkpoint precedente. Non si garantisce il completamento di qualsiasi dimensione d'archivio entro il limite: la protezione resta attiva. Il risparmio di tempo reale andrà misurato sul prossimo ciclo naturale; la prova locale verifica il lavoro evitato, non inventa una durata di produzione.

## Collaudo e rilascio

Otto prove dedicate coprono: timeout dopo 109 journal e ripresa in un nuovo runner con **zero riletture dei 109 journal**; ingestione di un solo journal nuovo; rifiuto di un journal cambiato; errore di pubblicazione; journal non valido; ripristino dei frammenti compressi; metadati corrotti; blocco del salvataggio fuori dall'archivio privato. Le prove usano storage e risultati locali, senza traffico API-Football o R2.

Suite Auditor mirata: **57 test, zero errori o fallimenti**. Suite Python completa: **982 test in 96,051 secondi, zero errori o fallimenti**. Eseguiti anche tutti i **20 file di test Node**, conclusi correttamente. Prima del merge resta obbligatoria la CI della PR.

I file modificati non attivano `Gold yesterday integration`, Fast, Night, Auditor o Publisher su push. Attivano la CI di selezione sulla PR. Stima: **3–4 minuti Actions aggiuntivi**. Il saldo economico residuo dell'account non è leggibile tramite le API repository disponibili. Dopo la CI verde, il solo commit di integrazione può usare `[skip ci]` per evitare la seconda esecuzione identica su main; nessun check della PR viene saltato. Nessun dispatch manuale.

Rollback: revert dei due moduli e dei relativi test, mantenendo i dati acquisiti. Il codice precedente ignora i metadati aggiuntivi del congelamento e continua a poterli leggere. Non è necessario cancellare journal, risultati o checkpoint.
