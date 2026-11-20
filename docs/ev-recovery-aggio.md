# Recupero statistico EV–aggio

## Estensione ai ruoli, 20 novembre 2026

`EV_RECOVERY_AGGIO_V1` può qualificare anche Consigliata e Colpaccio con
`-10% <= EV originale < 0%`. Le scelte operative positive vengono completate
per prime. Il recupero riempie ruoli operativi liberi prima del riempimento Studio,
senza sostituire scelte positive o impegni prematch già congelati.

| Ruolo | Probabilità V7 e quota | Evidenza aggiuntiva per il recupero |
| --- | --- | --- |
| Più probabile | Limiti V7 esistenti: almeno 45%, quota 1,55–2,15 | Regola già rilasciata: statistiche e scenario prudente almeno al fair dello stesso operatore |
| Consigliata | Limiti V7 esistenti: almeno 32%, quota 2,15–3,50 | Anche Poisson grezzo e statistica dalle medie devono entrambi raggiungere `1/quota` |
| Colpaccio | Limiti V7 esistenti: almeno 16%, quota almeno 3,50 | Anche Poisson grezzo e statistica dalle medie devono entrambi raggiungere `1/quota`; restano i vincoli di mercato e movimento già previsti per il segno 2 |

Il riferimento `1/quota` è il pareggio economico del contratto binario: non è
una soglia percentuale scelta dal campione. L'unica tolleranza corrisponde
all'arrotondamento delle probabilità salvate a otto decimali. I due calcoli
Poisson/statistico condividono dati sui gol: si richiede concordanza, non si
sommano come prove indipendenti. La classifica all'interno di ciascun ruolo
conserva il criterio V7 esistente; non vengono ottimizzati nuovi pesi sul ROI.

Restano obbligatori dati supportati, storico e statistiche prematch datate,
quota reale eseguibile, modello V7, probabilità fair e aggio coerenti dello
stesso bookmaker, probabilità calibrata e scenario prudente almeno al fair.
L'aggio serve a identificare il riferimento fair; non è sommato all'EV.

La promozione è BLU, etichettata **Rivalutazione statistica**, con EV negativo
originale visibile. La motivazione dei due nuovi ruoli mostra anche Poisson,
statistica e probabilità di pareggio alla quota. Probabilità, lambda, medie,
quote, EV ed EV prudente di origine non cambiano. Nessun ROI è usato nel gate;
nessuna promozione Gold o Platinum deriva da questo recupero.

L'estensione è configurabile separatamente tramite `roles` e
`price_support_roles`. Per tornare al rilascio precedente impostare
`roles = ('most_probable',)`. Per spegnere tutti i recuperi impostare
`enabled = False`. Le selezioni già congelate come Studio non sono promosse
retroattivamente; i nuovi recuperi conservano le prove nei propri impegni.

## Verifica e limiti

I test distinguono i ruoli, le rispettive fasce quota/probabilità, gli esiti
di gate e routing, i segnali positivi preesistenti, i congelamenti e le prove
di supporto. Nessun nuovo scan o recupero API è necessario al rilascio.
Il merge deve contenere `[night-before-fast]` per saltare il Fast da push.

Collaudo locale: **96 test mirati superati** su V7, Platinum e Publisher con
trasporti simulati. Su **64 fixture sintetiche complete**, confronto con il
codice precedente: 32 nuovi recuperi Consigliata e 11 Colpaccio, tutte e 45 le
scelte positive precedenti conservate. Più probabile invariato, così come
probabilità, lambda, quote, EV e annotazioni di prezzo. La configurazione con
il solo ruolo Più probabile riproduce esattamente la baseline nei 64 casi,
incluso il contenuto degli snapshot. Non sono conteggi di partite reali.

Gli estratti storici disponibili non conservano per tutte le alternative la
probabilità statistica separata dal prezzo, il Poisson grezzo e l'intero record
di affidabilità. Il Poisson ricostruito dai lambda calibrati V7 non sostituisce
questa evidenza. Perciò il collaudo funzionale non dimostra un miglioramento
predittivo o di ROI: questi controlli restano successivi e distinti per ruolo.
