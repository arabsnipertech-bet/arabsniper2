# Audit movimenti quota — 21 e 22 ottobre 2026

Analisi esplorativa della versione context-goals-v3.4: 123 partite archiviate, 121 con esito disponibile e 2 pendenti. Ogni mercato usa l'ultimo snapshot prematch selezionato dal daily report e il suo esatto record sorgente. Non sono stati modificati modello, soglie o sito online.

Riproduzione: `python auditor/movement_review.py`. Il JSON omonimo contiene fonti, hash, aggregazioni separate per giornata e tutte le righe, inclusi errori e pendenti.

## Definizioni

Colori riprodotti dalla funzione UI: giallo = variazione assoluta almeno 3% e inferiore a 8%; rosso = almeno 8%. Occorrono almeno due operatori concordi e la maggioranza stretta degli operatori confrontabili. Il colore riguarda l'ultimo tratto direzionale osservato, non necessariamente il movimento netto iniziale/finale. Salita e discesa si riferiscono alla quota decimale. Movimento non confermato e storico assente sono separati dal verde confermato; assenza di giallo/rosso non significa quota stabile.

I mercati Over e gli esiti 1/X/2 sono analizzati separatamente. La favorita 1X2 è l'esito con probabilità book più alta nello snapshot; nessuna informazione del risultato viene usata per sceglierla. Le aggregazioni si sovrappongono e non vanno sommate.

## Colore sulla quota dello stesso Over

| Segnale | Over 2,5 FT | Over 1,5 PT |
|---|---:|---:|
| Tutti | 73/121 (60,3%) | 39/121 (32,2%) |
| Verde confermato | 15/25 (60,0%) | 9/29 (31,0%) |
| Giallo | 21/32 (65,6%) | 15/39 (38,5%) |
| Rosso | 7/12 (58,3%) | 3/12 (25,0%) |
| Giallo o rosso | 28/44 (63,6%) | 18/51 (35,3%) |
| Senza giallo/rosso, inclusi sconosciuti | 45/77 (58,4%) | 21/70 (30,0%) |

L'intensità maggiore non corrisponde a una frequenza maggiore di Over. Per Over 2,5 il rosso fa 3/4 il primo giorno e 4/8 il secondo; per Over 1,5 PT il giallo passa da 4/15 a 11/24. Nessuna soglia è validata da questi due giorni.

## Colore sul mercato 1X2

| Almeno un esito 1/X/2 giallo o rosso | Over 2,5 FT | Over 1,5 PT |
|---|---:|---:|
| Presente | 44/80 (55,0%) | 22/80 (27,5%) |
| Assente | 29/41 (70,7%) | 17/41 (41,5%) |
| Assente, limitato a storico disponibile su tutti gli esiti | 27/36 (75,0%) | 16/36 (44,4%) |

La direzione dell'associazione è negativa in entrambe le giornate: FT con segnale 19/36 e 25/44, senza 18/27 e 11/14; PT con segnale 8/36 e 14/44, senza 9/27 e 8/14. Anche limitando il segnale alla favorita si osservano 32/60 FT e 15/60 PT. Questo può essere un segnale da controllare, non prova che il movimento causi meno gol: campionati, probabilità iniziali e copertura dello storico possono spiegare parte della differenza.

## Direzione e incrocio FT/PT

- Quota Over 2,5 in salita gialla/rossa: FT 12/19, PT 8/19. Per il PT sono 5/12 il primo giorno e 3/7 il secondo.
- Quota Over 2,5 in discesa gialla/rossa: FT 16/25, PT 8/25 (0/4 e 8/21).
- Quota Over 1,5 PT in salita gialla/rossa: PT 7/22.
- Quota Over 1,5 PT in discesa gialla/rossa: PT 11/29.

L'incrocio salita FT / esito PT è una pista esplorativa, selezionata fra molte comparazioni e con soli 19 casi. Non giustifica un bonus automatico. Il JSON include rendimenti teorici a puntata unitaria delle quote archiviate: mancano prezzi per quattro partite concluse, esclusi dai rendimenti; non sono rendimenti eseguiti né stime di guadagno futuro.

## Pallino d'oro ed escluse

Tra tutti gli Over 2,5 con pallino d'oro, quelli con movimento proprio giallo/rosso fanno 18/26 (69,2%); gli altri 23/30 (76,7%). Imporre il colore al pallino non migliora quindi la frequenza osservata.

Nessuna delle nove partite vicine alle soglie del pallino (definizione esplorativa precedente: entrambe le percentuali almeno 47, scarto massimo 13 punti, senza gold) ha movimento giallo/rosso sulla propria quota Over 2,5. Le sei riuscite non sono quindi spiegate da quel segnale. Due hanno giallo sulla quota Over 1,5 PT: Avispa Fukuoka–Gamba Osaka e Al Ahly–Olympic El Qanah; entrambe hanno realizzato l'Over 2,5, ma due casi non bastano a formulare una regola. Anche le tre perdenti sono mantenute nei dati.

## Decisione di ricerca

Mantenere separati colore, direzione, mercato, consenso e inversioni. Prima di un cambiamento operativo, verificare su giornate successive le ipotesi fissate qui, confrontando gruppi con fasce book/statistiche simili e copertura dello storico comparabile. Due giornate, molti confronti e piccoli sottogruppi non consentono di definire una correlazione affidabile o il range migliore. Il possibile uso dei movimenti 1X2 come filtro negativo merita verifica; un generico bonus per giallo/rosso non è sostenuto da questo campione.
