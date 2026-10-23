# Incrocio movimenti 1X2 e Over — 21–22 ottobre 2026

123 partite archiviate, 121 concluse. Ultimo snapshot prematch context-goals-v3.4, con verifica temporale e riferimenti esatti ai record sorgente. Risultati esplorativi: nessuna nuova regola di affidabilità validata.

## Riscontri principali

| Combinazione (entrambi gialli/rossi confermati) | Mercato valutato | 21 ottobre | 22 ottobre | Totale | Solo Gold FT |
|---|---|---:|---:|---:|---:|
| 1 ↑ + Over ↓ | over25_ft | 0/0 | 8/10 | 8/10 | 6/8 |
| 1 ↑ + Over ↑ | over25_ft | 5/8 | 2/3 | 7/11 | 4/5 |
| 1 ↓ + Over ↓ | over25_ft | 1/2 | 4/9 | 5/11 | 4/8 |
| 2 ↓ + Over ↓ | over25_ft | 0/0 | 6/8 | 6/8 | 4/6 |
| X ↑ + Over ↓ | over25_ft | 1/3 | 9/14 | 10/17 | 8/14 |
| Favorita inizialmente osservata ↓ + Over ↓ | over25_ft | 1/1 | 8/14 | 9/15 | 7/13 |
| Favorita inizialmente osservata ↑ + Over ↑ | over25_ft | 1/2 | 1/3 | 2/5 | 2/3 |
| 1 ↑ + Over ↓ | over15_ht | 0/1 | 6/11 | 6/12 | n/a |
| 1 ↓ + Over ↓ | over15_ht | 0/1 | 1/9 | 1/10 | n/a |
| Favorita inizialmente osservata ↓ + Over ↓ | over15_ht | 0/0 | 5/15 | 5/15 | n/a |
| Favorita inizialmente osservata ↑ + Over ↑ | over15_ht | 1/2 | 0/4 | 1/6 | n/a |

## Interpretazione e scelta per la pagina

Il caso 1 in salita + Over 2,5 FT in discesa ha 8 successi su 10, tutti nella seconda giornata. I Gold del gruppo fanno 6/8, rispetto a 41/56 per tutti i Gold: non dimostra un miglioramento indipendente. Favorita iniziale in discesa + Over 2,5 in discesa fa 9/15, di cui Gold 7/13. Il semplice rafforzamento della favorita non sostiene un nuovo pallino di conferma.

Per il PT, 1 in salita + Over 1,5 in discesa fa 6/12 (0/1 e 6/11); 1 in discesa + Over 1,5 in discesa fa 1/10. La favorita iniziale in discesa insieme all’Over PT fa 5/15, solo nella seconda giornata. Non emerge una regola pronta per reinserire il PT tra le consigliate. Over 2,5 PT: 12/121 nel totale; X in salita + Over 2,5 PT in discesa fa 5/24, suddivisi in 0/5 e 5/19.

Modifiche preparate nella stessa proposta: elenco Over 2,5 FT predefinito, consigliate Over 3,5 solo quando già selezionate dal modello, elenco separato Over 1,5 PT in osservazione, tabella nella scheda con percentuali e movimenti 1/X/2, Over 1,5 PT, Over 2,5 FT e PT. Gold invariato; nessun pallino rosso di maggiore affidabilità e nessun bonus alle percentuali. Nessuna pubblicazione.

## Limiti da preservare nella verifica successiva

- Giallo: variazione assoluta 3–<8%; rosso: almeno 8%, entrambi con almeno due operatori e maggioranza concorde. I gruppi separati per colore sono conservati nel JSON.
- Salita/discesa riguarda l’ultimo tratto, che può seguire un’inversione. La favorita iniziale usa la mediana delle probabilità senza aggio delle prime osservazioni disponibili: non è l’apertura ufficiale e gli operatori possono avere finestre diverse.
- “Continua a scendere” richiede una verifica del percorso. Con il criterio severo di nessuna inversione registrata in nessuno dei due mercati, i gruppi favorita iniziale in calo + Over in calo non hanno casi: non possiamo attribuire i loro risultati a una discesa ininterrotta.
- Gli incroci sono copresenze nello snapshot, non prove di movimenti simultanei. Per studiare la simultaneità occorrono finestre allineate e stessi operatori.
- Il JSON conserva celle statistiche/book di 10 punti, Gold, casi singoli, perdenti, pendenti e giornate. La frammentazione del campione non consente una graduatoria robusta a parità di probabilità iniziali.
- Molti confronti sullo stesso campione producono vincitori apparenti. Queste sono ipotesi da fissare e verificare su giornate successive, non soglie da ottimizzare sui risultati già noti.
- Quote, probabilità book e movimenti sono misure collegate della stessa fonte: non rappresentano tre conferme indipendenti della statistica.

Riproduzione: `python auditor/movement_review.py` e `python auditor/joint_movement_review.py`. Il JSON completo contiene anche gli incroci del movimento FT con il risultato PT e viceversa.
