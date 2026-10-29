# Platinum: quota e flusso

`platinum-flow-v1.1` aggiunge una classificazione indipendente ai cinque obiettivi
gol già osservati, comprese le equivalenti linee asiatiche a mezzo gol. Non
cambia la whitelist, le probabilità, i criteri, le scelte o le graduatorie Gold
`gold-goals-v6.1`. La Combo PT+ST conserva la propria logica.

Tutti i controlli di flusso sono obbligatori:

- OCI da +30 a +100; quota valida, recente o proveniente dalla cache originale
  di massimo 12 ore.
- Calo almeno del 3% rispetto alla prima quota dello stesso operatore, coerente
  con la serie selezionata e con un aumento della probabilità senza margine.
- Almeno due operatori concordi e maggioranza stretta degli operatori abbinati.
- Movimento confermato, persistenza almeno 50%, nessuna inversione.
- Aggio dello stesso operatore compreso fra 0% e 9%.
- **Via A:** EV stimato almeno +10%, con qualsiasi quota valida.
- **Oppure via B:** quota almeno 2,50 e EV stimato almeno −15%; resta ammesso
  l'EV non calcolabile quando manca anche la stima statistica per storico assente.

Il pavimento della via B è inclusivo: −15% passa, valori inferiori sono esclusi
da Platinum e rimangono nel Radar se ne rispettano i controlli già esistenti.
EV nullo con una stima statistica presente, valori non finiti e tipi errati non
vengono interpretati come storico assente. Qualità, linearità e numerosità
statistica non aggiungono blocchi: un EV noto viene confrontato con il pavimento
anche quando la statistica è debole. Platinum
non attesta una probabilità di vittoria superiore al Gold: OCI descrive le
variazioni delle quote, non volumi di denaro misurati.

## Interfaccia

La decisione viene scritta prima del calcio d'inizio dal motore, con versione e
orario. Il client richiede questa decisione e ne verifica i controlli; non
attribuisce Platinum agli archivi anteriori al rilascio. Le selezioni già
registrate rimangono leggibili durante la partita e dopo il risultato.

Il cartellino e il box principale Platinum sono argento, con dicitura in
grassetto. Il pulsante PLATINUM mostra il numero di partite distinte. La griglia
CSS è invariata. Se esiste anche una proposta Gold, resta consultabile nel
dettaglio “Giocata ufficiale Gold v6.1”, con il suo mercato, quota e rango
originali. La categoria Radar mostra gli incontri di studio senza Platinum o
Gold; i controlli del Radar restano identici. Il filtro Combo è indipendente.

## Audit prospettico

Prima della pubblicazione Fast/Night/Auditor, il passaggio di audit registra
separatamente `arab_quote_platinum_audit.json` e il relativo report privato.
La raccolta Platinum non termina alla fine del precedente studio Gold del 29
ottobre. Non richiama API sportive e non ricalcola le scelte storiche.

La versione `platinum-flow-v1.1` identifica il nuovo pavimento EV. Il registro
conserva integralmente gli snapshot `platinum-flow-v1`, distinguendoli nei report.
Le classificazioni v1 di partite già iniziate rimangono leggibili con le regole
originali; soltanto le partite ancora future vengono rivalutate con v1.1.

Ogni rilevazione conserva la scelta principale, prezzo effettivo, operatore,
OCI, movimento, aggio, statistiche disponibili, decisione e orari. Registra anche
il ritiro della proposta. Le viste prima/ultima rilevazione restano separate,
con una proposta per partita. Risultati non definitivi o mancanti restano
pendenti; gli esiti FT usano i 90 minuti. Il ROI è teorico a una unità, non un
registro di scommesse eseguite. Le sovrapposizioni con Gold non vanno sommate.

Il rilascio riclassifica soltanto le partite future delle cinque giornate già
salvate. Non effettua nuove raccolte statistiche. Vecchie schede già iniziate
restano immutabili. I test condividono casi di confine fra Python e JavaScript
e verificano anche il filtro, il colore argento e la vista mobile.
