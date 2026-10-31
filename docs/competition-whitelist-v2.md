# Perimetro competizioni Gold

## Versione attiva v2.2 · 31 ottobre 2026

La policy `gold-senior-whitelist-v2.2` in `src/gold_competitions.py` controlla la competizione prima di acquisire statistiche e quote della singola partita. Fast e Night applicano la stessa decisione al calendario già salvato, alla matrice e alla lista live. Gli archivi prematch già congelati conservano la classificazione originaria.

| Paese | Famiglia esclusa |
|---|---|
| Ucraina | Persha Liga e categorie inferiori |
| Polonia | II Liga e categorie inferiori |
| Norvegia | 2. Division e categorie inferiori |
| Slovacchia | 2. liga, 3. liga e categorie inferiori |
| Bulgaria | Third League e categorie inferiori |
| Repubblica Ceca | 3. liga, ČFL e MSFL, inclusi i gruppi |
| Austria | Regionalliga, compresi i gironi territoriali |
| Romania | Liga III, comprese tutte le serie e i playoff |

I nomi sono normalizzati per paese, accenti, maiuscole e punteggiatura. Le famiglie esplicitamente escluse coprono anche suffissi e gruppi nuovi. Per i paesi europei o UEFA del catalogo, le eccezioni di terzo livello sono la Serie C italiana e la League One inglese. La League Two inglese è ammessa al quarto livello, con gli alias English, EFL, Sky Bet e Football League Two. Le altre seconde divisioni già ammesse e le coppe autorizzate seguono il catalogo per paese.

L’ammissione permette l’osservazione. Le stesse regole Gold e Platinum verificano disponibilità della quota, aggio, valore e flusso di ciascun mercato. Un’autorizzazione di lega non promuove automaticamente una partita.

Durante il passaggio di versione, il browser verifica i feed già salvati con `web/competition-scope.js`. Per aggiornare la parte dati di questo asset dopo una modifica della policy eseguire `python src/gold_competitions.py --sync-browser`. Il test di parità controlla la corrispondenza con il backend e i riferimenti versionati delle due pagine.

## Perimetro storico v2

Effective change requested 27 October 2026. This is an explicit exception to the
code freeze through 2 November; no scheduled monitoring is added or changed.

`gold-senior-whitelist-v2` admits the first three senior league levels of the
same countries as v1. Exact aliases include parallel groups and league promotion /
relegation playoffs. Existing domestic and international cup lists are unchanged.
Youth/reserve fixture exclusions are unchanged. Admission does not award Gold.
`gold-goals-v6`, probability/price limits, OCI, margin and hysteresis are unchanged.

The frontend accepts v1 (a strict subset) during rollout, so the existing feeds
remain visible until the ordinary scan produces v2 data. A whitelist change must
run a normal schedule scan, not a cached price-only refresh: excluded fixtures
are absent from old snapshots and cannot be recovered by repricing them.

## Coverage limits

Names were checked against the [API-Football catalogue](https://www.api-football.com/coverage)
on 27 October 2026. Whitelist membership is permission, not a coverage guarantee.
The public catalogue does not currently list third-level competitions for
Azerbaijan, Bolivia, Colombia, Ireland, Kazakhstan, Paraguay, Peru, Uruguay or
Venezuela. Their explicit senior competition aliases are admitted if supplied by
the provider; this change cannot manufacture fixtures, statistics or bookmaker odds.
No extra provider or paid service is introduced.

Country-local numbering is not the pyramid level: Croatian Second NL, Polish
II Liga and Turkish 2. Lig are third-level; Third NL, III Liga and 3. Lig are
fourth-level and excluded. Finland's third level is Ykkönen, not Kakkonen.
Australia's NPL state leagues remain level 2 alongside the Australian Championship;
state second divisions are level 3. Sources:
[FA NPL](https://nationalpremierleagues.com.au/about-npl),
[Australian Championship](https://australianchampionship.com.au/faqs),
[Football NSW](https://footballnsw.com.au/competitions/league-one-mens/).
Other national sources: [FAI](https://www.fai.ie/national-league/results/),
[PFL Azerbaijan](https://liqa2.pfl.az/), [Kazakhstan](https://qfl.kz/en/league/second-league/),
[APF](https://www.apf.org.py/primera-b), [AUF](https://www.auf.org.uy/documentos/),
[Difutbol](https://apps.difutbol.org/?idCompeticion=341848806),
[FPF Peru](https://fpf.org.pe/wp-content/uploads/2026/06/REGLAMENTO-COPA-PERU-2026.pdf).

## Exact admission aliases

Normalization ignores case, accents and punctuation. No wildcard admits unknown groups.

| Country | First level | Second level | Third level |
| --- | --- | --- | --- |
| austria | Bundesliga | 2. Liga | Regionalliga - Mitte; Regionalliga - Ost; Regionalliga - West; Regionalliga - Salzburg; Regionalliga - Tirol; Regionalliga - North; Regionalliga - South |
| azerbaijan | Premyer Liqa; Premier League | Birinci Dasta; I Liqa; First Division | II Liqa; Second League |
| bulgaria | First League | Second League | Third League - Northeast; Third League - Northwest; Third League - Southeast; Third League - Southwest |
| croatia | HNL; 1. HNL | First NL; 1. NL; 2. HNL | Second NL; 2. NL |
| denmark | Superliga | 1. Division; 1st Division | 2. Division; 2nd Division; 2nd Division - Group 1; 2nd Division - Group 2 |
| finland | Veikkausliiga | Ykkosliiga | Ykkonen |
| france | Ligue 1 | Ligue 2 | National; Championnat National; Ligue 3 |
| germany | Bundesliga | 2. Bundesliga | 3. Liga |
| england | Premier League | Championship | League One |
| ireland | Premier Division | First Division | FAI National League; National League |
| northern ireland | Premiership | Championship | Premier Intermediate League |
| italy | Serie A | Serie B | Serie C; Serie C - Girone A; Serie C - Girone B; Serie C - Girone C; Serie C - Promotion - Play-offs; Serie C - Relegation - Play-offs |
| kazakhstan | Premier League | 1. Division; First Division | 2. Division; Second Division; Second League |
| norway | Eliteserien | 1. Division; OBOS-ligaen | 2. Division; 2. Division - Group 1; 2. Division - Group 2; 2. Division - Play-offs |
| netherlands | Eredivisie | Eerste Divisie | Tweede Divisie |
| poland | Ekstraklasa | I Liga | II Liga; II Liga - East; II Liga - West |
| portugal | Primeira Liga | Segunda Liga; Liga Portugal 2 | Liga 3 |
| czech republic | Czech Liga; First League | FNL; 2. Liga | 3. liga - CFL A; 3. liga - CFL B; 3. liga - MSFL; 3. liga - Promotion Play-off |
| romania | Liga I; Liga 1; Superliga | Liga II; Liga 2 | Liga III; Liga 3; Liga III - Play-offs; Liga III - Serie 1; Liga III - Serie 2; Liga III - Serie 3; Liga III - Serie 4; Liga III - Serie 5; Liga III - Serie 6; Liga III - Serie 7; Liga III - Serie 8; Liga III - Serie 9; Liga III - Serie 10 |
| scotland | Premiership | Championship | League One |
| serbia | Super Liga | Prva Liga | Srpska Liga - Belgrade; Srpska Liga - East; Srpska Liga - Vojvodina; Srpska Liga - West |
| slovakia | Super Liga | 2. Liga | 3. Liga; 3. liga - Bratislava; 3. liga - Center; 3. liga - East; 3. liga - West; 3. liga - Play-offs |
| spain | La Liga; Primera Division | Segunda Division; La Liga 2 | Primera Division RFEF; Primera Federacion; Primera Division RFEF - Play Offs; Primera Division RFEF - Group 1; Primera Division RFEF - Group 2; Primera Division RFEF - Group 3; Primera Division RFEF - Group 4; Primera Division RFEF - Group 5 |
| sweden | Allsvenskan | Superettan | Ettan; Ettan - Norra; Ettan - Sodra; Ettan - Relegation Round |
| switzerland | Super League | Challenge League | 1. Liga Promotion; Promotion League |
| turkey | Super Lig | 1. Lig | 2. Lig; 2. Lig - White; 2. Lig - Red |
| ukraine | Premier League | Persha Liga | Druha Liga; Druha Liga - Group A; Druha Liga - Group B |
| australia | A-League; A-League Men | Australian Championship; Capital Territory NPL; ACT Premier League; New South Wales NPL; Northern NSW NPL; Northern Territory Premier League; Queensland NPL; South Australia NPL; Tasmania NPL; Victoria NPL; Western Australia NPL | Capital Territory NPL 2; New South Wales NPL 2; NNSW League 1; Queensland Premier League; South Australia State League 1; Tasmania Northern Championship; Tasmania Southern Championship; Victoria NPL 2; Victoria Premier League 1; Western Australia State League 1 |
| china | Super League | League One | League Two |
| south korea | K League 1 | K League 2 | K3 League |
| japan | J1 League | J2 League | J3 League; J2/J3 League |
| argentina | Liga Profesional Argentina; Primera Division | Primera Nacional | Primera B Metropolitana; Torneo Federal A; Federal A |
| bolivia | Primera Division | Nacional B; Copa Simon Bolivar | Primera A |
| brazil | Serie A | Serie B | Serie C |
| chile | Primera Division | Primera B | Segunda Division |
| colombia | Primera A | Primera B | Primera C |
| mexico | Liga MX | Liga de Expansion MX | Liga Premier Serie A; Liga Premier Serie B; Primera Premier |
| paraguay | Division Profesional; Division Profesional - Apertura; Division Profesional - Clausura | Division Intermedia | Primera B; Primera B Nacional |
| peru | Primera Division; Liga 1 | Segunda Division; Liga 2 | Liga 3 |
| usa | Major League Soccer; MLS | USL Championship | USL League One; NISA; MLS Next Pro |
| uruguay | Primera Division; Primera Division - Apertura; Primera Division - Clausura | Segunda Division | Primera Division Amateur; Primera Divisional C |
| venezuela | Primera Division | Segunda Division | Tercera Division |
