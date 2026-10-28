"""Strict senior competition scope, separate from statistical confidence.

Exact country/name aliases admit tiers 1-3, including their regional groups.
Unknown competitions and tiers 4+ fail closed; country and cup scope is unchanged.
Domestic cups are admitted only where explicitly requested. Romania is allowed.
"""
from collections import Counter
import re
import unicodedata

SCOPE_ID = 'gold-senior-whitelist-v2.1'


def norm(value):
    text = ''.join(c for c in unicodedata.normalize('NFKD', str(value or '').lower())
                   if not unicodedata.combining(c))
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', text).split())


TOP = {
    'austria': ('Bundesliga',), 'azerbaijan': ('Premyer Liqa', 'Premier League'),
    'bulgaria': ('First League',), 'croatia': ('HNL', '1. HNL'),
    'denmark': ('Superliga',), 'finland': ('Veikkausliiga',), 'france': ('Ligue 1',),
    'germany': ('Bundesliga',), 'england': ('Premier League',),
    'ireland': ('Premier Division',), 'northern ireland': ('Premiership',),
    'italy': ('Serie A',), 'kazakhstan': ('Premier League',), 'norway': ('Eliteserien',),
    'netherlands': ('Eredivisie',), 'poland': ('Ekstraklasa',), 'portugal': ('Primeira Liga',),
    'czech republic': ('Czech Liga', 'First League'), 'romania': ('Liga I', 'Liga 1', 'Superliga'),
    'scotland': ('Premiership',), 'serbia': ('Super Liga',), 'slovakia': ('Super Liga',),
    'spain': ('La Liga', 'Primera Division'), 'sweden': ('Allsvenskan',),
    'switzerland': ('Super League',), 'turkey': ('Super Lig',), 'ukraine': ('Premier League',),
    'australia': ('A-League', 'A-League Men'), 'china': ('Super League',),
    'south korea': ('K League 1',), 'japan': ('J1 League',),
    'argentina': ('Liga Profesional Argentina', 'Primera Division'),
    'bolivia': ('Primera Division',), 'brazil': ('Serie A',), 'chile': ('Primera Division',),
    'colombia': ('Primera A',), 'mexico': ('Liga MX',),
    'paraguay': ('Division Profesional', 'Division Profesional - Apertura', 'Division Profesional - Clausura'),
    'peru': ('Primera Division', 'Liga 1'), 'usa': ('Major League Soccer', 'MLS'),
    'uruguay': ('Primera Division', 'Primera Division - Apertura', 'Primera Division - Clausura'),
    'venezuela': ('Primera Division',),
}
# Country-scoped aliases, never a substring/prefix rule. Provider spellings were
# checked against https://www.api-football.com/coverage on 2026-10-27.
# A permitted competition still needs provider fixtures: this is not a claim of
# API coverage, available odds, or eligibility for a Gold signal.
SECOND = {
    'austria': ('2. Liga',), 'azerbaijan': ('Birinci Dasta', 'I Liqa', 'First Division'),
    'bulgaria': ('Second League',), 'croatia': ('First NL', '1. NL', '2. HNL'),
    'denmark': ('1. Division', '1st Division'), 'finland': ('Ykkosliiga',),
    'france': ('Ligue 2',), 'germany': ('2. Bundesliga',), 'england': ('Championship',),
    'ireland': ('First Division',), 'northern ireland': ('Championship',),
    'italy': ('Serie B',), 'kazakhstan': ('1. Division', 'First Division'),
    'norway': ('1. Division', 'OBOS-ligaen'), 'netherlands': ('Eerste Divisie',),
    'poland': ('I Liga',), 'portugal': ('Segunda Liga', 'Liga Portugal 2'),
    'czech republic': ('FNL', '2. Liga'), 'romania': ('Liga II', 'Liga 2'),
    'scotland': ('Championship',), 'serbia': ('Prva Liga',), 'slovakia': ('2. Liga',),
    'spain': ('Segunda Division', 'La Liga 2'), 'sweden': ('Superettan',),
    'switzerland': ('Challenge League',), 'turkey': ('1. Lig',), 'ukraine': ('Persha Liga',),
    # NPL state leagues remain the second tier alongside the national tournament.
    'australia': ('Australian Championship', 'Capital Territory NPL', 'ACT Premier League',
                  'New South Wales NPL', 'Northern NSW NPL', 'Northern Territory Premier League',
                  'Queensland NPL', 'South Australia NPL', 'Tasmania NPL', 'Victoria NPL',
                  'Western Australia NPL'), 'china': ('League One',),
    'south korea': ('K League 2',), 'japan': ('J2 League',),
    'argentina': ('Primera Nacional',), 'bolivia': ('Nacional B', 'Copa Simon Bolivar'),
    'brazil': ('Serie B',), 'chile': ('Primera B',), 'colombia': ('Primera B',),
    'mexico': ('Liga de Expansion MX',), 'paraguay': ('Division Intermedia',),
    'peru': ('Segunda Division', 'Liga 2'), 'usa': ('USL Championship',),
    'uruguay': ('Segunda Division',), 'venezuela': ('Segunda Division',),
}
THIRD = {
    'austria': ('Regionalliga - Mitte', 'Regionalliga - Ost', 'Regionalliga - West',
                'Regionalliga - Salzburg', 'Regionalliga - Tirol',
                'Regionalliga - North', 'Regionalliga - South'),
    'azerbaijan': ('II Liqa', 'Second League'),
    'bulgaria': tuple('Third League - '+region for region in
                      ('Northeast', 'Northwest', 'Southeast', 'Southwest')),
    'croatia': ('Second NL', '2. NL'),
    'denmark': ('2. Division', '2nd Division', '2nd Division - Group 1', '2nd Division - Group 2'),
    'finland': ('Ykkonen',), 'france': ('National', 'Championnat National', 'Ligue 3'),
    'germany': ('3. Liga',), 'england': ('League One',),
    'ireland': ('FAI National League', 'National League'),
    'northern ireland': ('Premier Intermediate League',),
    'italy': ('Serie C', 'Serie C - Girone A', 'Serie C - Girone B', 'Serie C - Girone C',
              'Serie C - Promotion - Play-offs', 'Serie C - Relegation - Play-offs'),
    'kazakhstan': ('2. Division', 'Second Division', 'Second League'),
    'norway': ('2. Division', '2. Division - Group 1', '2. Division - Group 2', '2. Division - Play-offs'),
    'netherlands': ('Tweede Divisie',), 'poland': ('II Liga', 'II Liga - East', 'II Liga - West'),
    'portugal': ('Liga 3',),
    'czech republic': ('3. liga - CFL A', '3. liga - CFL B', '3. liga - MSFL', '3. liga - Promotion Play-off'),
    'romania': ('Liga III', 'Liga 3', 'Liga III - Play-offs') + tuple('Liga III - Serie '+str(i) for i in range(1, 11)),
    'scotland': ('League One',),
    'serbia': tuple('Srpska Liga - '+region for region in ('Belgrade', 'East', 'Vojvodina', 'West')),
    'slovakia': ('3. Liga', '3. liga - Bratislava', '3. liga - Center', '3. liga - East',
                 '3. liga - West', '3. liga - Play-offs'),
    'spain': ('Primera Division RFEF', 'Primera Federacion', 'Primera Division RFEF - Play Offs')
             + tuple('Primera Division RFEF - Group '+str(i) for i in range(1, 6)),
    'sweden': ('Ettan', 'Ettan - Norra', 'Ettan - Sodra', 'Ettan - Relegation Round'),
    'switzerland': ('1. Liga Promotion', 'Promotion League'),
    'turkey': ('2. Lig', '2. Lig - White', '2. Lig - Red'),
    'ukraine': ('Druha Liga', 'Druha Liga - Group A', 'Druha Liga - Group B'),
    'australia': ('Capital Territory NPL 2', 'New South Wales NPL 2', 'NNSW League 1',
                  'Queensland Premier League', 'South Australia State League 1',
                  'Tasmania Northern Championship', 'Tasmania Southern Championship',
                  'Victoria NPL 2', 'Victoria Premier League 1', 'Western Australia State League 1'),
    'china': ('League Two',), 'south korea': ('K3 League',), 'japan': ('J3 League', 'J2/J3 League'),
    'argentina': ('Primera B Metropolitana', 'Torneo Federal A', 'Federal A'),
    'bolivia': ('Primera A',), 'brazil': ('Serie C',), 'chile': ('Segunda Division',),
    'colombia': ('Primera C',),
    'mexico': ('Liga Premier Serie A', 'Liga Premier Serie B', 'Primera Premier'),
    'paraguay': ('Primera B', 'Primera B Nacional'), 'peru': ('Liga 3',),
    'usa': ('USL League One', 'NISA', 'MLS Next Pro'),
    'uruguay': ('Primera Division Amateur', 'Primera Divisional C'),
    'venezuela': ('Tercera Division',),
}

CUPS = {
    'austria': ('Cup', 'OFB Cup'), 'azerbaijan': ('Cup',), 'bulgaria': ('Cup', 'Super Cup'),
    'croatia': ('Cup', 'Super Cup'), 'denmark': ('DBU Pokalen',),
    'finland': ('Suomen Cup', 'League Cup'), 'france': ('Coupe de France', 'Trophee des Champions'),
    'germany': ('DFB Pokal', 'Super Cup'), 'england': ('FA Cup', 'League Cup', 'Community Shield'),
    'ireland': ('FAI Cup', 'League Cup', 'Super Cup'),
    'northern ireland': ('Irish Cup', 'League Cup', 'Charity Shield'),
    'italy': ('Coppa Italia', 'Super Cup', 'Supercoppa Italiana'),
    'kazakhstan': ('Cup', 'League Cup', 'Super Cup'), 'norway': ('NM Cupen', 'Super Cup'),
    'netherlands': ('KNVB Beker', 'Super Cup'), 'poland': ('Cup', 'Super Cup'),
    'portugal': ('Taca de Portugal', 'Taca da Liga', 'Super Cup'),
    'czech republic': ('Cup',), 'romania': ('Cupa Romaniei', 'Supercupa'),
    'scotland': ('FA Cup', 'Scottish Cup', 'League Cup'), 'serbia': ('Cup',),
    'slovakia': ('Cup',), 'spain': ('Copa del Rey', 'Super Cup'),
    'sweden': ('Svenska Cupen',), 'switzerland': ('Schweizer Cup',),
    'turkey': ('Cup', 'Super Cup'), 'ukraine': ('Cup', 'Super Cup'),
    'china': ('FA Cup', 'Super Cup'), 'japan': ('Emperor Cup', 'Emperors Cup', 'J-League Cup', 'Super Cup'),
}
INTERNATIONAL = {'UEFA Champions League', 'UEFA Europa League', 'UEFA Conference League',
    'UEFA Europa Conference League', 'AFC Champions League', 'AFC Champions League Elite',
    'CONMEBOL Libertadores', 'Copa Libertadores', 'CONMEBOL Sudamericana', 'Copa Sudamericana',
    'CONCACAF Champions League', 'CONCACAF Champions Cup'}
COUNTRY_ALIASES = {'united states': 'usa', 'united states of america': 'usa',
    'the netherlands': 'netherlands', 'czechia': 'czech republic', 'korea republic': 'south korea',
    'republic of ireland': 'ireland', 'turkiye': 'turkey'}
FORBIDDEN = re.compile(r'\b(?:u\s*\d{1,2}|under\s*\d*|youth|junior|juniors|reserve|reserves|academy|women|womens|femenina|feminino|femminile|frauen)\b')
TEAM_FORBIDDEN = re.compile(r'\b(?:u\s*\d{1,2}|under\s*\d+|youth|reserve|reserves|academy|jong)\b|\b(?:ii|iii|b)\s*$')
# Exact senior identities only. A whitelisted league can also contain genuine
# reserve teams (e.g. Dortmund II), so league membership cannot waive all suffixes.
SENIOR_ROMAN_NAMES = frozenset(('willem ii',))
TOP_NAMES = {k: {norm(n) for n in names} for k, names in TOP.items()}
LOWER_NAMES = {level: {k: {norm(n) for n in names} for k, names in mapping.items()}
               for level, mapping in ((2, SECOND), (3, THIRD))}
CUP_NAMES = {k: {norm(n) for n in names} for k, names in CUPS.items()}
INTERNATIONAL_NAMES = {norm(n) for n in INTERNATIONAL}


def profile(league):
    country, name = norm(league.get('country')), norm(league.get('name'))
    country = COUNTRY_ALIASES.get(country, country)
    allowed, tier, reason = False, 'blocked', 'competition_not_whitelisted'
    if FORBIDDEN.search(name): reason = 'youth_reserve_or_nonlisted_senior_category'
    elif name in INTERNATIONAL_NAMES and country in ('world', 'europe', 'asia', 'international', 'north america', 'south america'):
        allowed, tier, reason = True, 'international_cup', 'whitelisted_international_cup'
    elif name in TOP_NAMES.get(country, set()):
        allowed, tier, reason = True, 'senior_top_division', 'whitelisted_top_division'
    elif country in TOP_NAMES and name in LOWER_NAMES[2].get(country, set()):
        allowed, tier, reason = True, 'senior_division_2', 'whitelisted_second_division'
    elif country in TOP_NAMES and name in LOWER_NAMES[3].get(country, set()):
        allowed, tier, reason = True, 'senior_division_3', 'whitelisted_third_division'
    elif name in CUP_NAMES.get(country, set()):
        allowed, tier, reason = True, 'national_cup', 'whitelisted_national_cup'
    return {'allowed': allowed, 'score': 84 if allowed else 0, 'tier': tier, 'reason': reason,
            'policy_id': SCOPE_ID}


def fixture_profile(raw):
    result = profile(raw.get('league') or {})
    teams = raw.get('teams') or {}
    def reserve(side):
        name = norm((teams.get(side) or {}).get('name'))
        return bool(TEAM_FORBIDDEN.search(name)) and not (result['allowed'] and name in SENIOR_ROMAN_NAMES)
    if any(reserve(side) for side in ('home', 'away')):
        result.update(allowed=False, score=0, tier='blocked', reason='youth_or_reserve_fixture')
    return result


def fixture_allowed(raw, excluded=()):
    return (raw.get('fixture') or {}).get('id') is not None and fixture_profile(raw)['allowed']


def output_profile(item):
    meta = item.get('fixture') or {}
    return fixture_profile({'league': {'name': meta.get('league'), 'country': meta.get('country')},
        'teams': {s: {'name': meta.get(s)} for s in ('home', 'away')}})


def output_allowed(item, excluded=()):
    return (item.get('fixture') or {}).get('fixture_id') is not None and output_profile(item)['allowed']


def enforce_day(day):
    kept, removed = [], Counter()
    previous = day.get('gold_scope') or {}
    for item in day.get('matches', []):
        decision = output_profile(item)
        if output_allowed(item):
            item['gold_scope'] = decision
            kept.append(item)
        else: removed[decision['reason']] += 1
    day['matches'] = kept
    if previous.get('policy_id') == SCOPE_ID:
        removed.update(previous.get('exclusion_reasons') or {})
    day['gold_scope'] = {'policy_id': SCOPE_ID, 'allowed_fixtures': len(kept),
        'excluded_fixtures': sum(removed.values()), 'exclusion_reasons': dict(removed),
        'romania_allowed': True, 'scope': 'senior_first_three_divisions_and_explicit_cups'}
    coverage = day.setdefault('universal_coverage', {})
    if coverage.get('competition_policy') != SCOPE_ID:
        # A saved schedule cannot establish the size of a fresh provider list.
        coverage.update(previous_available_fixture_ids=coverage.get('available_fixture_ids'),
                        available_fixture_ids=None, coverage_basis='saved_schedule')
    coverage.update(displayed_fixtures=len(kept), competition_policy=SCOPE_ID,
                    excluded_from_saved_schedule=sum(removed.values()))
    return sum(removed.values())

