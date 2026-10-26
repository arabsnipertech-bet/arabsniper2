"""Strict senior competition scope, separate from statistical confidence.

Exact country/name aliases fail closed for unknown, regional and lower leagues.
Domestic cups are admitted only where explicitly requested. Romania is allowed.
"""
from collections import Counter
import re
import unicodedata

SCOPE_ID = 'gold-senior-whitelist-v1'


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
TOP_NAMES = {k: {norm(n) for n in names} for k, names in TOP.items()}
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
    elif name in CUP_NAMES.get(country, set()):
        allowed, tier, reason = True, 'national_cup', 'whitelisted_national_cup'
    return {'allowed': allowed, 'score': 84 if allowed else 0, 'tier': tier, 'reason': reason,
            'policy_id': SCOPE_ID}


def fixture_profile(raw):
    result = profile(raw.get('league') or {})
    teams = raw.get('teams') or {}
    if any(TEAM_FORBIDDEN.search(norm((teams.get(side) or {}).get('name'))) for side in ('home', 'away')):
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
        'romania_allowed': True, 'scope': 'senior_top_divisions_and_explicit_cups'}
    coverage = day.setdefault('universal_coverage', {})
    if coverage.get('competition_policy') != SCOPE_ID:
        # A saved schedule cannot establish the size of a fresh provider list.
        coverage.update(previous_available_fixture_ids=coverage.get('available_fixture_ids'),
                        available_fixture_ids=None, coverage_basis='saved_schedule')
    coverage.update(displayed_fixtures=len(kept), competition_policy=SCOPE_ID,
                    excluded_from_saved_schedule=sum(removed.values()))
    return sum(removed.values())
