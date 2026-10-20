"""User-agreed selection policy, kept separate from the probability engine."""
import re
from datetime import datetime
import unicodedata

def norm(value):
    return ''.join(c for c in unicodedata.normalize('NFKD', str(value or '').lower()) if not unicodedata.combining(c)).strip()

# Ordered top to third tier. Exact aliases prevent a fourth tier passing as a third.
LEAGUES = {
 'italy': [('serie a',), ('serie b',), ('serie c', 'serie c - girone a', 'serie c - girone b', 'serie c - girone c')],
 'england': [('premier league',), ('championship',), ('league one',)],
 'spain': [('la liga', 'primera division'), ('segunda division', 'la liga 2'), ('primera division rfef', 'primera federacion', 'primera rfef - group 1', 'primera rfef - group 2')],
 'germany': [('bundesliga',), ('2. bundesliga', '2 bundesliga'), ('3. liga', '3 liga')],
 'france': [('ligue 1',), ('ligue 2',), ('national', 'championnat national')],
 'portugal': [('primeira liga',), ('segunda liga', 'liga portugal 2'), ('liga 3',)],
 'belgium': [('jupiler pro league', 'first division a'), ('challenger pro league', 'second division'), ('first amateur division', 'national division 1', 'division 1 amateur')],
 'netherlands': [('eredivisie',), ('eerste divisie',), ('tweede divisie',)],
 'sweden': [('allsvenskan',), ('superettan',), ('ettan - norra', 'ettan - sodra', 'ettan')],
 'denmark': [('superliga',), ('1st division', '1. division'), ('2nd division', '2. division')],
 'norway': [('eliteserien',), ('1. division', 'obos-ligaen'), ('2. division - group 1', '2. division - group 2', '2. division')],
 'finland': [('veikkausliiga',), ('ykkosliiga',), ('ykkonen',)],
 'egypt': [('premier league',), ('second league', 'second league - group a', 'second league - group b', 'second league - group c')],
 'japan': [('j1 league',), ('j2 league',), ('j3 league',)],
 'china': [('super league',), ('league one',), ('league two',)],
 'australia': [('a-league',), ('australian championship', 'australian premier league'), ('new south wales npl', 'victoria npl', 'queensland npl', 'western australia npl', 'south australia npl', 'tasmania npl', 'northern nsw npl', 'act premier league')],
 'usa': [('major league soccer', 'mls'), ('usl championship',), ('usl league one', 'nisa')],
 'canada': [('canadian premier league',), ('canadian soccer league',), ('league1 ontario', 'league1 british columbia', 'ligue1 quebec')],
 'mexico': [('liga mx',), ('liga de expansion mx',), ('liga premier serie a', 'liga premier serie b')],
 'brazil': [('serie a',), ('serie b',), ('serie c',)],
 'argentina': [('liga profesional argentina', 'primera division'), ('primera nacional',), ('primera b metropolitana', 'torneo federal a', 'federal a')],
 'chile': [('primera division',), ('primera b',), ('segunda division',)],
 'colombia': [('primera a',), ('primera b',), ('primera c',)],
 'uruguay': [('primera division - apertura', 'primera division - clausura', 'primera division'), ('segunda division',), ('primera division amateur',)],
 'paraguay': [('division profesional - apertura', 'division profesional - clausura', 'division profesional'), ('division intermedia',), ('primera b', 'primera b nacional')],
 'peru': [('primera division', 'liga 1'), ('segunda division', 'liga 2'), ('liga 3',)],
 'ecuador': [('liga pro', 'serie a'), ('serie b',), ('segunda categoria',)],
 'bolivia': [('primera division',), ('nacional b', 'copa simon bolivar'), ('primera a',)],
 'venezuela': [('primera division',), ('segunda division',), ('tercera division',)],
 'costa rica': [('primera division',), ('liga de ascenso',), ('liga de ascenso - apertura', 'liga de ascenso - clausura')],
 'honduras': [('liga nacional',), ('liga de ascenso',), ('liga mayor',)],
 'guatemala': [('liga nacional',), ('primera division',), ('segunda division',)],
 'el salvador': [('primera division',), ('segunda division',), ('tercera division',)],
 'panama': [('liga panamena de futbol',), ('liga prom',), ('liga nacional de ascenso',)],
 'nicaragua': [('primera division',), ('segunda division',), ('tercera division',)],
 'jamaica': [('premier league',), ('championship',), ('major league',)],
 'dominican republic': [('liga mayor', 'liga dominicana de futbol'), ('liga de expansion',), ('tercera division',)],
 'haiti': [('ligue haitienne',), ('division 2',), ('division 3',)],
 'trinidad and tobago': [('pro league',), ('super league',), ('premier division',)],
 'belize': [('premier league',), ('second division',), ('third division',)],
 'guyana': [('elite league',), ('division 1',), ('division 2',)],
 'suriname': [('eerste divisie',), ('tweede divisie',), ('derde divisie',)],
}
COUNTRY_ALIASES = {'united states': 'usa', 'united states of america': 'usa', 'the netherlands': 'netherlands'}
INTERNATIONAL = {'uefa champions league', 'uefa europa league', 'uefa europa conference league', 'uefa conference league',
 'conmebol libertadores', 'copa libertadores', 'conmebol sudamericana', 'copa sudamericana',
 'concacaf champions league', 'concacaf champions cup', 'afc champions league', 'afc champions league elite', 'caf champions league'}
CUPS = {
 'italy': {'coppa italia'}, 'england': {'fa cup'}, 'spain': {'copa del rey'}, 'germany': {'dfb pokal', 'dfb-pokal'},
 'france': {'coupe de france'}, 'portugal': {'taca de portugal'}, 'belgium': {'cup', 'belgian cup'},
 'netherlands': {'knvb beker'}, 'sweden': {'svenska cupen'}, 'denmark': {'dbu pokalen'},
 'norway': {'nm cupen'}, 'finland': {'suomen cup'}, 'egypt': {'cup', 'egypt cup'},
 'australia': {'australia cup'}, 'japan': {'emperor cup', 'emperors cup'}, 'china': {'fa cup'},
 'usa': {'us open cup'}, 'canada': {'canadian championship'}, 'mexico': {'copa mx'},
 'brazil': {'copa do brasil'}, 'argentina': {'copa argentina'}, 'chile': {'copa chile'},
 'colombia': {'copa colombia'}, 'uruguay': {'copa uruguay'}, 'paraguay': {'copa paraguay'},
 'ecuador': {'copa ecuador'}, 'venezuela': {'copa venezuela'},
}

def competition(league):
    country = COUNTRY_ALIASES.get(norm(league.get('country')), norm(league.get('country')))
    name = norm(league.get('name'))
    blocked = bool(re.search(r'\b(u1[5-9]|u2[013]|under|women|femenina|feminino|frauen|reserve|reserves|youth|academy|femminile)\b', name))
    if blocked:
        return {'allowed': False, 'score': 10, 'tier': 'blocked', 'reason': 'youth_reserve_or_women'}
    if name in INTERNATIONAL:
        return {'allowed': True, 'score': 88, 'tier': 'international_major', 'reason': 'user_major_cup'}
    if name in CUPS.get(country, set()) or norm(league.get('type')) == 'cup':
        return {'allowed': False, 'score': 0, 'tier': 'blocked', 'reason': 'national_cup_excluded'}
    for level, aliases in enumerate(LEAGUES.get(country, []), 1):
        if name in aliases:
            return {'allowed': True, 'score': {1:84,2:72,3:64}[level], 'tier': f'user_division_{level}', 'reason': 'user_allowlist'}
    return {'allowed': False, 'score': 0, 'tier': 'blocked',
            'reason': 'unmapped_competition_review' if country in LEAGUES else 'country_not_allowed'}

def drought(rows):
    """Conservative lower bound from scored halves. Never fabricate goal minutes."""
    minutes = 0; complete = False
    for row in sorted(rows, key=lambda r: str(r.get('date', '')), reverse=True):
        if row.get('halftime_available') is False or row.get('team_goals_ft') is None or row.get('team_goals_ht') is None:
            break
        ft, ht = row['team_goals_ft'], row['team_goals_ht']
        if ft == 0:
            minutes += 90
        elif ft == ht:
            minutes += 45; complete = True; break
        else:
            complete = True; break
    return {'minutes_lower_bound': minutes, 'bonus_relative': min(minutes//45, 4)*.01,
            'method': 'completed_halves_lower_bound', 'sample_censored': not complete}

def movements(market, side):
    start = (market.get('open') or {}).get('odds', {}).get(side)
    current = (market.get('current') or {}).get('odds', {}).get(side)
    points = [p.get('odds', {}).get(side) for p in market.get('history', [])]
    points = [p for p in points if isinstance(p, (float,int)) and p>1]
    signs = [1 if b>a else -1 for a,b in zip(points,points[1:]) if b!=a]
    drop = (start-current)/start*100 if isinstance(start,(int,float)) and start>1 and isinstance(current,(int,float)) and current>1 else None
    initial = (market.get('open') or {}).get('odds', {})
    final = (market.get('current') or {}).get('odds', {})
    favorite = lambda odds: min(((k,v) for k,v in odds.items() if isinstance(v,(int,float)) and v>1), key=lambda kv:kv[1], default=(None,None))[0]
    duration_hours = None
    history = market.get('history', [])
    if len(history)>1:
        try:
            duration_hours = max((datetime.fromisoformat(history[-1]['ts'])-datetime.fromisoformat(history[0]['ts'])).total_seconds()/3600,0)
        except (ValueError,KeyError,TypeError):
            pass
    consensus = ((market.get('selections',{}).get(side) or {}).get('bookmaker_consensus') or {}).get('ratio')
    return {'consensus_ratio':consensus,'observation_hours':duration_hours,
            'drop_pct_per_hour':round(drop/duration_hours,3) if drop is not None and duration_hours else None,
            'opening_observed_odd': start, 'current_odd': current, 'drop_pct': round(drop,3) if drop is not None else None,
            'direction_reversals': sum(a!=b for a,b in zip(signs,signs[1:])),
            'favorite_inversion': favorite(initial)!=favorite(final) if initial and final else False,
            'observation_count': len(points), 'opening_note': 'first observed quote, not certified bookmaker opening'}

