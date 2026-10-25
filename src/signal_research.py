"""Append-only observations for fixed exploratory signals; no probability bonuses."""
from collections import defaultdict
from copy import deepcopy
from datetime import date
from math import sqrt
from statistics import mean
from zoneinfo import ZoneInfo
from multimarket_model import stamp, number

PROTOCOL_ID = 'september-signals-v1'
VALIDATION_FROM = '2026-10-20'
COLLECTION = 'signal_observations'
CONFIG = {'home_initial_min': 1.50, 'home_initial_max': 1.70,
          'movement_pct': 2, 'minimum_paired_books': 2,
          'max_quote_age_hours': 6, 'history_matches': 8,
          'goal_mean_min': 1.5, 'drought_matches': 2,
          'probability_bonus': 0}
TZ = ZoneInfo('Europe/Rome')


def record_key(record):
    return f"{record['protocol_id']}::{record['fixture']['fixture_id']}::{record['captured_at']}"


def favourite(quotes, as_of, kickoff):
    now, start = stamp(as_of), stamp(kickoff)
    pairs = []
    for q in quotes.get('1x2_ft', []):
        movement = q.get('movement', {})
        first = stamp(movement.get('first_observed_at'))
        observed = stamp(q.get('observed_at'))
        updated = stamp(q.get('provider_updated_at'))
        initial, current = movement.get('first_odds', {}), q.get('odds', {})
        if (not all((now, start, first, observed, updated)) or
                not first < observed <= now < start or updated > observed or
                (now-updated).total_seconds() > 6*3600 or not q.get('bookmaker_id') or
                q.get('bet_id') != 1 or q.get('line') is not None or
                not all(number(x.get(s)) is not None and number(x[s]) > 1
                        for x in (initial, current) for s in ('1','x','2'))):
            continue
        pairs.append({'bookmaker_id': str(q['bookmaker_id']),
                      'bookmaker': q.get('bookmaker'),
                      'first_observed_at': first.isoformat(),
                      'observed_at': observed.isoformat(),
                      'provider_updated_at': updated.isoformat(),
                      'first_odds': deepcopy(initial), 'odds': deepcopy(current)})
    # Duplicated operators must not inflate the sample or change the mean.
    ids = [p['bookmaker_id'] for p in pairs]
    if len(ids) != len(set(ids)):
        return {'status': 'unavailable', 'reason': 'duplicate_operator', 'pairs': pairs}
    if len(pairs) < CONFIG['minimum_paired_books']:
        return {'status': 'unavailable', 'reason': 'insufficient_paired_books', 'pairs': pairs}
    first = mean(float(p['first_odds']['1']) for p in pairs)
    current = mean(float(p['odds']['1']) for p in pairs)
    delta = 100*(current/first-1)
    in_band = CONFIG['home_initial_min'] <= first <= CONFIG['home_initial_max']
    group = ('drop' if delta <= -2+1e-9 else 'rise' if delta >= 2-1e-9 else 'stable') if in_band else 'outside_band'
    return {'status': 'available', 'group': group, 'in_initial_band': in_band,
            'first_home_mean': first, 'current_home_mean': current,
            'change_pct': delta, 'pairs': sorted(pairs, key=lambda p:p['bookmaker_id']),
            'opening_certified': False, 'closing_certified': False}


def team_history(profile, league_id, as_of, kickoff):
    now, start = stamp(as_of), stamp(kickoff)
    if not now or not start or league_id is None:
        return {'status': 'unavailable', 'reason': 'missing_cutoff_or_league'}
    cutoff = min(now, start).astimezone(TZ).date()
    eligible = []
    for r in profile.get('last_matches', []):
        try:
            day = date.fromisoformat(str(r.get('date')))
        except ValueError:
            continue
        if day < cutoff and str(r.get('league_id')) == str(league_id):
            eligible.append(r)
    eligible.sort(key=lambda r:(r['date'],str(r.get('fixture_id'))), reverse=True)
    seen = set(); latest = []
    for r in eligible:
        fid = r.get('fixture_id')
        if fid is None or str(fid) in seen:
            continue
        seen.add(str(fid)); latest.append(r)
        if len(latest) == 8: break
    if len(latest) < 8:
        return {'status': 'unavailable', 'reason': 'fewer_than_eight_prior_league_games', 'n': len(latest)}
    # Do not use legacy zero-filled scores or silently skip missing recent scores.
    if any(not isinstance(r.get('research_ft'), dict) or
           any(number(r['research_ft'].get(k)) is None or
               number(r['research_ft'][k]) < 0 for k in ('scored','conceded')) for r in latest):
        return {'status': 'unavailable', 'reason': 'unverified_regulation_scores', 'n': 8}
    gf = [float(r['research_ft']['scored']) for r in latest]
    ga = [float(r['research_ft']['conceded']) for r in latest]
    return {'status': 'available', 'n': 8, 'mean_scored': mean(gf), 'mean_conceded': mean(ga),
            'two_without_scoring': gf[:2] == [0,0], 'two_without_conceding': ga[:2] == [0,0],
            'source_games': [{'fixture_id': r['fixture_id'], 'date': r['date'],
                             'league_id': r['league_id'], **r['research_ft']} for r in latest]}


def capture(store, output, as_of):
    now = stamp(as_of)
    if not now: return
    for day in output.get('days', []):
        for match in day.get('matches', []):
            meta = match.get('fixture', {}); start = stamp(meta.get('kickoff'))
            if not start or now >= start or meta.get('result') or meta.get('fixture_id') is None:
                continue
            assessment = match.get('multimarket', {})
            record = {'protocol_id': PROTOCOL_ID, 'captured_at': as_of,
                      'fixture': deepcopy(meta), 'config': deepcopy(CONFIG),
                      'model_id': assessment.get('model_id'),
                      'favourite': favourite(assessment.get('quotes', {}), as_of, meta['kickoff']),
                      'teams': {side: team_history(match.get('statistics', {}).get(side, {}),
                                meta.get('league_id'), as_of, meta['kickoff']) for side in ('home','away')},
                      'forecasts': deepcopy(assessment.get('markets', []))}
            store.setdefault(COLLECTION, {}).setdefault(record_key(record), record)


def event_results(result):
    if result.get('status') not in {'FT','AET','PEN'}: return {}
    def score(key):
        n = number(result.get(key))
        return n if n is not None and n >= 0 and n.is_integer() else None
    h,a,x,y = [score(k) for k in ('home_ft','away_ft','home_ht','away_ht')]
    ft = h+a if h is not None and a is not None else None
    pt = x+y if x is not None and y is not None else None
    st = ft-pt if ft is not None and pt is not None and h>=x and a>=y else None
    out = {s: (s == ('1' if h>a else '2' if a>h else 'X')) if ft is not None else None for s in ('1','X','2')}
    for line in (.5,1.5,2.5):
        out[f'over{line}_ht'] = pt>line if pt is not None else None
        out[f'under{line}_ht'] = pt<line if pt is not None else None
    out['gg_ht_yes'] = x>0 and y>0 if pt is not None else None
    out['gg_ht_no'] = not out['gg_ht_yes'] if pt is not None else None
    for line in (2.5,3.5): out[f'over{line}_ft'] = ft>line if ft is not None else None
    for p,s in ((.5,.5),(1.5,.5),(1.5,1.5)):
        out[f'over{p}_ht_over{s}_sh'] = pt>p and st>s if st is not None else None
    for side,scored,conceded,ht in [('home',h,a,x),('away',a,h,y)]:
        out[side+'_scored'] = scored>0 if scored is not None else None
        out[side+'_conceded'] = conceded>0 if conceded is not None else None
        out[side+'_scored_ht'] = ht>0 if ht is not None else None
    return out


def settle(store, results):
    for record in store.get(COLLECTION, {}).values():
        result = results.get(str(record['fixture']['fixture_id'])) or {}
        outcomes = event_results(result)
        if outcomes:
            record['settlement'] = {'result': deepcopy(result), 'outcomes': outcomes}


def frequency(values):
    n=len(values)
    if not n: return {'n': 0, 'wins': 0, 'hit_rate_pct': None, 'wilson_95_pct': None}
    wins=sum(values); p=wins/n; z=1.96; d=1+z*z/n
    c=(p+z*z/(2*n))/d; e=z*sqrt(p*(1-p)/n+z*z/(4*n*n))/d
    return {'n': n, 'wins': wins, 'hit_rate_pct': round(100*p,3),
            'wilson_95_pct': [round(100*(c-e),3),round(100*(c+e),3)]}


def build_report(store):
    by_fixture={}
    for r in store.get(COLLECTION, {}).values():
        now, start=stamp(r.get('captured_at')),stamp(r.get('fixture',{}).get('kickoff'))
        if r.get('protocol_id') != PROTOCOL_ID or not now or not start or now>=start:
            continue
        fid=str(r['fixture']['fixture_id'])
        # Only the first baseline and first evaluable price are used downstream.
        # Retaining every repeated observation inflated memory without changing results.
        compact={k:v for k,v in r.items() if k != 'forecasts'}
        pair=by_fixture.setdefault(fid, {'first':compact, 'usable':None})
        if now<stamp(pair['first']['captured_at']): pair['first']=compact
        if r['favourite']['status']=='available' and (pair['usable'] is None or
                now<stamp(pair['usable']['captured_at'])): pair['usable']=compact
    groups=defaultdict(list); coverage=defaultdict(int); fixtures=[]
    for fid, pair in by_fixture.items():
        first=pair['first']
        choice=pair['usable'] or first
        period=('retrospective' if choice.get('retrospective') else 'prospective' if
                stamp(choice['fixture']['kickoff']).astimezone(TZ).date().isoformat()>=VALIDATION_FROM else 'development')
        feature=choice['favourite']; group=feature.get('group','unavailable')
        coverage[(period,group)]+=1
        if feature['status']=='available':
            for market,y in choice.get('settlement',{}).get('outcomes',{}).items():
                if y is not None:
                    groups[(period,'favourite',group,market)].append(bool(y))
        teams={}
        for side in ('home','away'):
            # Freeze team baseline on first observation, including unavailable histories.
            t=first['teams'][side]; teams[side]=t
            if t['status']!='available': continue
            for kind,mean_key,flag,event in [('attack','mean_scored','two_without_scoring','scored'),
                                             ('defence','mean_conceded','two_without_conceding','conceded')]:
                if t[mean_key]<1.5: continue
                y=first.get('settlement',{}).get('outcomes',{}).get(side+'_'+event)
                if y is not None:
                    groups[(period,kind,'two_zeros' if t[flag] else 'no_two_zeros',event)].append(bool(y))
        fixtures.append({'fixture':deepcopy(choice['fixture']), 'captured_at':choice['captured_at'],
                         'period':period, 'favourite_group':group, 'favourite':deepcopy(feature),
                         'team_baseline_at':first['captured_at'], 'teams':deepcopy(teams),
                         'outcomes':deepcopy(choice.get('settlement',{}).get('outcomes',{}))})
    fixtures.sort(key=lambda r:(stamp(r['fixture']['kickoff']),str(r['fixture']['fixture_id'])))
    return {'protocol_id':PROTOCOL_ID,'config':deepcopy(CONFIG),'validation_from':VALIDATION_FROM,
            'note':'Observed frequencies, not calibrated probabilities. No automatic fitting or bonus. First evaluable paired-operator snapshot per fixture; first team baseline. Same-operator means are not the September archive market averages or closing quotes.',
            'coverage':[{'period':p,'group':g,'fixtures':n} for (p,g),n in sorted(coverage.items())],
            'groups':[dict(zip(('period','family','group','event'),key),**frequency(values)) for key,values in sorted(groups.items())],
            'fixtures':fixtures}
