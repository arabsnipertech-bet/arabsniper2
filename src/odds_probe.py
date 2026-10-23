"""Read-only, bounded comparison of API-Football odds query routes.

No keys, account details or raw API responses are written to reports.
"""
import importlib.util
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from audit_storage import read_store


def engine():
    spec = importlib.util.spec_from_file_location('probe_engine', Path(__file__).with_name('3appdays.py'))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def inventory(payload, fid):
    result = {}
    for row in (payload or {}).get('response', []):
        if str(row.get('fixture', {}).get('id')) != str(fid):
            continue
        for book in row.get('bookmakers', []):
            result.setdefault(str(book['id']), {'name': book['name'], 'markets': {}})['markets'].update({
                str(b['id']): sorted(str(v.get('value')) for v in b.get('values', []))
                for b in book.get('bets', [])})
    return result


def main():
    e = engine()
    c = e.APIClient(os.environ['API_SPORTS_KEY'])
    status = c.get('status', {}, use_cache=False)
    q = (status or {}).get('response', {}).get('requests', {})
    if not isinstance(q.get('limit_day'), int) or q['limit_day']-q['current'] < max(3750, q['limit_day']*.1)+1200:
        raise RuntimeError('Insufficient verified quota for probe')
    catalog = c.get('odds/bookmakers', {}, use_cache=False)
    bets = c.get('odds/bets', {}, use_cache=False)
    if not catalog or not bets:
        raise RuntimeError('Catalog unavailable')
    book_ids = [b['id'] for b in catalog['response']]
    bet_ids = [b['id'] for b in bets['response'] if b['id'] in (1,5) or
               ('first half' in str(b.get('name') or '').lower() and ('goals' in str(b.get('name') or '').lower() or 'score' in str(b.get('name') or '').lower()))]
    now = datetime.now(timezone.utc)
    matches = []
    for day in (1,2):
        data = read_store(Path(f'data/arab_quote_day{day}.json'))
        matches.extend(m['fixture'] for m in data['matches']
                       if datetime.fromisoformat(m['fixture']['kickoff']) > now)
    # Distinct leagues, mix low/high bookmaker coverage, deterministic sample.
    sample = []
    seen = set()
    for f in sorted(matches, key=lambda f:f['kickoff']):
        if f['league_id'] not in seen:
            sample.append(f); seen.add(f['league_id'])
        if len(sample) == 8:
            break
    report = {'generated_at': now.isoformat(), 'bookmakers_catalog': catalog['response'],
              'target_bets': [b for b in bets['response'] if b['id'] in bet_ids], 'fixtures': []}
    for f in sample:
        fid = f['fixture_id']
        base = c.get('odds', {'fixture':fid}, use_cache=False)
        initial = inventory(base, fid)
        extracted, audit = e.extract_bookmaker_markets_with_audit(base or {})
        row = {'fixture_id':fid,'match':f['match'],'league':f['league'], 'baseline':initial,
               'usable':{k:len(e.complete_market_books(extracted,k)) for k in e.MARKET_SELECTIONS},
               'rejected':{k:dict(Counter(r['reason'] for r in a.get('rejected',[]))) for k,a in audit.items() if isinstance(a,dict)},
               'queries':[]}
        queries = [('bookmaker', {'fixture':fid,'bookmaker':b}) for b in book_ids]
        queries += [('bet', {'fixture':fid,'bet':b}) for b in bet_ids]
        queries += [('league_date', {'league':f['league_id'],'season':f['season'],'date':f['date']})]
        for route, params in queries:
            if c.request_count >= 1100:
                break
            payload = c.get('odds',params,use_cache=False)
            current = inventory(payload,fid)
            new_books = sorted(set(current)-set(initial))
            new_markets = [(b,m) for b,v in current.items() for m in v['markets'] if m not in initial.get(b,{}).get('markets',{})]
            row['queries'].append({'route':route,'filter':params.get(route), 'ok':payload is not None,
                                   'books':len(current),'new_books':new_books,'new_book_markets':new_markets})
        report['fixtures'].append(row)
        print(json.dumps({'match':f['match'],'baseline_books':len(initial),'usable':row['usable'],
                          'queries':len(row['queries']),'queries_adding_books':sum(bool(x['new_books']) for x in row['queries']),
                          'queries_adding_markets':sum(bool(x['new_book_markets']) for x in row['queries'])}), flush=True)
    report['requests'] = c.request_count
    report['pagination_incomplete'] = c.odds_pagination_incomplete
    Path('probe-output').mkdir(exist_ok=True)
    Path('probe-output/odds-probe.json').write_text(json.dumps(report,indent=2))
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as out:
            out.write('## API-Football odds probe\n\n'+f'Calls: {c.request_count}; catalog: {len(book_ids)} bookmakers.\n\n')
            out.write('|Match|Baseline books|Queries adding books|Queries adding markets|\n|---|---:|---:|---:|\n')
            for r in report['fixtures']:
                out.write(f"|{r['match']}|{len(r['baseline'])}|{sum(bool(x['new_books']) for x in r['queries'])}|{sum(bool(x['new_book_markets']) for x in r['queries'])}|\n")
    print('PROBE_REPORT '+json.dumps(report,separators=(',',':')),flush=True)

if __name__ == '__main__':
    main()
