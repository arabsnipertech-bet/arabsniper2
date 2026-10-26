"""Public outcomes of actually published selections, separated by model version."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import json, math
from audit_storage import read_store, atomic_bytes
from homepage_preview import stamp

from lateral_matrix import MODEL_ID as MODEL
from statistics_first import VALIDATION_FROM
PREVIOUS_MODEL = 'statistics-first-v4.0'
LABELS = {'over15_ht':'Over 1,5 primo tempo', 'over25_ht':'Over 2,5 primo tempo',
          'over25_ft':'Over 2,5 finale', 'over35_ft':'Over 3,5 finale', 'gg_ht':'Goal primo tempo'}
ROME = ZoneInfo('Europe/Rome')

def project(report, day, model=MODEL):
    chosen = {}
    for row in report.get('markets', []):
        fixture = row.get('fixture') or {}
        kickoff, captured = stamp(fixture.get('kickoff')), stamp(row.get('captured_at'))
        published = row.get('published_selection') if model.startswith(('statistics-first-v4', 'lateral-matrix-v5')) else row.get('primary')
        if (row.get('model_id') != model or row.get('period') != 'prospective' or
            row.get('snapshot') != 'latest' or published is not True or
            row.get('market') not in LABELS or not kickoff or not captured or captured >= kickoff or
            kickoff.astimezone(ROME).date().isoformat() != day):
            continue
        fid = str(row['fixture_id'])
        key = (fid, row['market'])
        old = chosen.get(key)
        if old is None or captured > stamp(old['captured_at']): chosen[key] = row
    output = []
    for (fid, _), row in chosen.items():
        fixture = row['fixture']
        outcome = row.get('outcome')
        odd = row.get('odd')
        odd = float(odd) if isinstance(odd,(int,float)) and not isinstance(odd,bool) and math.isfinite(odd) and odd > 1 else None
        output.append({'fixture_id':fid, 'match':fixture.get('match',''), 'league':fixture.get('league',''),
                       'kickoff':fixture['kickoff'], 'captured_at':row['captured_at'],
                       'forecast_sha256':row.get('forecast_sha256'), 'label':LABELS[row['market']],
                       'market':row['market'], 'odd':odd, 'outcome':'won' if outcome is True else 'lost' if outcome is False else 'pending'})
    return sorted(output, key=lambda r:(r['kickoff'],r['fixture_id']))

def totals(rows):
    wins = sum(r['outcome']=='won' for r in rows)
    losses = sum(r['outcome']=='lost' for r in rows)
    return {'wins':wins,'losses':losses,'pending':len(rows)-wins-losses,'settled':wins+losses,
            'hit_rate':round(100*wins/(wins+losses),1) if wins+losses else None}

def build(reports, now=None):
    now = now or datetime.now(timezone.utc)
    yesterday = now.astimezone(ROME).date()-timedelta(days=1)
    start = yesterday-timedelta(days=29)
    by_day = {day:project(report,day) for day,report in reports.items() if start.isoformat()<=day<=yesterday.isoformat()}
    rows = by_day.get(yesterday.isoformat(),[])
    history = [r for day in sorted(by_day) for r in by_day[day]]
    previous = [r for day, report in reports.items() if start.isoformat() <= day <= yesterday.isoformat()
                for r in project(report, day, PREVIOUS_MODEL)]
    return {'schema_version':2,'generated_at':now.isoformat(),'date':yesterday.isoformat(),'model_id':MODEL,
            'validation_from':VALIDATION_FROM,
            'previous_model':{'model_id':PREVIOUS_MODEL, 'history':totals(previous)},
            'cohort':'Mercati principali con valore teorico stabile V5 nell’ultima analisi prima del calcio d’inizio. La stessa partita può avere più mercati; gli esiti sono correlati.',
            'available':yesterday.isoformat() in by_day,'daily':totals(rows),
            'history':dict(totals(history),from_date=start.isoformat(),to_date=yesterday.isoformat(),days_with_data=sum(bool(v) for v in by_day.values())),
            'examples':sorted([r for r in rows if r['outcome']=='won' and r['market'].startswith('over') and r['odd'] is not None], key=lambda r:(-r['odd'],r['kickoff'],r['fixture_id']))[:10],
            'all_results':rows}

def publish(root='.', now=None):
    root = Path(root)
    now = now or datetime.now(timezone.utc)
    end = now.astimezone(ROME).date()-timedelta(days=1)
    reports = {}
    for offset in range(30):
        day = (end-timedelta(days=offset)).isoformat()
        path = root/'auditarchive'/f'prematch_{day}_daily.json'
        if path.exists(): reports[day] = read_store(path)
    value = build(reports,now)
    atomic_bytes(root/'data/arab_quote_results_public.json',(json.dumps(value,ensure_ascii=False,separators=(',',':'))+'\n').encode())
    return value

if __name__=='__main__': publish()
