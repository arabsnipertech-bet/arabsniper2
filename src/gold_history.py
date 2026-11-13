"""Additive Gold display archive. Never writes a scan feed or runs the model.

A private two-day archive is attached ONLY to the published day1 projection.
Forecast objects freeze at kickoff; score overlays can still be corrected.
"""
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import subprocess
import urllib.request
from audit_storage import read_store, write_store

FILE = 'data/arab_quote_history.json'
ROME = ZoneInfo('Europe/Rome')
VERSION = 'gold-history-v1'
FINISHED = {'FT', 'AET', 'PEN'}
HALF_COMPLETE = FINISHED | {'HT', '2H', 'ET', 'BT', 'P'}


def stamp(value):
    try:
        d = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return d if d.tzinfo else None
    except (ValueError, TypeError, AttributeError):
        return None


def day_of(value):
    d = stamp(value)
    return d.astimezone(ROME).date().isoformat() if d else None


def forecast_time(match):
    assessment = match.get('multimarket') or {}
    v7 = (assessment.get('matrix') or {}).get('v7') or {}
    return stamp(v7.get('captured_at') if v7.get('status') == 'published' else assessment.get('captured_at'))


def valid_pair(home, away):
    return all(type(n) is int and n >= 0 for n in (home, away))


def live_pair(row, key):
    pair = row.get(key) or {}
    return valid_pair(pair.get('home'), pair.get('away'))


def final_live(row):
    return row.get('status') in FINISHED and (live_pair(row, 'fulltime') or
           row['status'] == 'FT' and live_pair(row, 'goals'))


def merge_result(old, new):
    if not new:
        return old
    if not old or old.get('status') not in FINISHED or not valid_pair(old.get('home_ft'), old.get('away_ft')):
        return new
    if new.get('status') not in FINISHED or not valid_pair(new.get('home_ft'), new.get('away_ft')):
        return old
    old_at, new_at = stamp(old.get('settled_at')), stamp(new.get('settled_at'))
    if old_at and new_at and new_at < old_at:
        return old
    if (not valid_pair(new.get('home_ht'), new.get('away_ht'))
            and valid_pair(old.get('home_ht'), old.get('away_ht'))
            and old['home_ht'] <= new['home_ft'] and old['away_ht'] <= new['away_ft']):
        return dict(new, home_ht=old['home_ht'], away_ht=old['away_ht'])
    return new


def merge_live(target, row):
    previous = target.get('live') or {}
    at, before = stamp(row.get('observed_at')), stamp(previous.get('observed_at'))
    if not at or before and at <= before:
        return
    result = target.get('result') or {}
    result_at = stamp(result.get('settled_at'))
    known_final = result.get('status') in FINISHED and valid_pair(result.get('home_ft'), result.get('away_ft'))
    if known_final and (row.get('status') not in FINISHED or result_at and at < result_at):
        return
    if final_live(previous) and row.get('status') not in FINISHED:
        return
    row = deepcopy(row)
    if final_live(previous) and not final_live(row):
        row['status'] = previous['status']
        row['fulltime'] = deepcopy(previous.get('fulltime') if live_pair(previous, 'fulltime') else previous['goals'])
        row['goals'] = deepcopy(previous.get('goals'))
        row['fulltime_observed_at'] = previous.get('fulltime_observed_at') or previous['observed_at']
    if (row.get('status') in HALF_COMPLETE and not live_pair(row, 'halftime')
            and previous.get('status') in HALF_COMPLETE and live_pair(previous, 'halftime')):
        row['halftime'] = deepcopy(previous['halftime'])
        row['halftime_observed_at'] = previous.get('halftime_observed_at') or previous['observed_at']
    target.update(live=row, status_short=row.get('status') or target.get('status_short'), elapsed=row.get('elapsed'))


def frozen_display(matches, root):
    """Use the existing display rules at the saved prematch time, in isolation."""
    result = subprocess.run(['node', str(Path(__file__).with_name('gold_history_display.cjs'))],
                            input=json.dumps(matches), capture_output=True, text=True,
                            timeout=45, check=True, cwd=root)
    return json.loads(result.stdout)


def merge_day(previous, feed, now, display):
    date = feed.get('date')
    saved = {str(m.get('fixture', {}).get('fixture_id')): deepcopy(m)
             for m in (previous or {}).get('matches', [])}
    pending = []
    for match in feed.get('matches', []):
        f = match.get('fixture') or {}
        fid, kickoff = str(f.get('fixture_id', '')), stamp(f.get('kickoff'))
        if not fid.isdigit() or not kickoff or day_of(f['kickoff']) != date:
            continue
        before = saved.get(fid)
        analysis_at = forecast_time(match)
        # Never invent a timestamp or an analysis after kickoff. During first
        # migration, started matches may contain a verifiably frozen estimate.
        valid = bool(analysis_at and analysis_at < kickoff and analysis_at <= now
                     and (match.get('multimarket') or {}).get('status') != 'no_prematch_snapshot')
        old_at = forecast_time(before or {})
        if before is None or (now < kickoff and valid and (not old_at or analysis_at >= old_at)):
            candidate = deepcopy(match)
            candidate.pop('_gold_history', None)
            candidate['_gold_history'] = {'version': VERSION, 'captured_at': analysis_at.isoformat() if valid else None,
                                          'verified_prematch': valid}
            # Store the full original record, never recalculate probabilities.
            saved[fid] = candidate
            if valid:
                pending.append(candidate)
        target = saved[fid]['fixture']
        if f.get('result'):
            target['result'] = deepcopy(merge_result(target.get('result'), f['result']))
        if f.get('live'):
            merge_live(target, f['live'])
        known = target.get('result') or {}
        if not final_live(target.get('live') or {}) and known.get('status') in FINISHED:
            target['status_short'] = known['status']
        elif not target.get('live'):
            for key in ('status_short', 'elapsed'):
                if f.get(key) is not None:
                    target[key] = f[key]
    if pending:
        metadata = display(pending)
        for m, view in zip(pending, metadata, strict=True):
            m['_gold_history'].update(view)
    result = {k: deepcopy(v) for k, v in feed.items() if k not in ('matches', 'previous_day', 'current_day_archive')}
    result.update(matches=list(saved.values()), history_view=True,
                  history_version=VERSION, history_updated_at=now.isoformat())
    return result


def refresh_scores(days, payload):
    if payload.get('schema_version') != 1 or payload.get('enabled') is not True:
        return
    for feed in days.values():
        for m in feed.get('matches', []):
            f = m['fixture']; row = payload.get('fixtures', {}).get(str(f['fixture_id']))
            if not isinstance(row, dict) or str(row.get('fixture_id')) != str(f['fixture_id']):
                continue
            merge_live(f, row)


def project(root, day1, *, now=None, scores=None, display=None, seed=None):
    root = Path(root); now = now or datetime.now(ROME)
    today = now.astimezone(ROME).date()
    keep = {today.isoformat(), (today-timedelta(days=1)).isoformat()}
    history = read_store(root/FILE, {})
    days = {k: v for k, v in history.get('days', {}).items() if k in keep}
    display = display or (lambda matches: frozen_display(matches, root))
    for feed in (seed, day1):
        if isinstance(feed, dict) and feed.get('date') in keep and isinstance(feed.get('matches'), list):
            date = feed['date']
            days[date] = merge_day(days.get(date), feed, now, display)
    if scores is None and days:
        try:
            # Shared score cache only. No new API-Football calls or scan changes.
            request = urllib.request.Request('https://arabsniperbet.com/api/live-scores',
                                             headers={'Accept': 'application/json', 'User-Agent': 'ArabSniperBet-Gold-History/1.0'})
            with urllib.request.urlopen(request, timeout=8) as response:
                scores = json.load(response)
        except Exception:
            scores = {}
    refresh_scores(days, scores or {})
    write_store(root/FILE, {'version': VERSION, 'days': days})
    result = deepcopy(day1)
    result['previous_day'] = days.get((today-timedelta(days=1)).isoformat())
    # Already available at midnight, before the next scan publishes a new day1.
    result['current_day_archive'] = days.get(today.isoformat())
    return result
