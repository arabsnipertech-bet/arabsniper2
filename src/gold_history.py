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
        incoming_at = stamp((f.get('live') or {}).get('observed_at'))
        saved_at = stamp((target.get('live') or {}).get('observed_at'))
        keys = ('result',) if saved_at and (not incoming_at or incoming_at < saved_at) else ('result', 'live', 'status_short', 'elapsed')
        for key in keys:
            if key in f and f[key] is not None:
                target[key] = deepcopy(f[key])
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
            at = stamp(row.get('observed_at'))
            previous = stamp((f.get('live') or {}).get('observed_at'))
            if not at or (previous and at <= previous):
                continue
            f['live'] = deepcopy(row)
            f['status_short'] = row.get('status', f.get('status_short'))
            f['elapsed'] = row.get('elapsed')


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
            with urllib.request.urlopen('https://arabsniperbet.com/api/live-scores', timeout=8) as response:
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
