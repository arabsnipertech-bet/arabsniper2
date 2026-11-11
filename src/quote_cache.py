"""Reuse genuine bookmaker observations until kickoff, with neutral live flow."""
from copy import deepcopy
from datetime import timedelta
from math import isfinite
from multimarket_model import stamp
from quote_sources import prematch_observation

MAX_HOURS = 12
PRICE_FIELDS = ('odd', 'first_odd', 'opening_odd', 'opening_delta_pct', 'first_observed_at', 'bookmaker', 'bookmaker_id',
    'bet_id', 'provider_updated_at', 'quote_observed_at', 'selected_margin_pct',
    'selected_margin_delta_pp', 'selected_market_fair_pct', 'selected_market_odds',
    'market_fair_pct', 'margin_pct', 'margin_delta_pp', 'margin_band', 'margin_trend',
    'delta_pct', 'oci', 'oci_method', 'oci_previous', 'oci_direction', 'selected_movement',
    'movement_confirmed', 'fair_direction_coherent', 'speed_pp_per_hour', 'fair_delta_pp', 'paired_books', 'agreeing_books',
    'market_behavior', 'category', 'category_reason', 'quote_adapter_version',
    'quote_source', 'quote_executable', 'quote_provenance', 'fair_basis', 'opening_market_odds',
    'prematch_stable')


def valid_number(value):
    return isinstance(value, (float, int)) and not isinstance(value, bool) and isfinite(value)


def source_time(source):
    # A newly downloaded copy of an old provider snapshot cannot renew 12h TTL.
    dates = [stamp(source.get(k)) for k in ('provider_updated_at', 'quote_observed_at')]
    return min((d for d in dates if d), default=None)


def within_window(source, as_of, kickoff):
    if (source.get('quote_executable') is True and source.get('bookmaker_id') is not None
            and stamp(source.get('provider_updated_at'))):
        return prematch_observation(source.get('provider_updated_at'), source.get('quote_observed_at'), as_of, kickoff)
    now, start, at = stamp(as_of), stamp(kickoff), source_time(source)
    dates = [stamp(source.get(k)) for k in ('provider_updated_at', 'quote_observed_at')]
    return bool(now and start and at and now < start and all(d <= now for d in dates if d)
                and 0 <= (now-at).total_seconds() <= MAX_HOURS*3600)


def remember(row):
    if row.get('quote_status') in ('recent', 'cached_stable') and valid_number(row.get('odd')) and row['odd'] > 1:
        row['cached_quote'] = {k: deepcopy(row.get(k)) for k in PRICE_FIELDS}


def preserve_study_flow(previous, current):
    """Keep the last measured OCI for display only when a new scan lacks a reading."""
    if valid_number(current.get('oci')) or current.get('quote_status') not in ('recent', 'cached_stable', 'missing', 'stale'):
        current.pop('study_flow', None)
        return
    candidates = [previous.get('study_flow'), previous.get('cached_quote')]
    if previous.get('quote_status') in ('recent', 'cached_stable'):
        candidates.append(previous)
    snapshots = [s for s in candidates if s and valid_number(s.get('oci')) and
                 valid_number(s.get('odd')) and s['odd'] > 1 and (s.get('bookmaker_id') or s.get('bookmaker')) and
                 stamp(s.get('quote_observed_at')) and stamp(s.get('provider_updated_at'))]
    if not snapshots:
        return
    last = max(snapshots, key=lambda s: stamp(s['quote_observed_at']))
    cached = current.get('cached_quote') or {}
    if valid_number(cached.get('oci')) and stamp(cached.get('quote_observed_at')) and stamp(cached['quote_observed_at']) >= stamp(last['quote_observed_at']):
        current.pop('study_flow', None)
        return
    fields = ('odd', 'first_odd', 'first_observed_at', 'delta_pct', 'bookmaker', 'bookmaker_id',
              'quote_observed_at', 'provider_updated_at', 'oci', 'oci_previous', 'paired_books',
              'agreeing_books', 'fair_delta_pp', 'speed_pp_per_hour', 'quote_source')
    current['study_flow'] = {k: last[k] for k in fields if last.get(k) is not None}


def recalculate(row):
    odd, win, loss = row.get('odd'), row.get('win_weight'), row.get('loss_weight')
    if not all(valid_number(v) for v in (odd, win, loss)): return False
    alternatives = row.get('sensitivity_weights') or [[win, loss]]
    if not all(len(pair) == 2 and all(valid_number(v) for v in pair) for pair in alternatives): return False
    evs = [100*(w*(odd-1)-l) for w, l in alternatives]
    row.update(ev_pct=round(100*(win*(odd-1)-loss), 4), ev_low_pct=round(min(evs), 4),
               ev_high_pct=round(max(evs), 4), implied_pct=100/odd,
               theoretical_value=win*(odd-1)-loss > 0, stable_value=min(evs) > 0 and row.get('quality') == 'supported')
    if valid_number(row.get('first_odd')) and row['first_odd'] > 1:
        row['opening_odd'] = row['first_odd']
        row['opening_delta_pct'] = 100*(odd/row['first_odd']-1)
        row['delta_pct'] = 0. if row.get('prematch_stable') else row['opening_delta_pct']
    return True


def mark_stable(row, kickoff):
    at = source_time(row)
    row.update(quote_status='cached_stable', cache_source_at=at.isoformat())
    if (row.get('quote_executable') is not True or row.get('bookmaker_id') is None
            or not stamp(row.get('provider_updated_at'))):
        row.update(quote_status_label='Quota stabile da cache',
                   cache_expires_at=(at+timedelta(hours=MAX_HOURS)).isoformat())
        return
    row.update(prematch_stable=True, cache_expires_at=kickoff,
               quote_status_label='Stabile da apertura' if row.get('first_odd') == row.get('odd') else 'Ultima quota osservata · stabile',
               delta_pct=0., oci=0., oci_method=None, oci_previous=None, oci_direction=None,
               oci_observed_at=None, oci_unavailable_reason=None, selected_movement=None,
               paired_books=0, agreeing_books=0, persistence=None, speed_pp_per_hour=None,
               fair_delta_pp=None, amplitude_pct=None, movement_hours=None, movement_volatile=False,
               selected_margin_delta_pp=None, margin_delta_pp=None, margin_trend='stable',
               movement_confirmed=False, fair_direction_coherent=False, market_behavior='neutral')
    if row.get('category') in ('mature', 'contrarian'):
        row.update(category='value_watch', category_reason='Valore teorico con quota stabile da confermare')


def restore(row, as_of, kickoff, allowed):
    if not allowed: return False
    candidates = [row.get('cached_quote') or {}]
    last = {k: deepcopy(row.get(k)) for k in PRICE_FIELDS}
    last['odd'] = row.get('odd') or row.get('last_odd')
    candidates.append(last)
    if not valid_number(last.get('odd')):
        candidates.append({**last, 'odd': row.get('first_odd'),
                           'quote_observed_at': row.get('first_observed_at')})
    for source in candidates:
        if not valid_number(source.get('odd')) or source['odd'] <= 1 or not within_window(source, as_of, kickoff): continue
        trial = deepcopy(row)
        trial.update(deepcopy(source))
        trial['oci_method'] = source.get('oci_method')
        if not recalculate(trial): continue
        mark_stable(trial, kickoff)
        row.update(trial)
        remember(row)
        return True
    return False
