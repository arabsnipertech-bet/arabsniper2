"""Bounded prematch reuse of an observed price; refreshes never reset its age."""
from copy import deepcopy
from datetime import timedelta
from math import isfinite
from multimarket_model import stamp

MAX_HOURS = 12
PRICE_FIELDS = ('odd', 'first_odd', 'first_observed_at', 'bookmaker', 'bookmaker_id',
    'bet_id', 'provider_updated_at', 'quote_observed_at', 'selected_margin_pct',
    'selected_margin_delta_pp', 'selected_market_fair_pct', 'selected_market_odds',
    'market_fair_pct', 'margin_pct', 'margin_delta_pp', 'margin_band', 'margin_trend',
    'delta_pct', 'oci', 'oci_previous', 'oci_direction', 'selected_movement',
    'movement_confirmed', 'fair_direction_coherent', 'paired_books', 'agreeing_books',
    'market_behavior', 'category', 'category_reason', 'quote_adapter_version')


def valid_number(value):
    return isinstance(value, (float, int)) and not isinstance(value, bool) and isfinite(value)


def source_time(source):
    # A newly downloaded copy of an old provider snapshot cannot renew 12h TTL.
    dates = [stamp(source.get(k)) for k in ('provider_updated_at', 'quote_observed_at')]
    return min((d for d in dates if d), default=None)


def within_window(source, as_of, kickoff):
    now, start, at = stamp(as_of), stamp(kickoff), source_time(source)
    dates = [stamp(source.get(k)) for k in ('provider_updated_at', 'quote_observed_at')]
    return bool(now and start and at and now < start and all(d <= now for d in dates if d)
                and 0 <= (now-at).total_seconds() <= MAX_HOURS*3600)


def remember(row):
    if row.get('quote_status') in ('recent', 'cached_stable') and valid_number(row.get('odd')) and row['odd'] > 1:
        row['cached_quote'] = {k: deepcopy(row.get(k)) for k in PRICE_FIELDS}


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
        row['delta_pct'] = 100*(odd/row['first_odd']-1)
    return True


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
        if not recalculate(trial): continue
        at = source_time(source)
        trial.update(quote_status='cached_stable', quote_status_label='Quota stabile da cache',
                     cache_source_at=at.isoformat(), cache_expires_at=(at+timedelta(hours=MAX_HOURS)).isoformat())
        row.update(trial)
        remember(row)
        return True
    return False
