"""Read-only review of sealed V7 evidence. No fitting, routing or provider calls."""
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from audit_storage import open_verified_json, read_store
from gold_storage import client, previous_manifest, restore_subset
from lateral_matrix import label, market_id, specification
from multimarket_model import stamp
from v7_probability import MEASUREMENT_PROTOCOL, finite
from v7_shadow_metrics import paired_metrics, research_report

START, END = date(2026, 11, 19), date(2026, 11, 25)
CUTOFF = datetime(2026, 11, 26, 7, tzinfo=timezone.utc)
FOCUS = [specification('total', 'ht', 'over', 1.5),
         specification('total', 'ft', 'over', 2.5),
         specification('btts', 'ft', 'yes'), specification('btts', 'ht', 'yes'),
         specification('total', 'ft', 'over', 3.5),
         specification('total', 'ht', 'over', 2.5),
         specification('total', 'sh', 'over', 1.5),
         specification('result', 'ft', '1'), specification('result', 'ft', '2')]
NAMES = {market_id(spec): label(spec) for spec in FOCUS}
OVER, GG = (market_id(specification(kind, period, side, line))
            for kind, period, side, line in [('total', 'ht', 'over', 1.5), ('btts', 'ft', 'yes', None)])


class ReadOnly:
    """The research has no storage mutation capability."""
    def __init__(self, storage):
        self.storage, self.reads, self.bytes = storage, 0, 0

    def get_object(self, **kwargs):
        key = kwargs['Key']
        if kwargs['Bucket'] != 'arabsniper-gold' or not (key in ('state/current.json', 'explorer/current.json')
                or key.startswith('state/objects/')):
            raise ValueError('Review read scope')
        result = self.storage.get_object(**kwargs)
        self.reads += 1
        self.bytes += result.get('ContentLength', 0)
        return result


def in_time(fixture, cutoff=CUTOFF):
    captured, kickoff, observed = (stamp(fixture.get(k)) for k in ('captured_at', 'kickoff', 'observed_at'))
    return bool(captured and kickoff and observed and captured < kickoff < observed <= cutoff)


def recorded_state(value):
    if value is None: return 'missing'
    if value is True: return 'passed'
    if value is False: return 'failed'
    if finite(value) and value == 0: return 'neutral'
    return 'recorded'


def compact_fixture(fixture):
    result = {k: fixture.get(k) for k in ('kickoff', 'captured_at', 'observed_at', 'status',
              'result', 'forecast_sha256', 'triad', 'score_periods', 'outcomes')}
    shadow = (fixture.get('decision_shadow') or {}).get('latest') or {}
    ledger = shadow.get('movement_study') or {}
    result['decision_shadow'] = {'latest': {**{k: shadow.get(k) for k in
        ('captured_at', 'forecast_sha256', 'research_weight_grid', 'variants')},
        'movement_study': {**{k: ledger.get(k) for k in ('protocol', 'config_sha256', 'captured_at', 'kickoff', 'available')},
                          'cohorts': [r for r in ledger.get('cohorts', []) if r.get('market_id') in NAMES]}}}
    trace = fixture.get('measurement') or {}
    if trace.get('protocol') == MEASUREMENT_PROTOCOL:
        keys = ('probability', 'raw_probability', 'statistical_probability', 'fair', 'odd', 'quote_usable',
                'observers', 'official_roles', 'supported', 'tier', 'without_peak', 'without_peak_state',
                'oci_state', 'missing_flow_fields', 'edge_exclusions', 'policy_exclusions')
        result['measurement'] = {**{k: trace.get(k) for k in ('protocol', 'config_sha256', 'equivalent_prices')},
            'markets': {key: {k: info.get(k) for k in keys} for key, info in trace.get('markets', {}).items()
                        if info.get('observers')}}
    return result


def summary(rows, probability='probability'):
    settled = [r for r in rows if type(r.get('outcome')) is int and r['outcome'] in (0, 1)]
    scored = [r for r in settled if finite(r.get(probability)) and 0 <= r[probability] <= 1]
    priced = [r['roi'] for r in settled if finite(r.get('roi'))]
    return dict(n=len(rows), settled=len(settled), fixtures=len({r['fixture_id'] for r in rows}),
        wins=sum(r['outcome'] for r in settled),
        hit_rate=sum(r['outcome'] for r in settled)/len(settled) if settled else None,
        probability_n=len(scored), missing_probability=len(settled)-len(scored),
        mean_probability=sum(r[probability] for r in scored)/len(scored) if scored else None,
        brier=sum((r[probability]-r['outcome'])**2 for r in scored)/len(scored) if scored else None,
        roi_n=len(priced), roi_control=sum(priced)/len(priced) if priced else None)


def original_choices(fid, fixture, day):
    """Never replace an original choice P with the later forecast's probability."""
    rows = []
    for role, pick in (fixture.get('triad') or {}).items():
        if not pick: continue
        key, captured = pick.get('market_id'), stamp(pick.get('captured_at'))
        kickoff = stamp(fixture.get('kickoff'))
        outcome = (fixture.get('outcomes') or {}).get(key) or {}
        win, loss = outcome.get('win'), outcome.get('loss')
        binary = type(win) in (int, float) and win in (0, 1) and loss in (0, 1) and win+loss == 1
        identity = (captured and kickoff and captured < kickoff
            and str(pick.get('fixture_id', fid)) == str(fid)
            and (not pick.get('kickoff') or stamp(pick['kickoff']) == kickoff))
        if pick.get('captured_at') and not identity:
            binary = False
        p = pick.get('probability') if identity else None
        odd = pick.get('odd')
        rows.append(dict(fixture_id=fid, date=day, role=role, market=key,
            probability=p if finite(p) and 0 <= p <= 1 else None, odd=odd,
            outcome=int(win) if binary else None,
            roi=win*(odd-1)-loss if binary and finite(odd) and odd > 1 else None,
            tier=pick.get('tier'), study_only=pick.get('study_only'),
            original_ev=p*odd-1 if binary and finite(p) and finite(odd) and odd > 1 else None))
    return rows


def cache_recovery_probe(archives):
    """Current cached outcomes only; separate from the frozen 07:00 backtest."""
    from unittest.mock import patch
    from audit_official_scores import cached_feed, needs_result, read_live_scores
    from v7_archive import FINISHED, VOID, rescheduled_result, ROME
    now = datetime.now(timezone.utc)
    recent = [a for a in archives if 1 <= (now.astimezone(ROME).date()-date.fromisoformat(a['date'])).days <= 4]
    try:
        data = cached_feed()
    except Exception as error:
        return dict(as_of=now.isoformat(), status='unavailable', error_type=type(error).__name__,
                    provider_calls=0, remote_writes=0)
    coverage = {}
    for archive in recent:
        pending = {fid:f for fid,f in archive['fixtures'].items() if f.get('status') == 'pending'
                   or f.get('status') == 'settled' and needs_result(f.get('result') or {})}
        with patch('audit_official_scores.cached_feed',return_value=data):
            scores = read_live_scores({fid:f['kickoff'] for fid,f in pending.items()},now)
        finals = {fid:s for fid,s in scores.items() if s.get('status') in FINISHED
                  and None not in (s.get('home_ft'),s.get('away_ft'))}
        coverage[archive['date']] = dict(pending_or_missing_ht=len(pending),
            cached_confirmed_finals=len(finals),
            cached_complete_ht_ft=sum(not needs_result(s) for s in finals.values()),
            cached_void_or_rescheduled=sum(s.get('status') in VOID or rescheduled_result(s) for s in scores.values()),
            still_without_cached_final=len(set(pending)-set(finals)))
    return dict(as_of=now.isoformat(),status='read',days=coverage,
                provider_calls=0,remote_writes=0,historical_probabilities_reconstructed=False)


def review(archives, draws=4000):
    coverage, samples, official = {}, [], []
    cohorts, comparisons, failures, trace_states = defaultdict(list), defaultdict(list), defaultdict(Counter), Counter()
    seen, kickoff_by_id, accepted = set(), defaultdict(set), []
    for archive in archives:
        day = archive['date']
        fixtures = {str(fid): f for fid, f in archive['fixtures'].items() if in_time(f)}
        for fid, f in fixtures.items(): kickoff_by_id[fid].add(f['kickoff'])
        accepted.append((day, fixtures, archive['samples']))
        original_fixtures = list(archive['fixtures'].values())
        coverage[day] = dict(Counter(f.get('status') or 'missing_status' for f in original_fixtures), fixtures=len(archive['fixtures']),
            temporally_available=len(fixtures), samples=len(archive['samples']),
            with_regulation_ft=sum(all(type((f.get('result') or {}).get(k)) is int for k in ('home_ft','away_ft')) for f in original_fixtures),
            with_halftime=sum(all(type((f.get('result') or {}).get(k)) is int for k in ('home_ht','away_ht')) for f in original_fixtures),
            with_new_measurement=sum((f.get('measurement') or {}).get('protocol') == MEASUREMENT_PROTOCOL for f in original_fixtures))
    conflicts = {fid for fid, starts in kickoff_by_id.items() if len(starts) > 1}
    research = []
    for day, fixtures, source in accepted:
        fixtures = {fid: f for fid, f in fixtures.items() if fid not in conflicts}
        research.append({'fixtures': fixtures})
        for row in source:
            fid, market = str(row['fixture_id']), row['market']
            if fid not in fixtures or (fid, market) in seen or row.get('date') != day: continue
            observed = stamp(row.get('observed_at'))
            if not observed or observed > CUTOFF: continue
            seen.add((fid, market)); samples.append(row)
        for fid, fixture in fixtures.items():
            picks = original_choices(fid, fixture, day); official.extend(picks)
            for pick in picks:
                if pick['outcome'] == 0:
                    failures['by_market'][NAMES.get(pick['market'], pick['market'])] += 1
                    failures['by_role'][pick['role']] += 1
                if finite(pick['original_ev']) and pick['original_ev'] < 0:
                    failures['original_negative_ev'][pick['role']] += 1
                if pick['tier'] in ('gold', 'platinum') and pick['study_only'] is True:
                    failures['study_with_qualified_badge'][day] += 1
            ledger = ((fixture.get('decision_shadow') or {}).get('latest') or {}).get('movement_study') or {}
            config = ledger.get('config_sha256')
            if not config or ledger.get('available') is not True: continue
            rows = [dict(r, fixture_id=fid, date=day, probability=r.get('probability'),
                         outcome=int(r['win']) if r.get('win') in (0, 1) and r.get('loss') in (0, 1)
                         and r['win']+r['loss'] == 1 else None)
                    for r in ledger.get('cohorts', [])]
            selected = {p['market'] for p in picks}
            for row in rows:
                if row.get('market_id') not in NAMES: continue
                group = f"{config}|{NAMES[row['market_id']]}"
                cohorts[group].append(row)
                trace_states['oci|'+str(row.get('oci_state', 'missing'))] += 1
                state = row.get('without_peak_supported')
                trace_states['without_peak|'+recorded_state(state)] += 1
                if row['market_id'] not in selected and row.get('supported') is True and finite(row.get('ev')):
                    name = 'unselected_positive_ev' if row['ev'] > 0 else 'unselected_nonpositive_ev'
                    failures[name]['rows'] += 1
                    failures[name]['wins'] += row['outcome'] == 1
            common = {r['market_id']: r for r in rows if r.get('supported') is True and r.get('quote_usable') is True}
            if OVER in common and GG in common:
                a, b = common[OVER], common[GG]
                same_book = a.get('bookmaker_id') is not None and str(a['bookmaker_id']) == str(b.get('bookmaker_id'))
                comparisons[f'{config}|'+('same_book' if same_book else 'different_or_missing_book')].append((a, b))
    pairs = {}
    for key, rows in comparisons.items():
        settled = [(a, b) for a, b in rows if a['outcome'] is not None and b['outcome'] is not None]
        pairs[key] = dict(n=len(rows), settled=len(settled),
            outcomes=dict(Counter(('both' if a['outcome'] and b['outcome'] else 'over_only' if a['outcome']
                          else 'gg_only' if b['outcome'] else 'neither') for a, b in settled)),
            over15_ht=summary([a for a, _ in rows]), gg_ft=summary([b for _, b in rows]),
            selection_rule_changed=False, note='Different target events: no paired Brier delta between Over and GG.')
    return dict(coverage=coverage, kickoff_conflicts=len(conflicts),
        monitored_markets={name: dict(summary([r for r in samples if r['market'] == key], 'calibrated'),
            fair=summary([r for r in samples if r['market'] == key], 'fair'),
            interpretation='Descriptive across evolving configurations; not a promotion test.') for key, name in NAMES.items()},
        official_by_role={role: summary([r for r in official if r['role'] == role]) for role in sorted({r['role'] for r in official})},
        official_by_market={name: summary([r for r in official if r['market'] == key]) for key, name in NAMES.items()},
        failure_counts={k: dict(v) for k, v in failures.items()}, trace_states=dict(trace_states),
        configuration_cells={key: dict(summary(rows), paired_vs_own_fair=paired_metrics(rows, draws)) for key, rows in cohorts.items()},
        over15_ht_vs_gg_ft=pairs,
        research_trace=research_report(research, CUTOFF, draws),
        bounds=dict(original_missing_probability_not_filled=True, configurations_not_pooled_for_bootstrap=True,
                    distinct_target_brier_not_compared=True, roi_selection_criterion=False, automatic_promotion=False))


def main():
    import ijson
    storage, bucket = ReadOnly(client(fast_restore=True)), os.environ['R2_BUCKET']
    index = previous_manifest(storage, bucket)
    days = [(START+timedelta(days=i)).isoformat() for i in range((END-START).days+1)]
    required = [f'auditarchive/prematch_{day}_v7.json' for day in days]
    extras = ['data/arab_quote_v7_weights.json', 'data/arab_quote_auditor_daily_priority.json',
              f'auditarchive/prematch_{END}_v7_learning.json']
    paths = [p for p in required+extras if p in index]
    if not all(p in index for p in required): raise ValueError('Missing closed-day archives')
    with tempfile.TemporaryDirectory(prefix='v7-weekend-review-') as folder:
        root = Path(folder); restore_subset(storage, bucket, root, index, paths, fast_restore=True)
        archives = []
        for day, path in zip(days, required):
            with open_verified_json(root/path) as stream:
                fixtures = {str(fid): compact_fixture(f) for fid, f in ijson.kvitems(stream, 'fixtures', use_float=True)}
            with open_verified_json(root/path) as stream:
                samples = list(ijson.items(stream, 'samples.item', use_float=True))
            archives.append(dict(date=day, fixtures=fixtures, samples=samples))
            print(json.dumps({'loaded_day': day, 'fixtures': len(fixtures), 'samples': len(samples)}), flush=True)
        report = review(archives)
        report['cached_results_probe'] = cache_recovery_probe(archives)
        learning = read_store(root/extras[-1], {})
        report['learning'] = {k: learning.get(k) for k in ('version', 'date', 'window_days', 'half_life_days',
            'state', 'checked_days', 'anomalies', 'verified_fixtures', 'comparison', 'update', 'manual_review')}
        report['existing_shadow'] = learning.get('movement_shadow')
        report['active_weight_state'] = read_store(root/extras[0], {})
        report['daily_priority_certificate'] = read_store(root/extras[1], {})
        report['source'] = dict(cutoff=CUTOFF.isoformat(), start=str(START), end=str(END),
            manifest_sha256=hashlib.sha256(json.dumps(index, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
            files={p:index[p] for p in paths}, remote_writes=0, football_requests=0,
            original_archives_modified=False, bootstrap_draws=4000)
        report['storage_reads'] = dict(gets=storage.reads, bytes=storage.bytes)
        output = ROOT/'test-artifacts/weekend-quality-review.json'
        output.parent.mkdir(exist_ok=True); output.write_text(json.dumps(report, indent=2, ensure_ascii=False))
        print(json.dumps({'report': output.name, 'remote_writes': 0, 'football_requests': 0,
                          'official_roles': report['official_by_role'], 'learning': report['learning']['state']}), flush=True)


if __name__ == '__main__':
    try: main()
    except Exception as error: raise SystemExit('Read-only weekend review stopped: '+type(error).__name__) from None
