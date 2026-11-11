"""Editorial tests exercise privacy, timing, immutable receipts and outcomes."""
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from unittest import TestCase, mock
import json
import sys

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from publisher_v71 import delivery, editorial, live, public_feed, report, selection
from publisher_v71.channels import DeliveryError, TelegramHistory
from publisher_v71.runner import add_group, plan_report, record_home_receipt
from publisher_v71.storage import Storage
from v7_decision import GOAL, fingerprint

NOW = datetime.fromisoformat('2026-11-10T22:55:00+00:00')


def pick(fid, minutes=40, tier='GOLD', role='most_probable'):
    return {'fixture_id': str(fid), 'match': f'Club {fid} – Ospiti', 'league': 'Campionato',
            'kickoff': selection.utc(NOW + timedelta(minutes=minutes)),
            'category': selection.CATEGORIES[role], 'tier': tier, 'role': role,
            'market': 'Over 2,5 finale', 'market_id': GOAL['over25_ft'], 'odd': 1.91,
            'probability': .58, 'captured_at': selection.utc(NOW - timedelta(minutes=3)),
            'quote_at': selection.utc(NOW - timedelta(minutes=5)), 'expires_at': selection.utc(NOW + timedelta(minutes=55)),
            'observed_at': selection.utc(NOW), 'reason': 'Snapshot prematch verificato',
            'forecast_sha256': 'x' * 64}


def board(item):
    fid, kickoff = item['fixture_id'], item['kickoff']
    captured = item['captured_at']
    f = dict(fixture_id=fid, kickoff=kickoff, status_short='NS', match=item['match'],
             league='Premier League', country='England')
    p = dict(market_id=item['market_id'], source_market_id='m', label=item['market'],
             odd=item['odd'], probability=item['probability'], tier=item['tier'])
    market = dict(source_market_id='m', odd=item['odd'], calibrated=item['probability'])
    source = dict(id='m', probability_model='arabsniper-v7.0', odd=item['odd'], quote_status='recent',
                  provider_updated_at=item['quote_at'], quote_observed_at=item['quote_at'],
                  bookmaker_id=2, quote_executable=True)
    v7 = dict(version='arabsniper-v7.0', status='published', fixture_id=fid, captured_at=captured,
              kickoff=kickoff, triad={item['role']: p}, markets={item['market_id']: market})
    v7['sha256'] = fingerprint(v7)
    return {'date': selection.rome_day(datetime.fromisoformat(kickoff)), 'updated_at': captured,
            'matches': [{'fixture': f, 'multimarket': {'matrix': {'v7': v7, 'rows': [source]}}}]}


def full_board(item):
    payload = board(item)
    matrix = payload['matches'][0]['multimarket']['matrix']
    snapshot = matrix['v7']
    snapshot['markets'][item['market_id']]['label'] = item['market']
    for role, name, label in [('recommended_alternative', 'over15_ht', 'Over 1,5 primo tempo'),
                              ('high_odds_shot', 'over25_ht', 'Over 2,5 primo tempo')]:
        key = GOAL[name]
        estimate = {'source_market_id': name, 'odd': None, 'model_fair_odd': 3.125}
        snapshot['markets'][key] = {**estimate, 'label': label, 'calibrated': .32, 'private': 'private source'}
        snapshot['triad'][role] = {**estimate, 'market_id': key, 'probability': .32, 'tier': 'BLU',
                                  'study_only': True, 'quote_state': 'model_estimate'}
        matrix['rows'].append({'id': name, 'probability_model': snapshot['version']})
    snapshot['sha256'] = fingerprint({k: v for k, v in snapshot.items() if k != 'sha256'})
    return payload


def certified_day():
    audit = {'date': '2026-11-11', 'generated_at': '2026-11-12T03:30:00Z', 'fixtures': {}, 'fixture_labels': {}}
    for number in reversed(range(1, 8)):
        pick_row = pick(number, number + 25)
        fid = pick_row['fixture_id']
        market = GOAL['over15_ht']
        audit['fixtures'][fid] = {
            'kickoff': pick_row['kickoff'], 'captured_at': pick_row['captured_at'],
            'status': 'settled', 'result': {'status': 'FT', 'home_ft': 1, 'away_ft': 1, 'home_ht': 1, 'away_ht': 1},
            'triad': {'most_probable': {'market_id': GOAL['over25_ft'], 'label': 'Over 2,5 finale'},
                      'recommended_alternative': {'market_id': market, 'label': 'Over 1,5 primo tempo',
                                                  'horizon': 'PT', 'study_only': True}},
            'outcomes': {market: {'win': 1, 'loss': 0}, GOAL['over25_ft']: {'win': 0, 'loss': 1}},
        }
        audit['fixture_labels'][fid] = pick_row['match']
    audit['fixtures']['1']['captured_at'] = audit['fixtures']['1']['kickoff']
    audit['fixtures']['2']['status'] = 'pending'
    return audit


class LocalStore(Storage):
    def __init__(self):
        super().__init__(None, '')
        self.immutable = {}
        self.saved = 0

    def save(self):
        self.saved += 1

    def freeze(self, key, value):
        if key in self.immutable:
            return False
        self.immutable[key] = json.loads(json.dumps(value))
        return True

    def read(self, key, default=None, **kwargs):
        return self.immutable.get(key.removeprefix('evidence/'), default), None


class PublisherTests(TestCase):
    def test_accepts_real_v7_contract_and_rejects_tampering_and_staleness(self):
        item = pick(1)
        payload = board(item)
        with mock.patch('publisher_v71.selection.output_allowed', return_value=True):
            rows = selection.published_candidates([payload, payload], NOW)
            self.assertEqual(len(rows), 1)
            self.assertEqual((rows[0]['probability'], rows[0]['category']), (.58, 'Probabile'))
            self.assertEqual(selection.rome_day(datetime.fromisoformat(rows[0]['kickoff'])), '2026-11-11')
            self.assertEqual(selection.published_candidates([board(pick(1, 120))], NOW + timedelta(minutes=70)), [])
            payload['matches'][0]['multimarket']['matrix']['v7']['triad']['most_probable']['odd'] = 8.00
            self.assertEqual(selection.published_candidates([payload], NOW), [])

    def test_group_two_three_exception_and_deadline(self):
        for size in (2, 3):
            picks = [pick(i, 32 + i * 30) for i in range(size)]
            self.assertEqual(selection.group_for(picks, NOW), picks)
        self.assertEqual(selection.group_for([pick(1, 19), pick(2, 55, 'BLU')], NOW), [])
        self.assertEqual(selection.group_for([pick(1, 41, 'PLATINUM')], NOW), [pick(1, 41, 'PLATINUM')])
        self.assertEqual(selection.group_for([pick(1, 41, 'BLU')], NOW), [])
        self.assertEqual(selection.group_for([pick(1, 41, 'GOLD', 'high_odds_shot')], NOW), [])

    def test_partial_projection_is_the_single_public_boundary(self):
        store = LocalStore()
        free, secret, other = pick(1), pick(2, 65), pick(3, 80)
        secret.update(market='PRIVATE MARKET', odd=88.8, probability=.123456, reason='PRIVATE REASON')
        self.assertTrue(add_group(store, [free, secret, other], NOW, ('telegram', 'facebook')))
        key = selection.group_id([free, secret, other])
        store.state['groups'][key]['eligible_home'] = True
        payload = public_feed.projection(store.state, NOW)
        self.assertEqual(len(payload['groups']), 1)
        projected = payload['groups'][0]
        self.assertEqual(len(projected['previews']), 2)
        for row in projected['previews']:
            self.assertEqual(set(row), set(selection.PREVIEW_FIELDS))
        self.assertNotIn('PRIVATE', json.dumps(payload))
        self.assertNotIn('88.8', json.dumps(payload))
        for post in store.state['posts'].values():
            self.assertNotIn('PRIVATE', post['text'])
            self.assertIn('quota 1,91', post['text'])
            self.assertLessEqual(len(post['text'].encode('utf-16-le')) // 2, 1024)
        self.assertEqual(public_feed.projection(store.state, NOW + timedelta(minutes=41))['groups'], [])

    def test_home_uses_complete_gold_triads_and_locked_schedule_without_best(self):
        incomplete = board(pick(1, 160, 'BLU'))
        complete = [full_board(pick(i, 160 + i, 'BLU')) for i in (2, 3, 4, 5)]
        # One interrupted fixture and one corrupt forecast must not be showcased.
        complete[-1]['matches'][0]['fixture']['status_short'] = 'PST'
        complete[0]['matches'][0]['multimarket']['matrix']['v7']['sha256'] = 'tampered'
        with mock.patch('publisher_v71.showcase.output_allowed', return_value=True):
            view = public_feed.projection(LocalStore().state, NOW, boards=[incomplete, *complete])
        self.assertEqual(view['groups'], [])
        self.assertEqual([row['fixture_id'] for row in view['gold_previews']], ['3', '4'])
        self.assertEqual([row['fixture_id'] for row in view['gold_schedule']], ['1', '2', '3', '4'])
        for fixture in view['gold_schedule']:
            self.assertEqual(set(fixture), {'fixture_id', 'match', 'league', 'kickoff'})
        proposals = view['gold_previews'][0]['proposals']
        self.assertEqual([row['category'] for row in proposals], list(selection.TRIAD_LABELS.values()))
        self.assertEqual(proposals[0]['odd'], 1.91)
        self.assertNotIn('odd', proposals[1])
        self.assertNotIn('private source', json.dumps(view))
        self.assertNotIn('model_fair_odd', json.dumps(view))

    def test_homepage_receipt_recovers_from_an_already_visible_revision(self):
        store = LocalStore()
        picks = [pick(1), pick(2, 65)]
        self.assertTrue(add_group(store, picks, NOW, ('telegram',)))
        key = selection.group_id(picks)
        group = store.state['groups'][key]
        original = {'groups': [selection.project_group(group, NOW)]}
        record_home_receipt(store, original, 'published-ref', NOW + timedelta(minutes=1))
        record_home_receipt(store, {'groups': []}, 'new-ref', NOW + timedelta(hours=1))
        self.assertEqual(group['homepage_revision'], 'published-ref')
        self.assertEqual(group['homepage_at'], selection.utc(NOW + timedelta(minutes=1)))

    def test_group_retry_uses_the_already_frozen_snapshot(self):
        store = LocalStore()
        original = [pick(1), pick(2, 65)]
        group_id = selection.group_id(original)
        store.freeze(f'2026-11-11/groups/{group_id}.json',
                     {'id': group_id, 'picks': original, 'created_at': selection.utc(NOW), 'eligible_home': False})
        changed = [dict(original[0], odd=2.02), original[1]]
        self.assertTrue(add_group(store, changed, NOW, ('telegram',)))
        post = next(iter(store.state['posts'].values()))
        self.assertEqual(post['view']['free']['odd'], 1.91)
        self.assertEqual(delivery.can_send(post, NOW, {'1': changed[0], '2': changed[1]}), 'quote_or_decision_changed')

    def test_fixture_claims_survive_active_ledger_retention(self):
        store = LocalStore()
        self.assertTrue(add_group(store, [pick(1), pick(2, 65)], NOW, ('telegram',)))
        store.prune(NOW + timedelta(days=10))
        self.assertFalse(store.state['groups'])
        self.assertEqual(store.state['runtime']['reserved_fixtures'], ['1', '2'])

    def test_unknown_delivery_reconciles_and_never_reposts(self):
        store = LocalStore()
        delivery.queue(store, 'academy', 'day', editorial.academy(NOW.date()), ('telegram',),
                       NOW, NOW + timedelta(hours=1))
        post = next(iter(store.state['posts'].values()))
        service = mock.Mock()
        service.send.side_effect = DeliveryError('uncertain', 'timeout')
        service.reconcile.return_value = None
        args = (store, post, service, {}, lambda: NOW, mock.Mock())
        delivery.attempt(*args)
        delivery.attempt(*args)
        self.assertEqual((post['status'], service.send.call_count), ('uncertain', 1))
        service.reconcile.return_value = {'id': '12', 'at': selection.utc(NOW)}
        delivery.attempt(*args)
        delivery.attempt(*args)
        self.assertEqual((post['status'], service.send.call_count), ('sent', 1))

    def test_retry_budget_and_stale_prices(self):
        store = LocalStore()
        delivery.queue(store, 'academy', 'day', editorial.academy(NOW.date()), ('telegram',),
                       NOW, NOW + timedelta(hours=2))
        post = next(iter(store.state['posts'].values()))
        service = mock.Mock()
        service.send.side_effect = DeliveryError('retry', 'rate_limit')
        for i in (0, 1, 4, 8):
            delivery.attempt(store, post, service, {}, lambda: NOW + timedelta(minutes=i), mock.Mock())
        self.assertEqual((post['status'], post['attempts'], service.send.call_count), ('failed', 3, 3))
        self.assertTrue(add_group(store, [pick(1), pick(2, 65)], NOW, ('telegram',)))
        best = next(p for p in store.state['posts'].values() if p['kind'] == 'best')
        changed = dict(pick(1), odd=1.95)
        self.assertEqual(delivery.can_send(best, NOW, {'1': changed, '2': pick(2, 65)}), 'quote_or_decision_changed')

    def test_auditor_receipts_separate_cohorts_and_include_loss(self):
        rows = [pick(i) for i in (1, 2, 3, 4)]
        audit = {'date': '2026-11-11', 'generated_at': '2026-11-11T03:30:00Z', 'fixtures': {}}
        for row, status, goals in zip(rows, ('settled', 'settled', 'void', 'pending'), (1, 3, 0, 0)):
            audit['fixtures'][row['fixture_id']] = {'status': status, 'kickoff': row['kickoff'],
                'result': {'status': 'FT', 'home_ft': goals, 'away_ft': 0, 'home_ht': 0, 'away_ht': 0}}
        verified = report.build(audit, rows, rows[:2])
        self.assertEqual(verified['cohorts']['public']['roi'], -4.5)
        self.assertEqual(verified['cohorts']['gold']['void'], 1)
        self.assertEqual(verified['cohorts']['gold']['pending'], 1)
        self.assertEqual([e['outcome'] for e in verified['examples']], ['lost', 'won'])
        self.assertIsNone(report.build(audit, [pick(8)], []))

    def test_recap_uses_up_to_four_certified_fixtures_and_a_winning_triad_slot(self):
        audit = certified_day()
        view = report.build(audit, [], [])
        rows = view['triad_examples']
        self.assertEqual([row['match'] for row in rows], [pick(i)['match'] for i in (3, 4, 5, 6)])
        self.assertTrue(all(row['category'] == 'Consigliata' and row['halftime'] == '1–1' for row in rows))
        self.assertEqual(view['cohorts']['gold']['total'], 0)
        self.assertEqual(view['examples'], [])
        for channel in ('telegram', 'facebook'):
            text = editorial.message('report', view, 'AS71-EXAMPLE', channel)
            for row in rows:
                self.assertIn(row['match'], text)
            self.assertIn('Esempi selezionati dalle schede Gold', text)
            self.assertIn('https://arabsniperbet.com/#risultati', text)
            self.assertLessEqual(len(text.encode('utf-16-le')) // 2, 1024)
        audit['fixture_labels'] = {'3': pick(3)['match']}
        self.assertEqual(len(report.build(audit, [], [])['triad_examples']), 1)

    def test_existing_recap_is_enriched_in_home_without_a_second_delivery(self):
        store = LocalStore()
        previous = '2026-11-11'
        old_view = {'date': previous, 'cohorts': {k: report.metrics([]) for k in editorial.COHORTS}, 'examples': []}
        now = datetime.fromisoformat('2026-11-12T10:00:00+00:00')
        store.state['reports'][previous] = {'created_at': selection.utc(now), 'view': old_view}
        with mock.patch('publisher_v71.report.certified', return_value=certified_day()) as certified:
            plan_report(store, now, ('telegram', 'facebook'), datetime.fromisoformat(previous).date())
            plan_report(store, now, ('telegram', 'facebook'), datetime.fromisoformat(previous).date())
        self.assertEqual(certified.call_count, 1)
        self.assertEqual(store.state['posts'], {})
        self.assertNotIn('triad_examples', old_view)
        self.assertEqual(len(store.state['reports'][previous]['view']['triad_examples']), 4)

    def test_live_uses_official_result_and_reserve(self):
        state = live.start_state(pick(1), pick(2, 105), NOW)
        after = NOW + timedelta(minutes=60)
        self.assertEqual(live.advance(state, {}, after, {'2': pick(2, 105)})['fixture_id'], '2')
        self.assertTrue(state['used_reserve'])
        end = NOW + timedelta(minutes=220)
        fixture = {'fixture_id': '2', 'observed_at': selection.utc(end - timedelta(minutes=4)),
                   'source': 'API-Football', 'status': 'FT', 'refresh_failed': False,
                   'fulltime': {'home': 2, 'away': 1}}
        result = live.advance(state, {'schema_version': 1, 'enabled': True, 'fixtures': {'2': fixture}}, end, {})
        self.assertEqual((result['status'], result['home'], result['away']), ('Finale', 2, 1))
        self.assertNotIn('market', result)

    def test_public_receipt_parser_only_exact_channel_messages(self):
        parser = TelegramHistory('AS71-ABCD')
        parser.feed('<div data-post="arabsniper_signals/12"><div class="tgme_widget_message_text">Sent<br>AS71-ABCD</div>'
                    '<time datetime="2026-11-10T23:00:00+00:00"></time></div>')
        self.assertEqual(parser.matches['12'], datetime(2026, 11, 10, 23, tzinfo=timezone.utc))

    def test_all_five_formats(self):
        best = {'free': selection.project_pick(pick(1), True), 'previews': [selection.project_pick(pick(2))]}
        result = {'date': '2026-11-10', 'cohorts': {k: report.metrics([]) for k in editorial.COHORTS}, 'examples': []}
        for kind, view in [('best', best), ('report', result), ('live', live.status_view(pick(1), 'Prematch', NOW)),
                           ('academy', editorial.academy(NOW.date())), ('gold', {})]:
            self.assertEqual(Image.open(BytesIO(editorial.card(kind, view))).size, (1080, 1350))
