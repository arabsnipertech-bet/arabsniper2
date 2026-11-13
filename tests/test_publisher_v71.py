"""Editorial tests exercise privacy, timing, immutable receipts and outcomes."""
from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from unittest import TestCase, mock
import json
import os
import sys

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from publisher_v71 import delivery, editorial, live, monitor, public_feed, report, runner, scores, selection, showcase, studio
from publisher_v71.channels import DeliveryError, Telegram, TelegramHistory
from publisher_v71.runner import add_group, plan_report, record_home_receipt
from publisher_v71.storage import Storage
from gold_storage import sha
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


def studio_board(now, fid, kickoff_in, quote_age=5):
    row = pick(fid, tier='BLU')
    row.update(kickoff=selection.utc(now + timedelta(minutes=kickoff_in)),
               captured_at=selection.utc(now - timedelta(minutes=3)),
               quote_at=selection.utc(now - timedelta(minutes=quote_age)))
    payload = full_board(row)
    snapshot = payload['matches'][0]['multimarket']['matrix']['v7']
    snapshot['triad']['most_probable']['study_only'] = True
    snapshot['sha256'] = fingerprint({key: value for key, value in snapshot.items() if key != 'sha256'})
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


def event(detail, minute, team, *, extra=None, comments=None):
    return {'type': 'Goal', 'detail': detail, 'comments': comments, 'time': {'elapsed': minute, 'extra': extra},
            'team': {'id': team, 'name': 'Casa' if team == 10 else 'Ospiti'}, 'player': {'id': minute}}


def fixture(phase, goals, events, *, halftime=None, fulltime=None):
    def pair(value):
        return dict(zip(('home', 'away'), value)) if value is not None else None

    return {'fixture': {'id': 1, 'status': {'short': phase, 'elapsed': 90 if phase == 'FT' else 40}},
            'goals': pair(goals), 'score': {'halftime': pair(halftime), 'fulltime': pair(fulltime)},
            'events': events, 'teams': {'home': {'id': 10}, 'away': {'id': 20}}}


class PublisherTests(TestCase):
    def test_streamed_gold_board_keeps_original_snapshot_and_price(self):
        original = full_board(pick(1))
        original['previous_day'] = {'matches': original['matches'] * 4}
        original['matches'][0]['discarded_data'] = ['x' * 1000]
        raw = json.dumps(original).encode()
        client = mock.Mock()
        client.get_object.return_value = {'Body': BytesIO(raw), 'ContentLength': 140_000_000}
        board_view = Storage(client, 'private').gold_board(1)
        self.assertEqual(board_view['source_bytes'], 140_000_000)
        self.assertEqual(len(board_view['matches']), 1)
        self.assertNotIn('previous_day', board_view)
        self.assertNotIn('discarded_data', board_view['matches'][0])
        with mock.patch('publisher_v71.selection.output_allowed', return_value=True):
            self.assertEqual(selection.published_candidates([original], NOW),
                             selection.published_candidates([board_view], NOW))
        self.assertEqual(showcase.proposals(original['matches'][0], NOW),
                         showcase.proposals(board_view['matches'][0], NOW))

    def test_verified_daily_recovery_recaps_real_results_without_learning_marker(self):
        day = date(2026, 11, 11)
        now = datetime.fromisoformat('2026-11-12T10:00:00+00:00')
        audit = certified_day()
        fixture_ids = audit.pop('fixture_labels')
        audit['version'] = report.VERSION
        audit['generated_at'] = selection.utc(now)
        prefix = f'auditarchive/prematch_{day}_'
        source = f'auditordata/prematch_{day}.json'
        outputs = {prefix + suffix: b'{}' for suffix in ('results.json', 'daily.md', 'calibration.json')}
        outputs[prefix + 'daily.json'] = json.dumps({'audit_date': str(day), 'fixtures': [
            {'fixture': {'fixture_id': fid, 'kickoff': audit['fixtures'][fid]['kickoff'], 'match': name}}
            for fid, name in fixture_ids.items()]}).encode()
        files = {source: b'{}', **outputs, prefix + 'v7.json': json.dumps(audit).encode()}
        receipt = {'version': 1, 'day': str(day), 'checked_at': selection.utc(now), 'pending': True,
                   'pending_summary': {'pending_fixtures_count': 9}, 'input_sha256': sha(files[source]),
                   'outputs': {path: sha(raw) for path, raw in outputs.items()}}
        files[prefix + 'checkpoint.json'] = json.dumps(receipt).encode()
        index = {path: {'sha256': sha(raw), 'bytes': len(raw)} for path, raw in files.items()}
        by_hash = {sha(raw): raw for raw in files.values()}
        client = mock.Mock()
        client.get_object.side_effect = lambda **request: {'Body': BytesIO(
            json.dumps({'version': 1, 'files': index}).encode() if request['Key'] == 'state/current.json'
            else by_hash[request['Key'].rsplit('/', 1)[-1]])}
        storage = Storage(client, 'private')
        evidence = report.certified(storage, day, with_fixture_labels=True)
        view = report.build(evidence, [], [])
        self.assertEqual(len(view['triad_examples']), 4)
        self.assertEqual(view['recovered'], {'pending_fixtures': 9, 'pending': True})
        copy = editorial.message('report', view, 'AS71-TEST', 'telegram', now=now, trial_ok=True)
        self.assertIn('partite in verifica 9', copy)
        self.assertIn('/prova', copy)
        self.assertEqual(copy.count('✓'), 4)
        index[prefix + 'results.json'] = {**index[prefix + 'results.json'], 'sha256': 'a' * 64}
        self.assertIsNone(report.certified(storage, day, with_fixture_labels=True))

    def test_gold_agenda_lists_only_today_with_locked_matches_and_trial(self):
        now = datetime.fromisoformat('2026-11-13T11:30:00+00:00')
        today = studio_board(now, 1, 235)
        next_day = studio_board(now, 2, 24 * 60)
        view = studio.agenda([next_day, today, today], now)
        self.assertEqual(view['date'], '2026-11-13')
        self.assertEqual([row['fixture_id'] for row in view['fixtures']], ['1'])
        self.assertNotIn('market', view['fixtures'][0])
        store = LocalStore()
        store.state['runtime']['trial_verified'] = True
        for _ in range(2):
            delivery.queue(store, 'agenda', view['date'], view, ('telegram', 'facebook'), now,
                           now + timedelta(hours=1))
        self.assertEqual(len(store.state['posts']), 2)
        for post in store.state['posts'].values():
            self.assertIn('Le partite di oggi in Gold', post['text'])
            self.assertIn('🔒 Club 1', post['text'])
            self.assertIn('/prova', post['text'])
            self.assertIsNone(delivery.can_send(post, now, {'1': view['fixtures'][0]}))
            self.assertEqual(delivery.can_send(post, now, {}), 'agenda_source_changed')

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

    def test_publisher_sends_studio_to_each_channel_once_with_locks_and_campaign(self):
        now = datetime.fromisoformat('2026-11-12T14:25:00+00:00')
        boards = [studio_board(now, 1, 35, 200), studio_board(now, 2, 95), studio_board(now, 3, 95)]
        snapshot = boards[0]['matches'][0]['multimarket']['matrix']['v7']
        snapshot['triad'] = {'most_probable': snapshot['triad']['most_probable']}
        snapshot['sha256'] = fingerprint({key: value for key, value in snapshot.items() if key != 'sha256'})
        board_today = dict(boards[0], matches=[b['matches'][0] for b in boards])
        store = LocalStore()
        store.state['legacy'] = {'handoff': {'created_at': selection.utc(NOW), 'ids': []}}
        telegram, facebook = mock.Mock(), mock.Mock()
        telegram.send.return_value = {'id': '123', 'at': selection.utc(now)}
        facebook.send.return_value = {'id': '604216843376166_456', 'at': selection.utc(now)}
        services = {'telegram': telegram, 'facebook': facebook}
        with (mock.patch.object(runner, 'clock', return_value=now),
              mock.patch.object(runner, 'read_local', return_value={'publisher': 'v7.1'}),
              mock.patch.object(runner, 'read_board', side_effect=lambda _, n: board_today if n == 1 else {}),
              mock.patch.object(runner, 'publish_home'), mock.patch.object(runner, 'plan_live'),
              mock.patch.object(runner, 'plan_editorial')):
            runner.tick(store, {'telegram': telegram}, '', trial_ok=True)
            runner.tick(store, services, '', trial_ok=True)
            runner.tick(store, services, '', trial_ok=True)
        for service in (telegram, facebook):
            self.assertEqual([call.args[0]['kind'] for call in service.send.call_args_list], ['agenda', 'studio'])
        post, image = telegram.send.call_args_list[-1].args
        self.assertEqual(post['kind'], 'studio')
        self.assertEqual(post['status'], 'sent')
        self.assertEqual(len(post['view']['previews']), 2)
        self.assertEqual(set(post['view']['previews'][0]), set(studio.PREVIEW_FIELDS))
        self.assertNotIn('odd', post['view']['free'])
        self.assertIn('Over 2,5 finale', post['text'])
        self.assertEqual(post['text'].count('🔒'), 2)
        self.assertIn('7 giorni gratis dal 12 al 18/11', post['text'])
        self.assertIn('/prova', post['text'])
        self.assertIn('10%', post['text'])
        self.assertIn('gold-dashboard-live.html', post['text'])
        self.assertLessEqual(len(post['text'].encode('utf-16-le')) // 2, 1024)
        self.assertEqual(Image.open(BytesIO(image)).size, (1080, 1350))
        page_post, page_image = facebook.send.call_args_list[-1].args
        self.assertEqual(page_post['view'], post['view'])
        self.assertEqual(page_post['text'].replace(page_post['marker'], post['marker']), post['text'])
        self.assertEqual(page_image, image)

    def test_studio_windows_and_delivery_recheck(self):
        now = datetime.fromisoformat('2026-11-12T14:45:00+00:00')
        boards = [studio_board(now, 1, 35), studio_board(now, 2, 45), studio_board(now, 3, 140)]
        pool = studio.candidates(boards, now)
        key, view = studio.group_for(pool, [], set(), now)
        store = LocalStore()
        until = datetime.fromisoformat(view['free']['kickoff']) - selection.LEAD
        delivery.queue(store, 'studio', key, view, ['telegram'], now, until)
        post = next(iter(store.state['posts'].values()))
        self.assertIsNone(studio.group_for(pool, [post], set(), now))
        self.assertIsNone(delivery.can_send(post, now, pool))
        self.assertEqual(delivery.can_send(post, now, {**pool, '1': dict(pool['1'], market='Changed')}),
                         'studio_source_changed')
        self.assertEqual(delivery.can_send(post, until, pool), 'expired')
        later = now + timedelta(minutes=85)
        self.assertEqual(studio.group_for(studio.candidates(boards, later), [post], set(), later)[0],
                         '2026-11-12/18/3')

    def test_best_footer_tracks_trial_health_and_campaign_dates_on_both_channels(self):
        view = {'free': selection.project_pick(pick(1), True), 'previews': []}
        active = datetime.fromisoformat('2026-11-12T16:00:00+00:00')
        later = active + timedelta(days=7)
        for channel in ('telegram', 'facebook'):
            for when, trial_ok, discount in [(active, True, True), (active, False, False), (later, True, False)]:
                text = editorial.message('best', view, 'AS71-TEST', channel, now=when, trial_ok=trial_ok)
                self.assertIn('/prova', text)
                self.assertIn('gold-dashboard-live.html', text)
                self.assertEqual('10%' in text, discount)

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

    def test_live_updates_first_half_and_final_from_fresh_scores(self):
        match = pick(1)
        kickoff = datetime.fromisoformat(match['kickoff'])
        state = live.start_state(match, None, NOW)
        for offset, status, goals, halftime, fulltime, expected in (
            (35, '1H', (0, 1), None, None, 'Primo tempo'),
            (50, 'HT', (0, 1), (0, 1), None, 'Intervallo'),
            (60, '2H', (0, 1), (0, 1), None, 'Secondo tempo'),
            (110, 'FT', (1, 2), (0, 1), (0, 1), 'Finale'),
        ):
            now = kickoff + timedelta(minutes=offset)
            fixture = {'fixture_id': '1', 'source': 'API-Football', 'observed_at': selection.utc(now),
                       'refresh_failed': False, 'status': status, 'goals': dict(zip(('home', 'away'), goals))}
            if halftime:
                fixture['halftime'] = dict(zip(('home', 'away'), halftime))
            if fulltime:
                fixture['fulltime'] = dict(zip(('home', 'away'), fulltime))
            feed = {'schema_version': 1, 'enabled': True, 'fixtures': {'1': fixture}}
            view = live.advance(state, feed, now, {})
            self.assertEqual((view['status'], view['home'], view['away']), (expected, 0, 1))
            self.assertEqual(view.get('halftime'), list(halftime) if halftime else None)
            state['view'] = view
        self.assertTrue(state['done'])
        self.assertIn('Parziale all’intervallo · 0–1', editorial.message('live', state['view'], 'AS71-TEST', 'telegram'))

    def test_live_score_corrections_require_two_new_observations(self):
        match = pick(1)
        now = datetime.fromisoformat(match['kickoff']) + timedelta(minutes=30)
        state = live.start_state(match, None, NOW)
        fixture = {'fixture_id': '1', 'source': 'API-Football', 'observed_at': selection.utc(now),
                   'refresh_failed': False, 'status': '1H', 'goals': {'home': 0, 'away': 1}}
        feed = {'schema_version': 1, 'enabled': True, 'fixtures': {'1': fixture}}
        state['view'] = live.advance(state, feed, now, {})
        first = now + timedelta(minutes=1)
        fixture.update(goals={'home': 0, 'away': 0}, observed_at=selection.utc(first))
        self.assertIsNone(live.advance(state, feed, first, {}))
        self.assertIsNone(live.advance(state, feed, first + timedelta(seconds=60), {}))
        second = first + timedelta(seconds=60)
        fixture['observed_at'] = selection.utc(second)
        self.assertEqual(live.advance(state, feed, second, {})['away'], 0)

    def test_live_monitor_edits_original_message_for_goals_and_both_penalty_outcomes(self):
        match = pick(1)
        kickoff = datetime.fromisoformat(match['kickoff'])
        day = selection.rome_day(kickoff)
        store = LocalStore()
        state = store.state['live'][day] = live.start_state(match, None, NOW)
        start = f'telegram/live/{day}/start'
        delivery.queue(store, 'live', day + '/start', state['view'], ['telegram'], NOW, kickoff)
        store.state['posts'][start].update(status='sent', receipt={'id': '181', 'at': selection.utc(NOW)})
        goal = event('Normal Goal', 7, 20)
        missed = event('Missed Penalty', 13, 10)
        penalty = event('Penalty', 45, 20, extra=2)
        shootout = event('Missed Penalty', 120, 10, comments='Penalty Shootout')
        telegram = Telegram('fake-token')
        reads = []
        for offset, phase, score, details, expected in (
            (7, '1H', (0, 1), [goal], 'goal'),
            (8, '1H', (0, 1), [goal], None),
            (13, '1H', (0, 1), [goal, missed, missed], 'missed'),
            (14, '1H', (0, 1), [goal, missed], None),
            (47, '1H', (0, 2), [goal, missed, penalty], 'penalty'),
            (49, 'HT', (0, 2), [goal, missed, penalty], 'photo'),
            (100, 'FT', (0, 2), [goal, missed, penalty, shootout], 'photo'),
        ):
            with self.subTest(phase=phase, offset=offset):
                now = kickoff + timedelta(minutes=offset)
                data = fixture(phase, score, details,
                               halftime=(0, 2) if phase in ('HT', 'FT') else None,
                               fulltime=(0, 2) if phase == 'FT' else None)
                receipt = {'chat': {'username': 'arabsniper_signals'}, 'message_id': 181,
                           'date': int(NOW.timestamp()), 'edit_date': int(now.timestamp())}

                def provider(path, key, params=None):
                    reads.append((path, params))
                    if path == 'status':
                        return {'subscription': {'active': True},
                                'requests': {'current': 0, 'limit_day': 10000}}, {}
                    return [data], {'x-ratelimit-requests-remaining': '9000'}

                with (mock.patch.object(store, 'cycle', return_value=nullcontext(True)),
                      mock.patch.object(runner, 'clock', return_value=now),
                      mock.patch.object(scores, 'provider', side_effect=provider),
                      mock.patch.object(telegram, 'call', return_value=receipt) as api,
                      mock.patch.dict(os.environ, {'API_SPORTS_KEY': 'fake'})):
                    result = monitor.poll(store, {'telegram': telegram}, '1')
                self.assertEqual(result, 'finished' if phase == 'FT' else 'polled')
                if not expected:
                    api.assert_not_called()
                    continue
                api.assert_called_once()
                method, params = api.call_args.args
                media = json.loads(params['media'])
                self.assertEqual((method, params['message_id']), ('editMessageMedia', '181'))
                self.assertIn(f'{score[0]}–{score[1]}', media['caption'])
                name, content, mime = api.call_args.kwargs['files']['live_card']
                if expected != 'photo':
                    self.assertEqual((name, mime, media['type']), (expected + '.gif', 'image/gif', 'animation'))
                    image = Image.open(BytesIO(content))
                    self.assertEqual((image.format, image.size), ('GIF', (640, 640)))
                    self.assertTrue(image.is_animated)
                if expected == 'missed':
                    self.assertIn('RIGORE SBAGLIATO · Casa · 13′', media['caption'])
                if expected == 'penalty':
                    self.assertIn('RIGORE SEGNATO! · Ospiti · 45+2′', media['caption'])
        self.assertEqual(reads, [('status', None)] + [('fixtures', {'id': '1'})] * 7)
        self.assertEqual(state['view']['status'], 'Finale')
        self.assertEqual(state['view']['halftime'], [0, 2])
        self.assertEqual([e['kind'] for e in state['view']['events']], ['goal', 'missed', 'penalty'])
        self.assertNotIn('animation', state['view'])
        self.assertNotIn('events', public_feed.projection(store.state, now)['live'])

    def test_late_explicit_penalty_replaces_generic_goal_without_duplicates(self):
        match = pick(1)
        kickoff = datetime.fromisoformat(match['kickoff'])
        state = live.start_state(match, None, NOW)
        for offset, details, expected in (
            (11, [], 'goal'),
            (12, [event('Penalty', 10, 10)], 'penalty'),
            (13, [event('Penalty', 10, 10)], None),
        ):
            now = kickoff + timedelta(minutes=offset)
            row = fixture('1H', (1, 0), details)
            feed = scores.feed({'1': scores.fixture_view(row, selection.utc(now), with_events=True)})
            view = live.advance(state, feed, now, {})
            if expected:
                self.assertEqual((view['animation'], [e['kind'] for e in view['events']]), (expected, [expected]))
                state['view'] = view
            else:
                self.assertIsNone(view)

    def test_animation_rejection_falls_back_to_current_score_card(self):
        telegram = Telegram('fake')
        post = {'kind': 'live', 'edit_id': '181', 'text': 'RIGORE SBAGLIATO · 0–1',
                'view': {'animation': 'missed', 'home': 0, 'away': 1, 'status': 'Primo tempo'}}
        with (mock.patch('telegram_live_art.animation', return_value=b'GIF89a') as art,
              mock.patch.object(telegram, 'call', side_effect=[
                  DeliveryError('rejected', 'telegram_http_400'), None]) as api):
            self.assertEqual(telegram.send(post, b'score-card')['id'], '181')
        art.assert_called_once_with('missed', '0–1 · PRIMO TEMPO')
        self.assertEqual([json.loads(call.args[1]['media'])['type'] for call in api.call_args_list],
                         ['animation', 'photo'])
        self.assertEqual(api.call_args.kwargs['files']['live_card'][1], b'score-card')

    def test_event_quota_reserve_stops_match_requests(self):
        store = LocalStore()
        quota = {'subscription': {'active': True}, 'requests': {'current': 9000, 'limit_day': 10000}}
        with (mock.patch.object(scores, 'provider', return_value=(quota, {})) as provider,
              mock.patch.dict(os.environ, {'API_SPORTS_KEY': 'fake'})):
            for _ in range(2):
                self.assertEqual(scores.detailed(store, '1', NOW, lambda: NOW, mock.Mock()), {})
        provider.assert_called_once_with('status', 'fake')

    def test_publisher_cycle_respects_a_lease_and_releases_only_its_own(self):
        client = mock.Mock()
        store = Storage(client, 'bucket')
        with mock.patch.object(store, 'read', return_value=({'until': selection.utc(NOW)}, '"old"')):
            client.put_object.return_value = {'ETag': '"ours"'}
            with store.cycle(NOW) as acquired:
                self.assertTrue(acquired)
                client.put_object.assert_called_once()
        self.assertEqual([c.kwargs['IfMatch'] for c in client.put_object.call_args_list], ['"old"', '"ours"'])
        client.reset_mock()
        with mock.patch.object(store, 'read', return_value=(
                {'until': selection.utc(NOW + timedelta(minutes=1))}, '"ours"')):
            with store.cycle(NOW) as acquired:
                self.assertFalse(acquired)
        client.put_object.assert_not_called()

    def test_old_final_gets_halftime_as_an_edit_of_the_original_message(self):
        match = pick(1)
        kickoff = datetime.fromisoformat(match['kickoff'])
        now = kickoff + timedelta(minutes=115)
        day = selection.rome_day(kickoff)
        store = LocalStore()
        state = live.start_state(match, None, NOW)
        state.update(done=True, view=live.status_view(match, 'Finale', now, [0, 1]))
        store.state['live'][day] = state
        start_id = f'telegram/live/{day}/start'
        store.state['posts'][start_id] = {'status': 'sent', 'receipt': {'id': '181'}}
        fixture = {'fixture_id': '1', 'source': 'API-Football', 'observed_at': selection.utc(now),
                   'refresh_failed': False, 'status': 'FT',
                   'fulltime': {'home': 0, 'away': 1}, 'halftime': {'home': 0, 'away': 1}}
        feed = {'schema_version': 1, 'enabled': True, 'fixtures': {'1': fixture}}
        with (mock.patch.object(runner.scores, 'read', return_value=feed) as cache,
              mock.patch.object(runner, 'read_local', return_value={}),
              mock.patch.object(runner, 'clock', return_value=now)):
            runner.plan_live(store, now, {}, ('telegram', 'facebook'))
            runner.plan_live(store, now, {}, ('telegram', 'facebook'))
        cache.assert_called_once()
        edits = [post for key, post in store.state['posts'].items() if key != start_id]
        self.assertEqual(len(edits), 1)
        edit = edits[0]
        self.assertEqual((edit['channel'], edit['edit_id']), ('telegram', '181'))
        self.assertIn('Parziale all’intervallo · 0–1', edit['text'])
        service = Telegram('mock-token')
        receipt = {'chat': {'username': 'arabsniper_signals'}, 'message_id': 181,
                   'date': int(now.timestamp()), 'edit_date': int(now.timestamp())}
        with mock.patch.object(service, 'call', return_value=receipt) as api:
            delivery.attempt(store, edit, service, {}, lambda: now, mock.Mock())
        method, target = api.call_args.args
        self.assertEqual((method, target['message_id']), ('editMessageMedia', '181'))
        self.assertEqual(json.loads(target['media'])['caption'], edit['text'])
        self.assertEqual(Image.open(BytesIO(api.call_args.kwargs['files']['live_card'][1])).size, (1080, 1350))
        self.assertEqual(edit['receipt']['id'], '181')

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
