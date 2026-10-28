from copy import deepcopy
import io
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'auditor'))
from published_gold_export import collect_published


class PublishedFeed:
    def __init__(self):
        self.calls = []
        self.match = {'fixture': {'fixture_id': 12, 'kickoff': '2026-10-28T19:00:00Z', 'match': 'Synthetic fixture'},
            'gold_scope': {'allowed': True}, 'multimarket': {'model_id': 'lateral-matrix-v5.0', 'matrix': {
                'rows': [{'id': 'goal', 'spec': {'kind': 'total', 'period': 'ft', 'side': 'over', 'line': 2.5},
                    'opportunity': {'eligible': False, 'rank': None}, 'statistical_pct': 43, 'odd': 2.5}]}}}
        self.data = {'matches': [self.match], 'generated_at': '2026-10-28T10:00:00Z', 'gold_scope': {'policy_id': 'published'}}

    def get_object(self, Bucket, Key):
        self.calls.append(Key)
        if Key not in [f'arab_quote_day{i}.json' for i in range(1, 6)]:
            raise AssertionError('Only read the published Gold feeds')
        return {'Body': io.BytesIO(json.dumps(self.data).encode())}

    def put_object(self, **kwargs): raise AssertionError('No publication writes')
    def delete_object(self, **kwargs): raise AssertionError('No deletes')


class PublishedGoldExport(unittest.TestCase):
    def test_exact_served_feeds_no_expiry_or_policy_recalculation(self):
        source = PublishedFeed(); before = deepcopy(source.data)
        result = collect_published(source, 'test', '2026-10-28', '2026-10-28T13:00:00+01:00')
        self.assertEqual(len(source.calls), 5)
        self.assertEqual(len(result['payload']['matches']), 1)
        self.assertEqual(result['payload']['matches'][0]['multimarket']['matrix']['rows'], source.match['multimarket']['matrix']['rows'])
        self.assertEqual(source.data, before)
        self.assertEqual(result['provider_calls'], 0)
        self.assertEqual(result['checkpoint_writes'], 0)
        self.assertEqual(len(result['sources']), 5)

    def test_rome_date_and_invalid_dates(self):
        source = PublishedFeed()
        source.match['fixture']['kickoff'] = '2026-10-27T23:30:00Z'
        result = collect_published(source, 'test', '2026-10-28', '2026-10-28T13:00:00+01:00')
        self.assertEqual(len(result['payload']['matches']), 1)
        self.assertEqual(collect_published(source, 'test', '2026-10-27', '2026-10-28T13:00:00+01:00')['payload']['matches'], [])
        with self.assertRaises(ValueError): collect_published(source, 'test', '2026-13-44', '')
