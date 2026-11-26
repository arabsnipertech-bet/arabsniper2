from datetime import datetime, timezone
from io import BytesIO
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

sys.path.insert(0,str(Path(__file__).parents[1]/'src'))
from audit_official_scores import LIVE_URL, MAX_CACHE_BYTES, cached_feed, read_live_scores


class CachedScoreTransport(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,11,26,7,tzinfo=timezone.utc)
        self.row=dict(fixture_id='7',source='API-Football',status='FT',
            provider_kickoff='2026-11-25T19:00:00Z',observed_at='2026-11-25T21:00:00Z',
            fulltime=dict(home=2,away=1),halftime=dict(home=0,away=0))

    def test_happy_path_never_starts_another_transport(self):
        with patch('audit_official_scores.urlopen',return_value=BytesIO(b'{"schema_version":1}')), \
             patch('audit_official_scores.subprocess.run') as run:
            self.assertEqual(cached_feed(),dict(schema_version=1))
            run.assert_not_called()

    def test_route_failure_uses_only_the_same_cache_and_preserves_zero_halftime(self):
        body=json.dumps(dict(schema_version=1,enabled=True,fixtures={'7':self.row})).encode()
        with patch('audit_official_scores.urlopen',side_effect=HTTPError(LIVE_URL,403,'unavailable',{},None)), \
             patch('audit_official_scores.subprocess.run',return_value=Mock(stdout=body)) as run:
            score=read_live_scores({'7':self.row['provider_kickoff']},self.now)['7']
        self.assertEqual((score['home_ht'],score['away_ht']),(0,0))
        args=run.call_args.args[0]
        self.assertEqual(args[-1],LIVE_URL)
        self.assertNotIn('/refresh',args[-1])
        self.assertTrue(run.call_args.kwargs['check'])
        self.assertEqual(run.call_args.kwargs['timeout'],15)

    def test_fallback_cannot_create_a_score_or_swallow_missing_halftime(self):
        row=dict(self.row,halftime={})
        body=json.dumps(dict(schema_version=1,enabled=True,fixtures={'7':row})).encode()
        with patch('audit_official_scores.urlopen',side_effect=OSError), \
             patch('audit_official_scores.subprocess.run',return_value=Mock(stdout=body)):
            score=read_live_scores({'7':row['provider_kickoff']},self.now)['7']
        self.assertIsNone(score['home_ht'])
        self.assertIsNone(score['away_ht'])
        self.assertEqual(score['home_ft'],2)

    def test_failed_or_oversized_fallback_returns_no_manufactured_result(self):
        for result in [Mock(stdout=b'x'*(MAX_CACHE_BYTES+1)),subprocess.TimeoutExpired('curl',15)]:
            with self.subTest(result=type(result).__name__), \
                 patch('audit_official_scores.urlopen',side_effect=OSError), \
                 patch('audit_official_scores.subprocess.run',side_effect=result if isinstance(result,Exception) else None,
                       return_value=result):
                self.assertEqual(read_live_scores({'7':self.row['provider_kickoff']},self.now),{})

    def test_fallback_does_not_accept_future_or_different_fixture_evidence(self):
        for changes in [dict(observed_at='2026-11-26T08:00:00Z'),dict(fixture_id='8'),dict(source='unknown')]:
            body=json.dumps(dict(schema_version=1,enabled=True,fixtures={'7':dict(self.row,**changes)})).encode()
            with self.subTest(changes=changes),patch('audit_official_scores.urlopen',side_effect=OSError), \
                 patch('audit_official_scores.subprocess.run',return_value=Mock(stdout=body)):
                self.assertEqual(read_live_scores({'7':self.row['provider_kickoff']},self.now),{})


if __name__=='__main__':unittest.main()
