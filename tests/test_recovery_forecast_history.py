"""The recovery index must preserve every original merge and update label."""
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from audit_journal import ForecastHistory, merge_forecast
from audit_ledger import AuditLedger
from audit_storage import read_store


def observation(fid, when, market='over25_ft', model='lateral-matrix-v5.0'):
    return {'fixture': {'fixture_id': fid, 'kickoff': '2026-11-07T23:00:00+01:00'},
            'captured_at': when, 'model_version': model,
            'multimarket': {'best_candidate': {'market': market, 'selection': 'over'},
                           'markets': [{'odd': 2.15, 'probability': .51}]},
            'untouched': {'unicode': 'è 🟡', 'numbers': [0, -1.2, 2**53+1]}}


class RecoveryForecastHistoryTests(unittest.TestCase):
    def test_full_archive_parity_with_out_of_order_ties_models_and_duplicate_keys(self):
        start = datetime(2026, 11, 7, 8, tzinfo=timezone.utc)
        rng = random.Random(7389)
        seed = [observation(str(i % 3), (start+timedelta(minutes=i)).isoformat())
                for i in range(15)]
        incoming = []
        for i in range(150):
            at = start+timedelta(minutes=rng.randrange(1, 45))
            if i % 2: at = at.astimezone(timezone(timedelta(hours=1)))
            incoming.append(('row-'+str(rng.randrange(80)), observation(
                str(rng.randrange(4)), at.isoformat(),
                market='over25_ft' if i % 3 else 'over15_ht',
                model='v-other' if i % 7 == 0 else 'lateral-matrix-v5.0')))
        with AuditLedger() as legacy, AuditLedger() as indexed, TemporaryDirectory() as folder:
            for ledger in (legacy, indexed):
                for i, record in enumerate(seed):
                    ledger.put('analysis_updates', 'seed-'+str(i), copy.deepcopy(record))
                ledger.put('fixtures', 'other', observation('0', start.isoformat()))
            history = ForecastHistory(indexed)
            for key, record in incoming:
                merge_forecast(legacy, 'analysis_updates', key, copy.deepcopy(record))
                merge_forecast(indexed, 'analysis_updates', key, copy.deepcopy(record), history=history)
                self.assertEqual(list(legacy.records()), list(indexed.records()))
            a, b = Path(folder)/'legacy.json', Path(folder)/'indexed.json'
            legacy.write(a); indexed.write(b)
            self.assertEqual(a.read_bytes(), b.read_bytes())

    def test_equal_instants_use_first_inserted_predecessor_and_strictly_earlier_time(self):
        with AuditLedger() as ledger:
            ledger.put('analysis_updates', 'tie-first', observation('1', '2026-11-07T10:00:00Z'))
            ledger.put('analysis_updates', 'tie-second', observation('1', '2026-11-07T11:00:00+01:00', 'over15_ht'))
            history = ForecastHistory(ledger)
            same = observation('1', '2026-11-07T10:00:00+00:00', 'over15_ht')
            merge_forecast(ledger, 'analysis_updates', 'equal', same, history=history)
            self.assertEqual(same['update_reason'], 'prima_analisi')
            later = observation('1', '2026-11-07T10:00:00.000001Z')
            merge_forecast(ledger, 'analysis_updates', 'later', later, history=history)
            self.assertEqual(later['update_reason'], 'aggiornamento_dati_e_quote')

    def test_index_avoids_redecoding_preceding_full_payloads(self):
        with AuditLedger() as ledger:
            for i in range(100):
                row = observation('1', f'2026-11-07T10:00:{i%60:02d}Z')
                row['padding'] = 'x'*10000
                ledger.put('analysis_updates', str(i), row)
            history = ForecastHistory(ledger)
            with patch.object(ledger, 'records', side_effect=AssertionError('full history scan')):
                row = observation('1', '2026-11-07T12:00:00Z')
                merge_forecast(ledger, 'analysis_updates', 'new', row, history=history)
            self.assertEqual(row['update_reason'], 'aggiornamento_dati_e_quote')

    def test_unsupported_legacy_metadata_retains_original_validation_behavior(self):
        for damaged in ('missing_date', 'naive_date', 'missing_fixture', 'model_type'):
            with self.subTest(damaged=damaged), AuditLedger() as legacy, AuditLedger() as indexed:
                first = observation('1', '2026-11-07T10:00:00Z')
                last = observation('1', '2026-11-07T11:00:00Z')
                if damaged == 'missing_date': first.pop('captured_at')
                elif damaged == 'naive_date': first['captured_at'] = '2026-11-07T10:00:00'
                elif damaged == 'missing_fixture': first['fixture'].pop('fixture_id'); last['fixture'].pop('fixture_id')
                else: first['model_version'] = last['model_version'] = None
                for ledger in (legacy, indexed):
                    ledger.put('analysis_updates', 'original', copy.deepcopy(first))
                history = ForecastHistory(indexed)
                errors = []
                for ledger, fast in ((legacy, None), (indexed, history)):
                    try:
                        merge_forecast(ledger, 'analysis_updates', 'next', copy.deepcopy(last), history=fast)
                    except Exception as error:
                        errors.append(type(error))
                    else:
                        errors.append(None)
                self.assertEqual(errors[0], errors[1])
                self.assertEqual(list(legacy.records()), list(indexed.records()))


if __name__ == '__main__': unittest.main()
