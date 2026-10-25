"""One bounded-memory audit pass after a Fast or Night fixture scan."""
from audit_ledger import AuditLedger, capture_ledger, settle_ledger
from task_progress import TaskProgress


def update_audit(ns, output, client):
    from prematch_audit import build_report
    from multimarket_audit import build_report as multimarket_report
    with TaskProgress('Archivio scan') as progress:
        with AuditLedger(ns['PREMATCH_AUDIT_FILE'], progress=progress.update) as ledger:
            ledger.metadata.setdefault('schema_version', 1)
            progress.update('registrazione previsioni: una partita alla volta')
            capture_ledger(ledger, output, ns['capture_prematch_predictions'], ns['now_rome']())
            settle_ledger(ledger, output)
            pending_dates = set()
            today = ns['now_rome']().date()
            for _, _, record in ledger.records():
                kickoff = ns['parse_iso'](record.get('fixture', {}).get('kickoff'))
                if kickoff and kickoff.date() < today and (today-kickoff.date()).days <= 14:
                    outcomes = record.get('settlement', {}).get('outcomes', {})
                    if not outcomes or any(v is None for v in outcomes.values()):
                        pending_dates.add(kickoff.date().isoformat())
            for date in sorted(pending_dates):
                progress.update(f'risultati API-Football {date}')
                payload = client.get('fixtures', {'date': date, 'timezone': 'Europe/Rome'}, use_cache=True)
                matches = []
                for row in (payload or {}).get('response', []):
                    result = ns['_result_payload'](row)
                    if result:
                        matches.append({'fixture': {'fixture_id': str(row.get('fixture', {}).get('id')),
                                                    'result': result}})
                settle_ledger(ledger, {'days': [{'matches': matches}]})
            progress.update('scrittura archivio verificabile')
            ledger.write(ns['PREMATCH_AUDIT_FILE'])
            progress.update('rapporto prepartita')
            ns['atomic_write_json'](ns['DATA_DIR']/'arab_quote_prematch_report.json', build_report(ledger))
            progress.update('rapporto multimercato')
            research = multimarket_report(ledger)
            ns['atomic_write_json'](ns['DATA_DIR']/'arab_quote_multimarket_report.json', research)
            return research
