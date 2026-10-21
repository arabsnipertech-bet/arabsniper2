"""Keep an auditable 30-day run history, not an estimated account total."""
import json, os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from audit_storage import read_store, atomic_bytes

if __name__ == '__main__':
    dashboard=read_store(Path('data/arab_quote_dashboard.json'))
    state=read_store(Path('data/arab_quote_run_state.json'),{})
    target=Path('data/arab_quote_usage_history.json')
    history=read_store(target,{'runs':[]})
    row={'run_id':os.getenv('GITHUB_RUN_ID',dashboard['generated_at']),
         'generated_at':dashboard['generated_at'],'mode':state.get('mode'),
         'days':len(dashboard.get('days',[])),**dashboard.get('api_health',{})}
    # Keep only non-sensitive usage counters, not response bodies/error details.
    row.pop('last_error',None)
    cutoff=(datetime.now(timezone.utc)-timedelta(days=30)).isoformat()
    rows={str(x['run_id']):x for x in history['runs'] if x['generated_at']>=cutoff}
    rows[str(row['run_id'])]=row
    atomic_bytes(target,json.dumps({'schema_version':1,'note':'Per-run counters; not provider daily billing totals. Includes retries, excludes live collector and auditor.','runs':list(rows.values())},separators=(',',':')).encode()+b'\n')
