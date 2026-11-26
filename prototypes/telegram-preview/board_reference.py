import json
import os
from pathlib import Path
from publisher_v71.storage import Storage

root = Path(os.environ['SHADOW_INPUT'])
class CapturedR2:
    def get_object(self, *, Bucket, Key):
        path = root / Key
        return {'Body':path.open('rb'), 'ContentLength':path.stat().st_size}

for i in range(1, 6):
    source = Storage(CapturedR2(), 'offline').gold_board(i)
    source.pop('source_bytes')
    (root / f'ref{i}.json').write_text(json.dumps(source, allow_nan=False))
