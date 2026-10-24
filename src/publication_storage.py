"""Pack legacy scan outputs and reject oversized publication files before git push."""
from pathlib import Path
import faulthandler
import json
import subprocess
import sys
import time
from audit_storage import FORMAT, read_store, write_store


def prepare_file(path):
    """Keep each file's decoded objects inside a short-lived worker process."""
    path = Path(path)
    started = time.monotonic()
    print(f'[publication] Reading {path.name} ({path.stat().st_size} bytes on disk)', flush=True)
    # Manifests are small; large plain JSON must not be decoded twice here.
    manifest = None
    if path.stat().st_size < 1024 * 1024:
        header = json.loads(path.read_bytes())
        if isinstance(header, dict) and header.get('_storage') == FORMAT:
            manifest = header
    if manifest is not None:
        print(f'[publication] Verifying existing parts: {path.name}, {manifest["bytes"]} decoded bytes', flush=True)
        # read_store verifies every part, the complete digest and JSON parsing.
        # Recompressing these immutable parts adds no validation or information.
        read_store(path)
    else:
        value = read_store(path)
        print(f'[publication] Packing {path.name}', flush=True)
        write_store(path, value)
        print(f'[publication] Checking lossless round trip: {path.name}', flush=True)
        if read_store(path) != value:
            raise ValueError(f'Lossless storage verification failed: {path}')
    print(f'[publication] Verified {path.name} in {time.monotonic()-started:.1f}s', flush=True)


def prepare(directory):
    directory = Path(directory)
    from homepage_preview import publish
    print('[publication] Building homepage preview', flush=True)
    publish(directory)
    for path in sorted(directory.glob('arab_quote_*.json')):
        # Process exit releases Python's allocator arenas before the next file.
        try:
            subprocess.run([sys.executable, '-u', str(Path(__file__).resolve()),
                            '--file', str(path.resolve())], check=True, timeout=600)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f'Publication verification exceeded 600s: {path.name}') from exc
    files = list(directory.glob('arab_quote_*.json')) + list((directory / '_audit_parts').glob('*'))
    oversized = [str(p) for p in files if p.stat().st_size >= 20 * 1024 * 1024]
    if oversized:
        raise ValueError('Publication files exceed safe limit: ' + ', '.join(oversized))
    print(f'Verified {len(files)} publication files: all below 20 MiB')


if __name__ == '__main__':
    faulthandler.enable()
    faulthandler.dump_traceback_later(60, repeat=True)
    try:
        if len(sys.argv) == 3 and sys.argv[1] == '--file':
            prepare_file(sys.argv[2])
        else:
            prepare(sys.argv[1] if len(sys.argv) > 1 else 'data')
    finally:
        faulthandler.cancel_dump_traceback_later()


