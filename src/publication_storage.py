"""Pack legacy scan outputs and reject oversized publication files before git push."""
from pathlib import Path
from audit_storage import read_store, write_store


def prepare(directory):
    directory = Path(directory)
    from homepage_preview import publish
    publish(directory)
    for path in sorted(directory.glob('arab_quote_*.json')):
        value = read_store(path)
        write_store(path, value)
        if read_store(path) != value:
            raise ValueError(f'Lossless storage verification failed: {path}')
    files = list(directory.glob('arab_quote_*.json')) + list((directory / '_audit_parts').glob('*'))
    oversized = [str(p) for p in files if p.stat().st_size >= 20 * 1024 * 1024]
    if oversized:
        raise ValueError('Publication files exceed safe limit: ' + ', '.join(oversized))
    print(f'Verified {len(files)} publication files: all below 20 MiB')


if __name__ == '__main__':
    import sys
    prepare(sys.argv[1] if len(sys.argv) > 1 else 'data')

