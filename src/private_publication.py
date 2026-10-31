"""Publish only explicit public projections after the R2 checkpoint succeeds."""
import subprocess
import time
from pathlib import Path
from gold_storage import PUBLIC, allowed

PUBLIC_FILES = PUBLIC | {'data/live_watchlist.json', 'data/arab_bet_catalog.json'}


CALIBRATION_FILE = 'data/arab_calibration_weights.json'


def git(root, *args):
    return subprocess.check_output(['git', *args], cwd=root)


def stage_public(root, snapshot):
    root = Path(root)
    tracked = git(root, 'ls-files', '-z').decode().split('\0')
    private = [path for path in tracked if path and allowed(path)]
    if private:
        git(root, 'rm', '-f', '--ignore-unmatch', '--', *private)
    for path, raw in snapshot.items():
        if path not in PUBLIC_FILES | {CALIBRATION_FILE}:
            raise ValueError('Not a public projection')
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        git(root, 'add', '--', path)
    changed = git(root, 'diff', '--cached', '--name-only', '--diff-filter=ACMR').decode().splitlines()
    if any(path not in snapshot for path in changed):
        raise ValueError('Unexpected staged file; refusing publication')


def publish(root, paths=PUBLIC_FILES):
    root = Path(root)
    if not set(paths) <= PUBLIC_FILES and set(paths) != {CALIBRATION_FILE}:
        raise ValueError('Unexpected public projection')
    snapshot = {p: (root / p).read_bytes() for p in paths if (root / p).is_file()}
    if CALIBRATION_FILE in paths and CALIBRATION_FILE not in snapshot:
        raise FileNotFoundError(CALIBRATION_FILE)
    git(root, 'config', 'user.name', 'github-actions[bot]')
    git(root, 'config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com')
    for attempt in range(3):
        git(root, 'fetch', 'origin', 'main')
        git(root, 'reset', '--hard', 'origin/main')
        stage_public(root, snapshot)
        if not git(root, 'diff', '--cached', '--name-only').strip():
            return
        git(root, 'commit', '-m', 'Publish public projections; keep Gold in private storage')
        try:
            git(root, 'push', 'origin', 'HEAD:main')
            return
        except subprocess.CalledProcessError:
            if attempt == 2:
                raise
            time.sleep(2 * (attempt + 1))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--results-only', action='store_true')
    args = parser.parse_args()
    publish(Path(__file__).resolve().parents[1], {'data/arab_quote_results_public.json'} if args.results_only else PUBLIC_FILES)
