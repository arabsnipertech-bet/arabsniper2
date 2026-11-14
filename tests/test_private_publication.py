import subprocess
import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from private_publication import stage_public, git, publish_watchlist, WATCHLIST


class PublicBoundary(unittest.TestCase):
    def test_removes_tracked_private_data_and_stages_only_public_projection(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            git(root, 'init', '-q')
            git(root, 'config', 'user.email', 'test@example.com')
            git(root, 'config', 'user.name', 'Test')
            files = ['data/arab_quote_day1.json', 'data/arab_quote_state.json',
                     'auditordata/prematch_2026-10-23.json',
                     'data/_audit_parts/' + 'a' * 64 + '.json.gz',
                     'web/index.html', 'data/arab_quote_homepage.json']
            for path in files:
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text('{}')
            git(root, 'add', '.')
            git(root, 'commit', '-qm', 'initial')
            # An untracked private file must never be picked up by publication.
            (root / 'data/arab_quote_day2.json').write_text('secret')
            stage_public(root, {'data/arab_quote_homepage.json': b'{"public":true}'})
            added = git(root, 'diff', '--cached', '--name-only', '--diff-filter=ACMR').decode().splitlines()
            deleted = git(root, 'diff', '--cached', '--name-only', '--diff-filter=D').decode().splitlines()
            self.assertEqual(added, ['data/arab_quote_homepage.json'])
            self.assertEqual(set(deleted), set(files[:4]))
            self.assertTrue((root / 'web/index.html').exists())

    def test_refuses_accidentally_staged_private_change(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            git(root, 'init', '-q')
            (root / 'unexpected.txt').write_text('private')
            git(root, 'add', '.')
            with self.assertRaises(ValueError):
                stage_public(root, {})

    def test_watchlist_handoff_preserves_scan_checkout_and_only_updates_the_projection(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / 'scan'
            remote = Path(folder) / 'origin.git'
            remote.mkdir()
            git(remote, 'init', '--bare', '-q')
            root.mkdir()
            git(root, 'init', '-q', '-b', 'main')
            git(root, 'config', 'user.email', 'test@example.com')
            git(root, 'config', 'user.name', 'Test')
            (root / WATCHLIST).parent.mkdir()
            (root / WATCHLIST).write_text('{"generated_at":"2026-11-14T09:00:00+00:00","fixtures":[]}')
            (root / 'README.md').write_text('baseline')
            git(root, 'add', '.')
            git(root, 'commit', '-qm', 'initial')
            git(root, 'remote', 'add', 'origin', str(remote))
            git(root, 'push', '-q', 'origin', 'main')
            base = git(root, 'rev-parse', 'HEAD')
            watch = ('{"generated_at":"2026-11-14T10:00:00+00:00",'
                     '"fixtures":[{"fixture_id":"10","kickoff":"2026-11-14T13:00:00+01:00"}]}')
            (root / WATCHLIST).write_text(watch)
            (root / 'README.md').write_text('unpublished edit')
            private = root / 'data/arab_quote_day1.json'
            private.write_text('private Gold data')

            publish_watchlist(root)

            self.assertEqual(git(root, 'rev-parse', 'HEAD'), base)
            self.assertEqual((root / 'README.md').read_text(), 'unpublished edit')
            self.assertEqual(private.read_text(), 'private Gold data')
            self.assertEqual(git(remote, 'show', 'main:' + WATCHLIST).decode(), watch)
            self.assertEqual(git(remote, 'diff', '--name-only', 'main^', 'main').decode().splitlines(), [WATCHLIST])
            (root / WATCHLIST).write_text(watch.replace('T10:00:00', 'T09:30:00'))
            publish_watchlist(root)
            self.assertEqual(git(remote, 'show', 'main:' + WATCHLIST).decode(), watch)


if __name__ == '__main__':
    unittest.main()
