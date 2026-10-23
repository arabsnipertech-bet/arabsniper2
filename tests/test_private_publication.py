import subprocess
import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from private_publication import stage_public, git


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


if __name__ == '__main__':
    unittest.main()
