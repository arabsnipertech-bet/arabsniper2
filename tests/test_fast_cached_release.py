"""Exercise the workflow's actual Git diff against linear and merged releases."""
from pathlib import Path
import re
import shlex
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from refresh_opportunities import cached_refresh_safe


class CachedReleaseGraph(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.repo = Path(self.directory.name)
        self.git('init', '-b', 'main')
        self.git('config', 'user.name', 'Release test')
        self.git('config', 'user.email', 'release-test@example.invalid')
        self.commit('README.md', 'base')

    def git(self, *args):
        return subprocess.run(['git', *args], cwd=self.repo, check=True,
                              capture_output=True, text=True).stdout

    def commit(self, path, value='changed'):
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(value)
        self.git('add', path)
        self.git('commit', '-m', path)

    def workflow_cached(self):
        workflow = (ROOT / '.github/workflows/fast_scan.yml').read_text()
        command = re.search(r'mode=\$\((git [^\n]+?) \| python src/refresh_opportunities.py --classify-push\)', workflow)
        self.assertIsNotNone(command, 'Test must exercise the actual workflow diff')
        paths = subprocess.check_output(shlex.split(command[1]), cwd=self.repo, text=True).splitlines()
        return cached_refresh_safe(paths)

    def merge(self, incoming, *, main_path='web/example.css'):
        self.git('switch', '-c', 'release')
        self.commit(incoming)
        self.git('switch', 'main')
        self.commit(main_path)
        self.git('merge', '--no-ff', 'release', '-m', 'Merge release')
        self.assertEqual(len(self.git('show', '-s', '--format=%P', 'HEAD').split()), 2)

    def test_linear_policy_release_uses_saved_inputs(self):
        self.commit('src/v7_decision.py')
        self.assertTrue(self.workflow_cached())

    def test_merged_policy_release_uses_saved_inputs(self):
        self.merge('src/v7_decision.py')
        self.assertTrue(self.workflow_cached())

    def test_merged_provider_change_still_requires_collection(self):
        self.merge('src/odds_collection.py')
        self.assertFalse(self.workflow_cached())

    def test_prior_main_changes_do_not_mask_the_incoming_policy_release(self):
        self.merge('src/v7_decision.py', main_path='src/odds_collection.py')
        self.assertTrue(self.workflow_cached())

    def test_empty_diff_never_suppresses_required_collection(self):
        self.git('commit', '--allow-empty', '-m', 'empty release')
        self.assertFalse(self.workflow_cached())


if __name__ == '__main__':
    unittest.main()
