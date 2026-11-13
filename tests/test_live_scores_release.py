import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('release',Path(__file__).resolve().parents[1]/'.github/ops/live-scores-release.py')
release=importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)
A='3107a825-630f-44da-b153-86577718eb15'
B='47de8620-c1e9-482b-8098-da60ce8f5379'
class ReleaseIdentityTests(unittest.TestCase):
    def test_exact_script_identity_required(self):
        with patch.object(release,'api',side_effect=[
            {'resources':{'script':{'etag':'abc'}}},{'resources':{'script':{'etag':'xyz'}}}]):
            self.assertFalse(release.same_script(A,B))
    def test_missing_etag_is_not_equivalent(self):
        with patch.object(release,'api',return_value={}):
            self.assertFalse(release.same_script(A,B))
    def test_equal_scripts_still_require_configuration_and_cron(self):
        data={'health':{'last_cron_at':'2026-11-13T09:02:00Z'},'fixtures':{},'status':'provider_rate_limit'}
        with patch.object(release,'public_response',return_value=(B,data)), patch.object(release,'same_script',return_value=True), patch.object(release,'verify_configuration') as config, patch.object(release,'active_version',return_value=B):
            self.assertEqual(release.verify_runtime(A,'2026-11-13T09:00:00Z'),B)
            config.assert_called_once()
    def test_deployment_propagation_is_bounded(self):
        with patch.object(release,'active_version',side_effect=[B,B,A]), patch.object(release,'sleep') as sleep:
            self.assertEqual(release.wait_active(A),A)
            self.assertEqual(sleep.call_count,2)
    def test_unrelated_version_cannot_satisfy_deployment(self):
        with patch.object(release,'active_version',return_value=B), patch.object(release,'same_script',return_value=False), patch.object(release,'sleep') as sleep:
            with self.assertRaises(RuntimeError): release.wait_active(A,True)
            self.assertEqual(sleep.call_count,6)
if __name__=='__main__': unittest.main()
