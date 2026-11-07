import base64,gzip,json,sys,tempfile,unittest
from pathlib import Path
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/p) for p in ('src','auditor')]
from auditor_history_chunks import digest
from repair_certificate import build,encrypt

class RepairCertificate(unittest.TestCase):
 def fixture(self,root):
  day='2026-11-05';prefix=f'auditarchive/prematch_{day}_';(root/'auditarchive').mkdir()
  report={'audit_date':day,'policy_id':'gold-goals-v6.1','four_level_benchmark':{'decision_benchmark':{'decision_path_comparison':{'first':{'pure_poisson':{'n':2}}}}}}
  values={'results.json':'{}','daily.json':json.dumps(report),'daily.md':'# Verified','calibration.json':'[]'}
  outputs={}
  for suffix,content in values.items():
   p=root/(prefix+suffix);p.write_text(content);outputs[prefix+suffix]=digest(p)
  receipt=dict(version=1,day=day,pending=False,input_sha256='a'*64,outputs=outputs)
  (root/(prefix+'checkpoint.json')).write_text(json.dumps(receipt))
  index={p:{'sha256':sha} for p,sha in outputs.items()};index[f'auditordata/prematch_{day}.json']={'sha256':'a'*64}
  state=dict(evidence_provenance={'complete':True,'dates':[day],'files':[{'date':day,'sha256':outputs[prefix+'calibration.json'],'samples':0}]},mismatch_shrinkage_factor=.17684)
  return index,state,day
 def test_certificate_binds_training_to_exact_sealed_outputs(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);index,state,day=self.fixture(root)
   result=build(root,index,state,[day],'b'*64)
   self.assertTrue(result['r2_chain_complete'])
   self.assertEqual(result['weights']['mismatch_shrinkage_factor'],.17684)
   self.assertEqual(result['dates'][day]['decision_path_comparison']['first']['pure_poisson']['n'],2)
   (root/f'auditarchive/prematch_{day}_daily.md').write_text('changed')
   with self.assertRaisesRegex(ValueError,'match seal'):build(root,index,state,[day],'b'*64)
 def test_missing_training_date_and_changed_input_cannot_certify(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);index,state,day=self.fixture(root)
   with self.assertRaisesRegex(ValueError,'missing'):build(root,index,state,['2026-11-06'],'b'*64)
   index[f'auditordata/prematch_{day}.json']['sha256']='c'*64
   with self.assertRaisesRegex(ValueError,'Frozen input changed'):build(root,index,state,[day],'b'*64)
 def test_only_private_recipient_can_read_authenticated_bundle(self):
  from cryptography.hazmat.primitives import hashes,serialization
  from cryptography.hazmat.primitives.asymmetric import rsa,padding
  from cryptography.hazmat.primitives.ciphers.aead import AESGCM
  private=rsa.generate_private_key(public_exponent=65537,key_size=2048)
  public=private.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo)
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'certificate.enc.json';report={'alpha':.17684,'private_metrics':[1,2,3]};encrypt(report,public,p)
   self.assertNotIn('private_metrics',p.read_text())
   e={k:base64.b64decode(v) for k,v in json.loads(p.read_text()).items()}
   key=private.decrypt(e['key'],padding.OAEP(mgf=padding.MGF1(hashes.SHA256()),algorithm=hashes.SHA256(),label=None))
   self.assertEqual(json.loads(gzip.decompress(AESGCM(key).decrypt(e['nonce'],e['ciphertext'],e['aad']))),report)
   with self.assertRaises(Exception):AESGCM(key).decrypt(e['nonce'],e['ciphertext'],b'wrong-context')
