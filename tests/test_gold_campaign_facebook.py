import importlib.util,unittest
from unittest import mock
from pathlib import Path
from datetime import datetime
spec=importlib.util.spec_from_file_location('fb',Path(__file__).resolve().parents[1]/'scripts/gold-campaign-facebook.py')
fb=importlib.util.module_from_spec(spec);spec.loader.exec_module(fb)
class FacebookCampaign(unittest.TestCase):
 def setUp(self):
  self.rows={};self.posts=[]
  verified=mock.patch.object(fb,'verify_ready')
  self.verify=verified.start()
  self.addCleanup(verified.stop)
  def query(sql,params=None):
   p=params or []
   if sql.startswith('CREATE'):return []
   if sql.startswith('SELECT'):return [self.rows[p[0]]] if p[0] in self.rows else []
   if sql.startswith('INSERT'):
    if p[0] in self.rows:return []
    self.rows[p[0]]={'status':'sending','post_id':None};return [{'campaign':p[0]}]
   if sql.startswith('UPDATE'):self.rows[p[1]]={'status':'sent','post_id':p[0]};return []
   self.fail(sql)
  fb.ops.query=query
  fb.meta=lambda path,body=None:self.send(path,body)
 def send(self,path,body):
  self.assertEqual(path,fb.PAGE+'/feed');self.assertIn('/prova',body['message'])
  self.posts.append(body);return {'id':fb.PAGE+'_12345'}
 def test_retry_does_not_repeat_publication(self):
  fb.publish_once('2026-11-12',0,'pranzo');fb.publish_once('2026-11-12',0,'pranzo')
  self.assertEqual(len(self.posts),1)
 def test_uncertain_send_does_not_repeat(self):
  self.rows[fb.ops.CAMPAIGN+':2026-11-12:pranzo']={'status':'sending','post_id':None}
  with self.assertRaises(AssertionError):fb.publish_once('2026-11-12',0,'pranzo')
  self.assertFalse(self.posts)
 def test_wrong_page_receipt_is_not_confirmed(self):
  fb.meta=lambda *a,**k:{'id':'wrong_123'}
  with self.assertRaises(AssertionError):fb.publish_once('2026-11-12',0,'pranzo')
  self.assertEqual(next(iter(self.rows.values()))['status'],'sending')
 def test_campaign_ends_without_remote_requests(self):
  fb.run(datetime.fromisoformat('2026-11-19T12:00:00+00:00'))
  self.verify.assert_not_called()
 def test_recovery_uses_the_same_page_receipt_after_the_due_time(self):
  fb.run(datetime.fromisoformat('2026-11-12T19:44:59+00:00'))
  self.verify.assert_not_called()
  due=datetime.fromisoformat('2026-11-12T20:15:00+00:00')
  fb.run(due)
  fb.run(due)
  self.assertEqual(len(self.posts),1)
  self.assertEqual(list(self.rows),['gold-engine-2026-11:2026-11-12:sera'])
  self.verify.assert_called_once()
if __name__=='__main__':unittest.main()
