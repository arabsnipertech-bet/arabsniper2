"""Same bounded Gold offer on the verified Facebook Page, with independent receipts."""
import importlib.util,json,os,sys
from datetime import datetime,timezone
from pathlib import Path
spec=importlib.util.spec_from_file_location('campaign',Path(__file__).with_name('gold-campaign-repeat.py'))
campaign=importlib.util.module_from_spec(spec);spec.loader.exec_module(campaign)
ops=campaign.ops
PAGE='604216843376166'
def meta(path,body=None):
    raw,_=ops.request('https://graph.facebook.com/v25.0/'+path,os.environ['META_TOKEN'],
        json.dumps(body).encode() if body is not None else None,'POST' if body is not None else 'GET',{'Content-Type':'application/json'})
    result=json.loads(raw)
    if 'error' in result:raise RuntimeError('Facebook request rejected')
    return result

def verify_ready():
    campaign.run(verify_only=True)
    identity=meta('me?fields=id,name')
    assert identity.get('id')==PAGE,'Wrong Facebook Page identity'
    assert isinstance(meta(PAGE+'/published_posts?fields=id&limit=1').get('data'),list)

def publish_once(day,index,label):
    key=ops.CAMPAIGN+':'+day+':'+label
    ops.query('CREATE TABLE IF NOT EXISTS facebook_campaign_announcements (campaign TEXT PRIMARY KEY,status TEXT NOT NULL,post_id TEXT,created_at INTEGER NOT NULL)')
    existing=ops.query('SELECT status,post_id FROM facebook_campaign_announcements WHERE campaign=?',[key])
    if existing:
        assert existing[0]['status']=='sent','Facebook send outcome uncertain; inspect Page before retrying'
        print('Facebook already sent: https://www.facebook.com/'+existing[0]['post_id']);return
    verify_ready()
    owned=ops.query("INSERT OR IGNORE INTO facebook_campaign_announcements(campaign,status,created_at) VALUES (?,'sending',unixepoch()) RETURNING campaign",[key])
    assert owned,'Another run owns this Facebook slot'
    sent=meta(PAGE+'/feed',{'message':campaign.message(day,index)})
    post=sent.get('id','')
    assert post.startswith(PAGE+'_') and post[len(PAGE)+1:].isdigit(),'Unverified Facebook publication receipt'
    ops.query("UPDATE facebook_campaign_announcements SET status='sent',post_id=? WHERE campaign=?",[post,key])
    print('Facebook published '+key+': https://www.facebook.com/'+post)

def run(now=None,verify_only=False,*,recovery=False):
    slot=campaign.selected(now or datetime.now(timezone.utc),recovery=recovery)
    if not slot and not verify_only:
        print('Outside Facebook campaign dates or windows; nothing sent');return
    if verify_only:
        verify_ready()
        print('Facebook Page identity and published-post access verified; no post sent');return
    publish_once(*slot)
if __name__=='__main__':
    try:run(verify_only='--verify' in sys.argv,recovery='--recover' in sys.argv)
    except Exception as e:
        print('STOP:',str(e) if isinstance(e,(AssertionError,RuntimeError)) else type(e).__name__)
        sys.exit(1)
