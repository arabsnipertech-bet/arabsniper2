"""One-time replacement content for the Nov 8 morning audit placeholder."""
import json,os
from datetime import datetime
from pathlib import Path
from telegram_digest import ROME,artifact,gh,reserve,prior_ids
from social_analysis import select

def prepare():
    now=datetime.now(ROME)
    if now.date().isoformat()!='2026-11-08' or not (10,45)<=(now.hour,now.minute)<(12,30):
        print('Correction window closed');return
    name='tg-2026-11-08-1045-analysis-correction'
    if artifact(name+'-sent'):
        print('Correction already sent');return
    claim=artifact(name+'-claim')
    if claim:
        from telegram_reconcile import recover
        recover(claim,name,now)
        with open(os.environ['GITHUB_OUTPUT'],'a') as out:print('name='+name,file=out)
        return
    original=artifact('tg-2026-11-08-1045-sent')
    if not original:raise RuntimeError('Original receipt missing')
    folder=Path('original');folder.mkdir(exist_ok=True)
    gh('run','download',str(original['workflow_run']['id']),'-n','tg-2026-11-08-1045-sent','-D',str(folder))
    old=json.loads((folder/'post.json').read_text())
    if old.get('picks') or 'in verifica' not in old.get('text',''):
        print('Original post is not the empty audit notice');return
    from gold_storage import client
    board=json.load(client().get_object(Bucket=os.environ['R2_BUCKET'],Key='arab_quote_day1.json')['Body'])
    chosen=select(board,now,now,now.replace(hour=12,minute=30,second=0,microsecond=0),prior_ids('2026-11-08','10:45'))
    print('Current Gold morning fixtures:',[p['match'] for p in chosen])
    if not chosen:
        print('No fresh unpublished morning analyses; no corrective empty post');return
    reserve(name,dict(day='2026-11-08',slot='10:45',picks=chosen,reserved=[],first='10:45',last='12:30',analysis_only=True))
if __name__=='__main__':prepare()
