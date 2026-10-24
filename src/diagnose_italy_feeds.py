"""Read-only aggregate health counts; never log Gold fixture or quote rows."""
import json, os
from gold_storage import client
s3=client();bucket=os.environ['R2_BUCKET']
for i in range(1,6):
    obj=s3.get_object(Bucket=bucket,Key=f'arab_quote_day{i}.json')
    d=json.loads(obj['Body'].read());groups={}
    for m in d.get('matches',[]):
        f=m.get('fixture',{}); league=str(f.get('league',''))
        if f.get('country')!='Italy' or not league.startswith(('Serie B','Serie C')):continue
        group='Serie B' if league.startswith('Serie B') else 'Serie C'
        g=groups.setdefault(group,dict(present=0,visible=0,home_missing=0,over25_missing=0,over15ht_missing=0))
        g['present']+=1;a=m.get('multimarket',{})
        g['visible']+=int(a.get('status')=='experimental' and str(a.get('model_id','')).startswith('context-goals-v3'))
        rows={(r.get('market'),r.get('selection')):r for r in a.get('markets',[])}
        for market,side,label in [('1x2_ft','1','home_missing'),('over25_ft','over','over25_missing'),('over15_ht','over','over15ht_missing')]:
            odd=rows.get((market,side),{}).get('odd')
            g[label]+=int(not isinstance(odd,(int,float)) or odd<=1)
    print('ITALY_HEALTH='+json.dumps({'day':i,'date':d.get('date'),'updated':d.get('updated_at'),'modified':str(obj['LastModified']),'total':len(d.get('matches',[])),'groups':groups},separators=(',',':')),flush=True)
