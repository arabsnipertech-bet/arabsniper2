"""Read-only promotional examples from frozen Auditor records, never new picks."""
import json
import math
import os
from datetime import timedelta
from zoneinfo import ZoneInfo
from multimarket_model import stamp
from lateral_matrix import settle_spec
from daily_audit import fingerprint

ROME = ZoneInfo('Europe/Rome')


def build(store, day):
    first, metadata = {}, {}
    for collection in ('multimarket_evaluations', 'analysis_updates', 'study_observations'):
        for record in store.get(collection, {}).values():
            f=record.get('fixture') or {}
            at,start=stamp(record.get('captured_at')),stamp(f.get('kickoff'))
            fid=str(f.get('fixture_id') or '')
            if (not fid or not at or not start or at>=start or record.get('retrospective')
                    or start.astimezone(ROME).date().isoformat()!=day): continue
            metadata.setdefault(fid,{}).update({k:v for k,v in f.items() if v})
            rows=[]
            if collection=='study_observations':
                rows=[('Studio',r) for r in record.get('studio_ideas',[]) if not r.get('suppressed')]
            else:
                for r in (record.get('multimarket') or {}).get('matrix',{}).get('rows',[]):
                    if r.get('quote_executable') is not True: continue
                    tier='Gold' if (r.get('opportunity') or {}).get('eligible') is True else 'Platinum' if (r.get('platinum') or {}).get('eligible') is True else None
                    if tier: rows.append((tier,r))
            for tier,row in rows:
                odd=row.get('odd');spec=row.get('spec')
                if (not spec or type(odd) not in (int,float) or not math.isfinite(odd) or odd<=1): continue
                key=(fid,json.dumps(spec,sort_keys=True))
                # Fix the first actual proposal and its quote before seeing outcomes.
                if key in first and at>=stamp(first[key]['captured_at']): continue
                result=(record.get('settlement') or {}).get('result') or {}
                value=settle_spec(spec,result) if result.get('status') in ('FT','AET','PEN') else None
                first[key]=dict(fixture_id=fid,tier=tier,label=row.get('label') or row.get('market',''),
                    odd=odd,captured_at=at.isoformat(),forecast_sha256=fingerprint(record),
                    won=value is not None and value[0]==1 and value[1]==0,result=result)
    best={}
    for row in sorted(first.values(),key=lambda r:(-r['odd'],r['fixture_id'],r['label'])):
        f=metadata[row['fixture_id']]
        name=f.get('match')
        if not name and isinstance(f.get('home'),str) and isinstance(f.get('away'),str):name=f['home']+' - '+f['away']
        if not row['won'] or not name or not f.get('country') or not f.get('league') or not row['label']:continue
        best.setdefault(row['fixture_id'],dict(row,match=name,country=f['country'],league=f['league']))
    winners=list(best.values())[:10]
    return dict(schema_version=1,date=day,examples=winners if len(winners)>=5 else [],
                ready=len(winners)>=5,selection_note='Selezione di successi, non bilancio completo della giornata.')


def load_yesterday(now):
    from gold_storage import client, previous_manifest, sha
    s3=client();bucket=os.environ['R2_BUCKET']
    day=(now.astimezone(ROME).date()-timedelta(days=1)).isoformat()
    item=previous_manifest(s3,bucket).get(f'auditarchive/prematch_{day}_promo.json')
    if not item:return None
    if item['bytes']>200000:raise ValueError('Oversized promotion report')
    raw=s3.get_object(Bucket=bucket,Key='state/objects/'+item['sha256'])['Body'].read()
    if len(raw)!=item['bytes'] or sha(raw)!=item['sha256']:raise ValueError('Promotion checksum mismatch')
    report=json.loads(raw)
    if report.get('date')!=day or not report.get('ready') or not 5<=len(report.get('examples',[]))<=10:return None
    return report


def message(report):
    return (f"✅ {len(report['examples'])} SUCCESSI DALLE ANALISI DEL {report['date']}\n\n"
            "Una selezione di proposte Gold, Platinum e Studio registrate prima del calcio d’inizio e verificate dall’auditor. Categorie e quote nell’immagine.\n\n"
            "Selezione di successi: non è il bilancio completo della giornata. Studio indica analisi di studio, non una qualificazione Gold.\n\n"
            "🎁 Esplora l’area Gold: 7 giorni gratuiti con /prova su @ArabSniperBot\n"
            "arabsniperbet.com\n18+ · I risultati passati non garantiscono quelli futuri.")
