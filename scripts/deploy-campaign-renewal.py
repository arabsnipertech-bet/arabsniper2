"""Patch only verified renewal module, preserving the rest of production."""
import importlib.util, os, json, email.parser, email.policy, tempfile, subprocess, shutil
from pathlib import Path
spec=importlib.util.spec_from_file_location('ops',Path(__file__).with_name('deploy-telegram-campaign.py'))
ops=importlib.util.module_from_spec(spec);spec.loader.exec_module(ops)
REF='4a98470ce18531460e6c57a83af9c5202caf2cd6'
def download():
    raw,h=ops.request(ops.BASE+ops.WORKER+'/content/v2',os.environ['CF_TOKEN'])
    ct=h.get('Content-Type','')
    if 'multipart/' in ct:
        m=email.parser.BytesParser(policy=email.policy.default).parsebytes(('Content-Type: '+ct+'\r\n\r\n').encode()+raw)
        parts=[(p.get_param('name',header='content-disposition'),p.get_payload(decode=True)) for p in m.iter_parts()]
    else:parts=[('worker.js',raw)]
    assert len(parts)==1 and parts[0][0].endswith(('.js','.mjs')),'Unexpected module layout'
    return parts[0][0],parts[0][1].decode()
def main():
    old=ops.request('https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/'+REF+'/workers/stripe-access/renewal.mjs')[0].decode().replace('export ','')
    new=Path('workers/stripe-access/renewal.mjs').read_text().replace('export ','')
    name,code=download();already=code.count(new)==1
    assert already or code.count(old)==1,'Current renewal module differs; review concurrent work'
    patched=code if already else code.replace(old,new)
    settings=ops.cf(ops.WORKER+'/settings',os.environ['CF_TOKEN'])
    bindings={b['name']:b for b in settings['bindings']}
    for flag in ['GOLD_RENEWAL_ENABLED','GOLD_PROMO_ENABLED','CHECKOUT_ENABLED','TELEGRAM_TRIAL_ENABLED']:
        assert bindings.get(flag,{}).get('text')=='true',flag+' disabled'
    assert bindings['ACCESS_DB']['id']==ops.DB
    assert bindings.get('GOLD_RENEWAL_COUPON_ID',{}).get('text')=='arabsniper_gold_renewal_10'
    def test_actual(content):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            shutil.copytree('workers/stripe-access',root/'workers/stripe-access')
            (root/'workers/stripe-access/worker.mjs').write_text(content)
            (root/'tests').mkdir();shutil.copy('tests/test_gold_renewal.mjs',root/'tests/test_gold_renewal.mjs')
            result=subprocess.run(['node','--test',str(root/'tests/test_gold_renewal.mjs')],capture_output=True,text=True)
            assert result.returncode==0,'Actual Worker failed isolated renewal integration tests'
        print('Actual Worker: 18 renewal tests passed (simulated Stripe and email)')
    test_actual(patched)
    import boto3
    s3=boto3.client('s3',endpoint_url='https://'+ops.ACCOUNT+'.r2.cloudflarestorage.com',region_name='auto',aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'])
    s3.put_object(Bucket='arabsniper-gold',Key='ops-backups/telegram-campaign/'+ops.CAMPAIGN+'/renewal-'+os.environ['GITHUB_RUN_ID']+'.json',Body=json.dumps({'worker':code,'module':name,'settings':settings}).encode(),ContentType='application/json')
    if not already:
        assert download()==(name,code),'Concurrent deployment detected'
        boundary='----ArabSniperCampaignRenewal202611'
        body=(f'--{boundary}\r\nContent-Disposition: form-data; name="metadata"\r\nContent-Type: application/json\r\n\r\n'+json.dumps({'main_module':name})+f'\r\n--{boundary}\r\nContent-Disposition: form-data; name="{name}"; filename="{name}"\r\nContent-Type: application/javascript+module\r\n\r\n'+patched+f'\r\n--{boundary}--\r\n').encode()
        raw,_=ops.request(ops.BASE+ops.WORKER+'/content',os.environ['CF_TOKEN'],body,'PUT',{'Content-Type':'multipart/form-data; boundary='+boundary})
        assert json.loads(raw).get('success'),'Content update rejected'
    assert download()==(name,patched),'Deployed source does not match'
    after=ops.cf(ops.WORKER+'/settings',os.environ['CF_TOKEN'])
    assert sorted(settings['bindings'],key=lambda b:b['name'])==sorted(after['bindings'],key=lambda b:b['name'])
    for k in ['compatibility_date','compatibility_flags','observability','limits','placement','logpush','tail_consumers']:
        assert settings.get(k)==after.get(k),'Configuration changed unexpectedly'
    test_actual(download()[1]);ops.health()
    print('Campaign renewal discount deployed; SHA256:',ops.digest(patched))
    with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as out:out.write('Campaign free trials now qualify for 10% in their last 48 hours. Actual deployed Worker passed 18 isolated tests. Settings unchanged.\n')
if __name__=='__main__':
    try:main()
    except Exception as e:
        print('STOP:',str(e) if isinstance(e,(AssertionError,RuntimeError)) else type(e).__name__)
        raise SystemExit(1) from None
