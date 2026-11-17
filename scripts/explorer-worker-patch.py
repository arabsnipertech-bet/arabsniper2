"""Add Explorer to the actual live Worker without replacing existing modules."""
from pathlib import Path
import shutil
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parents[1]

def patch_worker(code):
    start='const {authRoute, cleanupAuth} = (() => {'
    end='return {authRoute,cleanupAuth};'
    seam='    const testLogin=goldTestLoginPage(request,env);'
    assert code.count(start)==code.count(end)==code.count(seam)==1,'Live Worker seam changed; manual review required'
    assert 'async function sessionEmail(' in code and 'function cachedAccess(' in code
    assert 'const explorerRoute =' not in code,'Explorer already installed; inspect before updating'
    module='\nconst explorerRoute = (() => {\n'+(ROOT/'workers/stripe-access/explorer.mjs').read_text().replace('export ','')+'\nreturn explorerRoute;\n})();\n'
    route='    const explorer=await explorerRoute(request,env,{sessionEmail,\n      access:(e,email,now)=>cachedAccess(e,email,now,()=>access(e,email,now))});\n    if(explorer)return explorer;\n'
    patched=module+code.replace(start,'const {authRoute, cleanupAuth, sessionEmail} = (() => {').replace(end,'return {authRoute,cleanupAuth,sessionEmail};').replace(seam,route+seam)
    reversed_code=patched[len(module):].replace(route,'').replace('const {authRoute, cleanupAuth, sessionEmail} = (() => {',start).replace('return {authRoute,cleanupAuth,sessionEmail};',end)
    assert reversed_code==code,'Non-Explorer code changed'
    return patched

def verify(code):
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp)
        shutil.copytree(ROOT/'workers/stripe-access',root/'workers/stripe-access')
        (root/'workers/stripe-access/worker.mjs').write_text(code)
        (root/'tests').mkdir()
        names=['test_gold_auth.mjs','test_gold_renewal.mjs','test_gold_explorer.mjs']
        shutil.copytree(ROOT/'web',root/'web')
        for name in names:shutil.copy(ROOT/'tests'/name,root/'tests'/name)
        check=subprocess.run(['node','--check',str(root/'workers/stripe-access/worker.mjs')],capture_output=True)
        assert check.returncode==0,'Patched Worker syntax invalid'
        for name in names:
            test=subprocess.run(['node','--test',str(root/'tests'/name)],capture_output=True,text=True)
            assert test.returncode==0,'Actual Worker isolated suite failed: '+name
    return names
