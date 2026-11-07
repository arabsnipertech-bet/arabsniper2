const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {gzipSync} = require('node:zlib');
const {createHash, webcrypto} = require('node:crypto');
const html = fs.readFileSync('web/gold-dashboard-live.html','utf8');
const source = html.slice(html.indexOf('    async function loadJson(file,'), html.indexOf('    async function loadCurrentDay'));
const sha = b => createHash('sha256').update(b).digest('hex');
const value = {matches:[{name:'Città 日本', probability:42.123, live:{status:'HT'}}]};
const bytes = Buffer.from(JSON.stringify(value));
const parts = [], files = {};
for (let i=0;i<bytes.length;i+=7) {
  const chunk=bytes.subarray(i,i+7), hash=sha(chunk);
  parts.push({sha256:hash,bytes:chunk.length});
  files[`_audit_parts/${hash}.json.gz`]=gzipSync(chunk);
}
const manifest={_storage:'arabsniper-audit-parts-v1',bytes:bytes.length,sha256:sha(bytes),parts};
files['day.json']=JSON.stringify(manifest);
files['legacy.json']=JSON.stringify(value);
let active=0,peak=0;
const context={crypto:webcrypto,Uint8Array,TextDecoder,DecompressionStream,Response,Blob,AbortController,setTimeout,clearTimeout,
  rawUrl:x=>x,fetch:async path=>{active++;peak=Math.max(peak,active);await new Promise(r=>setTimeout(r,5));active--;return new Response(files[path],{status:files[path]?200:404});}};
vm.createContext(context);vm.runInContext(source,context);
(async()=>{
  for(const file of ['legacy.json','day.json']) assert.equal(JSON.stringify(await context.loadJson(file)),JSON.stringify(value));
  assert(peak>1&&peak<=3, 'bounded parallel downloads');
  const urls={GITHUB_BASE:'https://example.test/',Date};vm.createContext(urls);
  vm.runInContext(html.slice(html.indexOf('    function rawUrl(file)'),html.indexOf('    function escapeHtml')),urls);
  assert.equal(urls.rawUrl('_audit_parts/hash.json.gz'),'https://example.test/_audit_parts/hash.json.gz');
  delete files[`_audit_parts/${parts[0].sha256}.json.gz`];
  await assert.rejects(context.loadJson('day.json'),/non disponibil/);
  const rows=[{fixture:{fixture_id:1},label:'Città 日本'},{fixture:{fixture_id:2}}];
  const page=Buffer.from(JSON.stringify(rows)),hash=sha(page);
  files['_audit_parts/'+hash+'.json.gz']=gzipSync(page);
  const index={_gold_storage:'arabsniper-gold-pages-v1',date:'2026-11-07',match_count:2,pages:[{sha256:hash,bytes:page.length,count:2}],matches:[]};
  assert.equal(JSON.stringify((await context.loadJson(null,index)).matches),JSON.stringify(rows));
  await assert.rejects(context.loadJson(null,{...index,match_count:3}),/Conteggio/);
  await assert.rejects(context.loadJson(null,{...index,pages:[{...index.pages[0],bytes:page.length+1}]}),/Integrità pagina/);
  let attempts=0;const originalFetch=context.fetch;
  context.fetch=async path=>{if(attempts++===0)return new Response('busy',{status:503});return originalFetch(path);};
  assert.equal(JSON.stringify(await context.loadJson('legacy.json')),JSON.stringify(value));assert.equal(attempts,2);
  let reset=0,message='';const nodes={app:{hidden:false},loginOverlay:{classList:{add(){}}}};
  context.document={getElementById:id=>nodes[id]};context.resetCodeLogin=()=>reset++;context.loginMessage=m=>message=m;
  context.fetch=async()=>new Response('{}',{status:401});
  await assert.rejects(context.loadJson(null,index),e=>e.code==='GOLD_AUTH_REQUIRED');assert.equal(reset,1);assert(nodes.app.hidden);assert(message.includes('sessione'));
  console.log('Browser loader: legacy, split UTF-8, checksums and missing parts passed');
})().catch(e=>{console.error(e);process.exitCode=1;});
