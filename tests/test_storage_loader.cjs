const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {gzipSync} = require('node:zlib');
const {createHash, webcrypto} = require('node:crypto');
const html = fs.readFileSync('web/gold-dashboard-live.html','utf8');
const source = html.slice(html.indexOf('    async function loadJson(file)'), html.indexOf('    async function loadCurrentDay'));
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
const context={crypto:webcrypto,Uint8Array,TextDecoder,DecompressionStream,Response,
  rawUrl:x=>x,fetch:async path=>{active++;peak=Math.max(peak,active);await new Promise(r=>setTimeout(r,5));active--;return new Response(files[path],{status:files[path]?200:404});}};
vm.createContext(context);vm.runInContext(source,context);
(async()=>{
  for(const file of ['legacy.json','day.json']) assert.equal(JSON.stringify(await context.loadJson(file)),JSON.stringify(value));
  assert(peak>1&&peak<=3, 'bounded parallel downloads');
  const urls={GITHUB_BASE:'https://example.test/',Date};vm.createContext(urls);
  vm.runInContext(html.slice(html.indexOf('    function rawUrl(file)'),html.indexOf('    function escapeHtml')),urls);
  assert.equal(urls.rawUrl('_audit_parts/hash.json.gz'),'https://example.test/_audit_parts/hash.json.gz');
  delete files[`_audit_parts/${parts[0].sha256}.json.gz`];
  await assert.rejects(context.loadJson('day.json'),/non disponibile/);
  console.log('Browser loader: legacy, split UTF-8, checksums and missing parts passed');
})().catch(e=>{console.error(e);process.exitCode=1;});
