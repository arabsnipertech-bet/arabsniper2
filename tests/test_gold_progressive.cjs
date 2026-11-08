const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const {gzipSync}=require('node:zlib'),{createHash,webcrypto}=require('node:crypto');
const html=fs.readFileSync('web/gold-dashboard-live.html','utf8');
const source=html.slice(html.indexOf('    async function loadJson(file,'),html.indexOf('    async function loadCurrentDay'));
const pages=[],files={};
for(let i=0;i<3;i++){
 const raw=Buffer.from(JSON.stringify([{fixture:{fixture_id:i},probability:i+.25}]));
 const sha256=createHash('sha256').update(raw).digest('hex');
 pages.push({sha256,bytes:raw.length,count:1});files['_audit_parts/'+sha256+'.json.gz']=gzipSync(raw);
}
const index={_gold_storage:'arabsniper-gold-pages-v1',date:'2026-11-08',match_count:3,pages,matches:[]};
const calls=[];let release;const gate=new Promise(r=>release=r);
const c={crypto:webcrypto,Uint8Array,TextDecoder,DecompressionStream,Response,Blob,AbortController,DOMException,setTimeout,clearTimeout,rawUrl:x=>x,
 fetch:async path=>{calls.push(path);if(path.includes(pages[1].sha256))await gate;return new Response(files[path],{status:files[path]?200:404});}};
vm.createContext(c);vm.runInContext(source,c);
(async()=>{
 let previewResolve;const preview=new Promise(r=>previewResolve=r);let previews=0;
 const loading=c.loadJson(null,index,null,false,p=>{previews++;assert(p._gold_loading);assert(p.matches.length<3);previewResolve();});
 await preview;assert(previews>0,'render before slow page finishes');release();
 const final=await loading;assert.deepEqual(Array.from(final.matches,r=>r.fixture.fixture_id),[0,1,2]);assert(!final._gold_loading);
 const count=calls.length;final.matches[0].probability=999;
 const again=await c.loadJson(null,index);assert.equal(calls.length,count,'unchanged pages reused');assert.equal(again.matches[0].probability,.25,'cache cannot inherit render mutation');
 await assert.rejects(c.loadJson(null,{...index,pages:[{...pages[0],bytes:123},...pages.slice(1)]}),/Integrità/);
 const controller=new AbortController();controller.abort();
 await assert.rejects(c.loadJson(null,index,controller.signal),e=>e.name==='AbortError');
 // Even a fully cached day still reads its authenticated catalog.
 c.fetch=async()=>new Response('{}',{status:401});c.document={getElementById:id=>id==='app'?{}:{classList:{add(){}}}};c.resetCodeLogin=()=>{};c.loginMessage=()=>{};
 await assert.rejects(c.loadCalendarDay(1,'2026-11-08'),e=>e.code==='GOLD_AUTH_REQUIRED');
 assert.equal(c.loadJson.pageCache.size,0,'auth failure clears tab cache');
 console.log('PASS progressive Gold: early verified preview, final order, cache reuse, isolation, integrity, cancellation and auth expiry');
})().catch(e=>{console.error(e);process.exitCode=1});
