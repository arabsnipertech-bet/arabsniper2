const fs=require('fs'),vm=require('vm'),assert=require('node:assert/strict');
const code=fs.readFileSync('web/homepage.js','utf8');
async function run(payload,fail=false){
 const nodes=Object.fromEntries(['daily-picks','preview-date'].map(id=>[id,{textContent:'',innerHTML:''}]));
 const calls=[];let pending;
 class Clock extends Date{constructor(...a){super(...(a.length?a:['2026-10-22T12:00:00Z']));}static now(){return +new Clock();}}
 const ctx={Date:Clock,Intl,Set,Number,String,AbortSignal,handleUrlMessages(){},setInterval(){},document:{getElementById:id=>nodes[id],addEventListener(){}},fetch:async(url)=>{calls.push(url);return {ok:!fail,json:async()=>payload}}};
 vm.runInNewContext(code,ctx);await new Promise(r=>setImmediate(r));return {nodes,calls};
}
(async()=>{
 const data={date:'2026-10-22', selections:[1,2,3].map(id=>({fixture_id:String(id),match:'Home - Away',league:'League',kickoff:'2026-10-22T20:00:00+02:00',captured_at:'2026-10-22T10:00:00+02:00',odd:1.8,label:'Over 2,5 finale',statistical_pct:65,book_probability_pct:60}))};
 let r=await run(data);assert.equal((r.nodes['daily-picks'].innerHTML.match(/class="pick"/g)||[]).length,3);assert.equal(r.calls.length,1);assert.match(r.calls[0],/arab_quote_homepage.json/);
 r=await run({...data,date:'2026-10-21'});assert.match(r.nodes['daily-picks'].innerHTML,/Nessuna selezione/);
 const started=structuredClone(data);started.selections[0].kickoff='2026-10-22T10:00:00Z';
 r=await run(started);assert.equal((r.nodes['daily-picks'].innerHTML.match(/class="pick"/g)||[]).length,2);
 const evil=structuredClone(data);evil.selections[0].match='<img src=x onerror=alert(1)>';
 r=await run(evil);assert(!r.nodes['daily-picks'].innerHTML.includes('<img'));assert(r.nodes['daily-picks'].innerHTML.includes('&lt;img'));
 r=await run(data,true);assert.match(r.nodes['daily-picks'].innerHTML,/temporaneamente/);
 const html=fs.readFileSync('web/index.html','utf8');assert(!html.includes('localStorage.clear'));assert(!html.includes('logout=true'));assert(!html.includes('details_day1'));
 for(const plan of ['daily','weekly','monthly']) assert(html.includes("buy('"+plan+"', this)"));
 console.log('PASS: public feed only, three cards, stale day, escaped HTML, unavailable feed, Gold entry links and plan IDs');
})();

