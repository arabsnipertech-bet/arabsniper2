const fs=require('fs'),vm=require('vm'),assert=require('node:assert/strict');
async function render(data){
 const panel={innerHTML:''};
 class Clock extends Date{constructor(...a){super(...(a.length?a:['2026-11-08T12:00:00Z']));}static now(){return +new Clock();}}
 vm.runInNewContext(fs.readFileSync('web/results.js','utf8'),{Date:Clock,Intl,AbortController,setTimeout,clearTimeout,setInterval(){},document:{getElementById:()=>panel},fetch:async()=>({ok:true,json:async()=>data})});
 await new Promise(r=>setImmediate(r));return panel.innerHTML;
}
(async()=>{
 const row={outcome:'won',match:'Casa - Ospite',label:'Over 2,5',odd:1.9};
 const history={settled:3,wins:2,losses:1,pending:0,from_date:'2026-11-01',to_date:'2026-11-07'};
 let html=await render({available:false,date:'2026-11-07',all_results:[],history});
 assert(html.includes('3 esiti conclusi'));assert(!html.includes('V5 è in verifica'));
 html=await render({available:false,date:'2026-11-07',history,latest_verified:{date:'2026-11-05',all_results:[row]}});
 assert(html.includes('5 novembre 2026'));assert(html.includes('ultima giornata verificata'));assert(html.includes('Casa - Ospite'));
 html=await render({available:true,date:'2026-11-07',all_results:[row,{...row,outcome:'lost',match:'<script>evil</script>'}]});
 assert(html.includes('1 esiti centrati · 1 non centrati'));assert(!html.includes('<script>'));assert(!html.includes('in aggiornamento'));
 html=await render({latest_verified:{date:'2026-11-09',all_results:[row]}});assert(!html.includes('Casa - Ospite'));
 console.log('PASS verified results: missing yesterday, dated historical fallback, all outcomes, escaping, future rejection');
})();
