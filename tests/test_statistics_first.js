const assert=require('node:assert/strict');
const api=require('../web/statistics-first.js');
const NOW=Date.parse('2026-10-26T08:00:00Z');
function fixture(id,selected=true,score=60,confirmation='neutral'){
  return {fixture:{fixture_id:String(id),kickoff:'2026-10-26T20:00:00Z',status_short:'NS',match:'Synthetic <img src=x onerror=alert(1)>',league:'Test',country:'Italy'},
    multimarket:{model_id:api.MODEL,captured_at:'2026-10-26T08:00:00Z',profiles:{},markets:Object.keys(api.LABELS).map(market=>({
      market,selection:market==='gg_ht'?'yes':'over',statistical_pct:score,statistical_score:score,
      statistical_candidate:true,published_selection:selected,published_rank:1,odd:null,
      market_confirmation:{status:confirmation},selection_reasons:[],pros:[],cons:[]}))}};
}
const options={market:'over25_ft',scope:'selected',signal:'all',phase:'upcoming',sort:'probability',search:''};
const p={matches:[fixture(1,true,61),fixture(2,false,90),fixture(3,true,70,'confirmed')]};
assert.deepEqual(api.selectRows(p,options,NOW).map(m=>m.fixture.fixture_id),['3','1']);
assert.deepEqual(api.selectRows(p,{...options,signal:'confirmed'},NOW).map(m=>m.fixture.fixture_id),['3']);
assert.equal(api.selectRows(p,{...options,scope:'all'},NOW).length,3);
assert.equal(api.signal(p.matches[0],api.rowFor(p.matches[0],'over25_ft')).label,'ORO · STATISTICA');
assert.equal(api.signal(p.matches[1],api.rowFor(p.matches[1],'over25_ft')).label,'Fuori dalla lista top');
const old=fixture(4);old.multimarket.model_id='context-goals-v3.4';
assert.equal(api.selectRows({matches:[old]},options,NOW).length,0);
const finished=fixture(5);finished.fixture.status_short='FT';
assert.equal(api.selectRows({matches:[finished]},options,NOW).length,0);
assert.equal(api.selectRows({matches:[finished]},{...options,phase:'finished'},NOW).length,1);
const host={innerHTML:'',querySelectorAll:()=>[]};
api.render(p,host,{phase:()=> 'upcoming'});
assert(!host.innerHTML.includes('<img'));assert(host.innerHTML.includes('&lt;img'));
assert(host.innerHTML.includes('Quota non disponibile'));
api.render({matches:[]},host);
assert(host.innerHTML.includes('Nessuna partita supera questi criteri'));
console.log('PASS V4: statistical ranking, real selection flags, quote confirmation, old versions, empty lists and escaping');
module.exports={fixture};
