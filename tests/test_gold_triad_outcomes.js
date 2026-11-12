const assert=require('node:assert/strict');
const matrix=require('../web/lateral-matrix.js');
const match=(status='FT',ft=[2,1],ht=[1,1])=>({fixture:{
 fixture_id:'42',status_short:status,
 result:{status,home_ft:ft?.[0],away_ft:ft?.[1],home_ht:ht?.[0],away_ht:ht?.[1]}
}});
const pick=(kind,period,side,line)=>({source:{spec:{kind,period,side,line}},signal:{quote_state:'stable'}});
const settle=(m,p)=>matrix.triadOutcome(m,p);
assert.equal(settle(match(),pick('total','ft','over',2.5)),true);
assert.equal(settle(match(),pick('total','ft','under',2.5)),false);
assert.equal(settle(match(),pick('btts','ft','yes')),true);
assert.equal(settle(match(),pick('btts','ft','no')),false);
assert.equal(settle(match(),pick('result','ft','1')),true);
assert.equal(settle(match(),pick('result','ft','x')),false);
assert.equal(settle(match(),pick('total','ht','over',1.5)),null,
 'Without ArabLive half-time provider, remain neutral instead of inventing a score');
assert.equal(settle(match('1H'),pick('total','ft','over',2.5)),null);
assert.equal(settle(match('AET'),pick('total','ft','over',2.5)),null);
assert.equal(settle(match('PEN'),pick('total','ft','over',2.5)),null);
assert.equal(settle(match('FT',null),pick('total','ft','over',2.5)),null);
assert.equal(settle(match(),pick('asian_total','ft','over',2.25)),null);
assert.equal(settle(match(),pick('handicap','ft','home',-.5)),null);
assert.equal(settle(match(),pick('halves_total','ft','over',.5)),null);
assert.equal(settle(match(),{...pick('total','ft','over',2.5),signal:{quote_state:'model_estimate'}}),null);
const old=globalThis.ArabLive;
try{
 globalThis.ArabLive={fixture:m=>m.fixture,firstHalf:()=>({status:'complete',score:'1–1'})};
 assert.equal(settle(match(),pick('total','ht','over',1.5)),true);
 assert.equal(settle(match(),pick('btts','ht','yes')),true);
 assert.equal(settle(match(),pick('total','sh','over',.5)),true);
 assert.equal(settle(match(),pick('btts','sh','yes')),false);
 assert.equal(settle(match('FT',[0,0],[1,1]),pick('total','sh','over',.5)),null);
}finally{globalThis.ArabLive=old}
console.log('Gold V7 triad settlement tests passed');
