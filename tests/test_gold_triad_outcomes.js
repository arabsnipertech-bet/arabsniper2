const assert=require('node:assert/strict');
const matrix=require('../web/lateral-matrix.js');
const match=(status='FT',ft=[2,1],ht=[1,1])=>({fixture:{
 fixture_id:'42',status_short:status,
 result:{status,home_ft:ft?.[0],away_ft:ft?.[1],home_ht:ht?.[0],away_ht:ht?.[1]}
}});
const pick=(kind,period,side,line,extra)=>({source:{spec:{kind,period,side,line,extra}},signal:{quote_state:'stable'}});
const settle=(m,p)=>matrix.triadOutcome(m,p);
assert.equal(settle(match(),pick('total','ft','over',2.5)),true);
assert.equal(settle(match(),pick('total','ft','under',2.5)),false);
assert.equal(settle(match('FT',[0,2],[0,1]),pick('asian_total','ft','under',3.5)),true,
 'Czech Republic–Spain: the Asian half-line has the same outcome as Under 3.5 FT');
assert.equal(settle(match('FT',[2,2]),pick('asian_total','ft','under',3.5)),false);
assert.equal(settle(match(),pick('btts','ft','yes')),true);
assert.equal(settle(match(),pick('btts','ft','no')),false);
assert.equal(settle(match(),pick('result','ft','1')),true);
assert.equal(settle(match(),pick('result','ft','x')),false);
assert.equal(settle(match(),pick('total','ht','over',1.5)),null,
 'Without ArabLive half-time provider, remain neutral instead of inventing a score');
assert.equal(settle(match('1H'),pick('total','ft','over',2.5)),null);
const navalcarnero=match('AET',null,[0,1]);
navalcarnero.fixture.live={status:'AET',goals:{home:2,away:3},fulltime:{home:2,away:2}};
assert.equal(settle(navalcarnero,pick('result','ft','x')),true);
assert.equal(settle(navalcarnero,pick('result','ft','2')),false);
assert.equal(settle(navalcarnero,pick('btts','ft','yes')),true);
const penalties=match('PEN',[1,1],[1,0]);
penalties.fixture.live={status:'PEN',goals:{home:4,away:5}};
assert.equal(settle(penalties,pick('result','ft','x')),true,
 'A saved regulation result remains usable when the live feed has only the penalty result');
const missing90=match('AET',null,[0,1]);
missing90.fixture.live={status:'AET',goals:{home:2,away:3},fulltime:{home:null,away:null}};
assert.equal(settle(missing90,pick('btts','ft','yes')),null);
assert.equal(settle(match('FT',null),pick('total','ft','over',2.5)),null);
assert.equal(settle(match(),pick('asian_total','ft','over',2.25)),null);
assert.equal(settle(match(),pick('handicap','ft','home',-.5)),null);
assert.equal(settle(match(),pick('halves_total','ft','over',.5,.5)),null);
assert.equal(settle(match(),{...pick('total','ft','over',2.5),signal:{quote_state:'model_estimate'}}),true);
const old=globalThis.ArabLive;
try{
 globalThis.ArabLive={fixture:m=>m.fixture,firstHalf:m=>({status:'complete',score:`${m.fixture.result.home_ht}–${m.fixture.result.away_ht}`})};
 assert.equal(settle(match('FT',[2,1],[1,0]),pick('asian_total','ht','under',1.5)),true,
  'Guinea–Eritrea: the 1–0 half-time score settles the Asian Under 1.5 PT');
 assert.equal(settle(navalcarnero,pick('total','sh','over',1.5)),true);
 assert.equal(settle(navalcarnero,pick('btts','sh','yes')),true);
 assert.equal(settle(match(),pick('total','ht','over',1.5)),true);
 assert.equal(settle(match(),pick('btts','ht','yes')),true);
 assert.equal(settle(match(),pick('total','sh','over',.5)),true);
 assert.equal(settle(match(),pick('btts','sh','yes')),false);
 assert.equal(settle(match('FT',[0,0],[1,1]),pick('total','sh','over',.5)),null);
 assert.equal(settle(match(),pick('total','ft','under',3.5)),true);
 assert.equal(settle(match('FT',[4,1],[2,0]),pick('total','ft','under',3.5)),false);
 assert.equal(settle(match(),pick('halves_total','ft','over',.5,.5)),true);
 assert.equal(settle(match('FT',[1,0],[1,0]),pick('halves_total','ft','over',.5,.5)),false);
 assert.equal(settle(match(),pick('pt_ft_total','ft','over',1.5,2.5)),true);
 assert.equal(settle(match('FT',[2,0],[1,0]),pick('pt_ft_total','ft','over',1.5,2.5)),false);
 assert.equal(settle(match(),pick('pt_btts_ft_total','ft','yes',2.5)),true);
 assert.equal(settle(match('FT',[2,0],[1,0]),pick('pt_btts_ft_total','ft','yes',2.5)),false);
 assert.equal(settle(match(),pick('both_halves','ft','both')),false);
 assert.equal(settle(match('FT',[2,2],[1,1]),pick('both_halves','ft','both')),true);
}finally{globalThis.ArabLive=old}
console.log('Gold V7 triad settlement tests passed');
