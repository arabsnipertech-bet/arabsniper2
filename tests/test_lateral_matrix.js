const assert=require('node:assert/strict');
const api=require('../web/lateral-matrix.js');
const rows=[{id:'a',family:'totals',period:'ft',category:'mature',market_behavior:'convergence',ev_pct:12,ev_low_pct:5,quote_status:'recent',quality:'supported'},
 {id:'b',family:'combo',period:'ht',category:'contrarian',market_behavior:'divergence',ev_pct:30,ev_low_pct:20,quote_status:'recent',quality:'supported'},
 {id:'c',family:'btts',period:'ht',category:'no_quote',quote_status:'missing'}];
const matches=Array.from({length:160},(_,i)=>({fixture:{fixture_id:String(i+1),kickoff:'2026-10-26T20:00:00Z',match:'Casa '+i+' – Ospite',league:'Serie prova'},multimarket:{model_id:api.MODEL,matrix:{rows:structuredClone(rows)}}}));
const p={matches},opts={...api.state};
assert.equal(api.selectMatches(p,opts,0).length,160);
assert.equal(api.matchingRows(matches[0],{...opts,category:'divergence'}).length,1);
assert.equal(api.matchingRows(matches[0],{...opts,family:'combo'}).length,1);
assert.equal(api.matchingRows(matches[0],{...opts,category:'no_quote'}).length,1);
assert.equal(api.selectMatches(p,{...opts,search:'Casa 159'},0).length,1);
const frozen=JSON.stringify(p);api.selectMatches(p,{...opts,sort:'value'},0);assert.equal(JSON.stringify(p),frozen);
assert(!api.esc('<img src=x onerror=alert(1)>').includes('<img'));
const html=api.marketRow({...rows[0],label:'<script>x</script>',statistical_pct:61,odd:2,first_odd:2.2,margin_pct:4,oci:50},{});
assert(html.includes('&lt;script&gt;'));assert(html.includes('Aggio'));assert(html.includes('OCI'));
console.log('PASS V5 UI: all 160 fixtures, macro behaviors, market/period filters, immutable inputs and escaped text');
