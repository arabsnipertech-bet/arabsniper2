import test from 'node:test';
import assert from 'node:assert/strict';
import {officialGoldRecords} from '../web/explorer-official-records.mjs';
test('Official Gold view copies recorded roles without Explorer qualification or scoring',()=>{
 const official=Object.freeze({id:'a',kickoff:'2026-11-17T13:00:00Z',roles:['recommended_alternative'],ranking_roles:[],validated:false,quote_usable:false,odd:2,probability:.6});
 const other=Object.freeze({id:'b',kickoff:'2026-11-17T12:00:00Z',roles:['most_probable'],ranking_roles:['recommended_alternative'],validated:true});
 const later=Object.freeze({...official,id:'c',kickoff:'2026-11-17T14:00:00Z',probability:.99});
 const source=Object.freeze([later,other,official]);
 const result=officialGoldRecords(source);
 assert.deepEqual(result,[official,later]);assert.strictEqual(result[0],official);
 assert.deepEqual(source,[later,other,official]);assert.deepEqual(officialGoldRecords([{roles:[]},{}]),[]);
});
