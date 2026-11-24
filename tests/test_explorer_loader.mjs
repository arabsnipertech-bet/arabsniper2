import {test} from 'node:test';
import assert from 'node:assert/strict';
import {archiveLoader,currentPageHashes} from '../web/explorer-loader.mjs';
test('archive requests share a global bound and preserve requested output order',async()=>{
 let active=0,max=0,calls=0;const load=archiveLoader(async id=>{calls++;active++;max=Math.max(max,active);await new Promise(r=>setTimeout(r,Number(id)%3));active--;return id;},3);
 const ids=Array.from({length:20},(_,i)=>String(i));
 assert.deepEqual(await Promise.all(ids.map(load)),ids);assert.equal(max,3);assert.equal(calls,20);
 await Promise.all(ids.map(load));assert.equal(calls,20);
});
test('current index loads all and only pages covering the requested date/fixture; legacy stays compatible',()=>{
 const a='a'.repeat(64),b='b'.repeat(64),c='c'.repeat(64);
 const index={count:12,pages:[a,b,c],page_index_version:'fixture-pages-v1',page_index:[
  {sha256:a,count:4,dates:['2026-11-24'],fixtures:['1','2']},
  {sha256:b,count:4,dates:['2026-11-24','2026-11-25'],fixtures:['2','3']},
  {sha256:c,count:4,dates:['2026-11-25'],fixtures:['3','4']} ]};
 assert.deepEqual(currentPageHashes(index,{dates:['2026-11-24']}),[a,b]);
 assert.deepEqual(currentPageHashes(index,{fixture:'2'}),[a,b]);
 assert.deepEqual(currentPageHashes(index,{dates:['2026-11-25'],fixture:'4'}),[c]);
 assert.deepEqual(currentPageHashes(index,{fixture:'999'}),[]);
 assert.deepEqual(currentPageHashes(index),[a,b,c]);
 assert.deepEqual(currentPageHashes({pages:[a,b]},{fixture:'1'}),[a,b]);
 for(const broken of [{...index,count:13},{...index,page_index:index.page_index.slice(1)},
  {...index,page_index:[{...index.page_index[0],fixtures:[]},...index.page_index.slice(1)]}])assert.throws(()=>currentPageHashes(broken),/Indice/);
});
test('concurrent reads of the same hash fetch once and errors release slots for retry',async()=>{
 let calls=0;const load=archiveLoader(async id=>{calls++;if(calls===1)throw Error('offline');return id;},1);
 const first=load('a');assert.equal(load('a'),first);
 const later=load('b');await assert.rejects(first,/offline/);assert.equal(await later,'b');
 assert.equal(await load('a'),'a');assert.equal(calls,3);
});
