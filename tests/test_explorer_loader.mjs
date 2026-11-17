import {test} from 'node:test';
import assert from 'node:assert/strict';
import {archiveLoader} from '../web/explorer-loader.mjs';
test('archive requests share a global bound and preserve requested output order',async()=>{
 let active=0,max=0,calls=0;const load=archiveLoader(async id=>{calls++;active++;max=Math.max(max,active);await new Promise(r=>setTimeout(r,Number(id)%3));active--;return id;},3);
 const ids=Array.from({length:20},(_,i)=>String(i));
 assert.deepEqual(await Promise.all(ids.map(load)),ids);assert.equal(max,3);assert.equal(calls,20);
 await Promise.all(ids.map(load));assert.equal(calls,20);
});
test('concurrent reads of the same hash fetch once and errors release slots for retry',async()=>{
 let calls=0;const load=archiveLoader(async id=>{calls++;if(calls===1)throw Error('offline');return id;},1);
 const first=load('a');assert.equal(load('a'),first);
 const later=load('b');await assert.rejects(first,/offline/);assert.equal(await later,'b');
 assert.equal(await load('a'),'a');assert.equal(calls,3);
});
