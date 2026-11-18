import fs from 'node:fs';
import {Readable} from 'node:stream';
import {join} from 'node:path';
import assert from 'node:assert/strict';
import {Miniflare} from 'miniflare';
import {build} from 'esbuild';
const input = process.env.SHADOW_INPUT;
const {outputFiles} = await build({stdin:{contents:`
  import {readBoard} from './board.mjs';
  export default {async fetch(request,env) {
    return Response.json(await readBoard(env.GOLD,Number(new URL(request.url).pathname.slice(1))));
  }};`, resolveDir:process.cwd()}, bundle:true, format:'esm', write:false});
const mf = new Miniflare({modules:true,script:outputFiles[0].text,r2Buckets:['GOLD'],
  compatibilityDate:'2026-11-01',outboundService:()=>{throw Error('Unexpected outbound call');}});
try {
  const bucket = await mf.getR2Bucket('GOLD');
  for (let i=1;i<=5;i++) {
    const key = 'arab_quote_day'+i+'.json';
    const file = join(input,key);
    const size=fs.statSync(file).size;
    // Upload in parts to keep the local R2 test binding bounded on large weekend boards.
    const upload=await bucket.createMultipartUpload(key), parts=[];
    const stream=fs.createReadStream(file,{highWaterMark:8*1024*1024});
    for await (const part of stream) parts.push(await upload.uploadPart(parts.length+1,part));
    await upload.complete(parts);
    const t=Date.now();
    const res=await mf.dispatchFetch('http://shadow/'+i);
    assert.equal(res.status,200);
    const actual=await res.json();
    const expected=JSON.parse(fs.readFileSync(join(input,'ref'+i+'.json')));
    assert.deepEqual(actual,expected);
    console.log('board parity',i,size,'bytes',actual.matches.length,'prematch',Date.now()-t,'ms wall');
  }
} finally {await mf.dispose();}
