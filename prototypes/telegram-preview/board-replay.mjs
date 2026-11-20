import fs from 'node:fs';
import {isDeepStrictEqual} from 'node:util';
import {join} from 'node:path';
import assert from 'node:assert/strict';
import {Miniflare} from 'miniflare';
import {build} from 'esbuild';
const input = process.env.SHADOW_INPUT;
function cpuTicks() {
  let total=0;
  for (const pid of fs.readdirSync('/proc').filter(name=>/^\d+$/.test(name))) {
    try {
      if (fs.readFileSync('/proc/'+pid+'/comm','utf8').trim() !== 'workerd') continue;
      const fields=fs.readFileSync('/proc/'+pid+'/stat','utf8').split(') ')[1].split(' ');
      total += Number(fields[11])+Number(fields[12]);
    } catch (e) { if (!['ENOENT','EACCES','ESRCH'].includes(e.code)) throw e; }
  }
  return total;
}
const {outputFiles} = await build({stdin:{contents:`
  import {readBoard} from './board.mjs';
  export default {async fetch(request,env) {
    return Response.json(await readBoard(env.GOLD,Number(new URL(request.url).pathname.slice(1))));
  }};`, resolveDir:process.cwd()}, bundle:true, format:'esm', write:false});
const mf = new Miniflare({modules:true,script:outputFiles[0].text,r2Buckets:['GOLD'],
  compatibilityDate:'2026-06-01',outboundService:()=>{throw Error('Unexpected outbound call');}});
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
    const t=Date.now(),cpu=cpuTicks();
    const res=await mf.dispatchFetch('http://shadow/'+i);
    assert.equal(res.status,200);
    const actual=await res.json();
    const expected=JSON.parse(fs.readFileSync(join(input,'ref'+i+'.json')));
    if (!isDeepStrictEqual(actual,expected)) throw Error('Board projection mismatch on day '+i);
    console.log('board parity',i,size,'bytes',actual.matches.length,'prematch',Date.now()-t,'ms wall',cpuTicks()-cpu,'workerd CPU ticks');
  }
} finally {await mf.dispose();}
