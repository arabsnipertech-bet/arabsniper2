import fs from 'node:fs';
import {preview} from './worker.mjs';
const {post} = JSON.parse(fs.readFileSync(0));
const beforeDelivery = {...post,status:'ready',attempts:0,receipt:null,retry_at:null};
const ledger={schema:1,posts:{[post.id]:beforeDelivery}};
const object={size:Buffer.byteLength(JSON.stringify(ledger)),json:async()=>ledger};
const {proposals}=await preview({get:async()=>object},Date.parse(post.created_at));
if(proposals.length!==1) throw Error('Replay did not propose exactly one delivery');
console.log(JSON.stringify(proposals[0]));
