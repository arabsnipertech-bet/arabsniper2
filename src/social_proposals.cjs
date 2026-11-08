'use strict';
const fs=require('fs');
const payload=JSON.parse(fs.readFileSync(0,'utf8'));
if(!Number.isFinite(payload.now))throw new Error('Missing reference time');
Date.now=()=>payload.now;
const matrix=require('../web/lateral-matrix.js');
const out={};
for(const match of payload.matches||[])out[String(match.fixture.fixture_id)]=matrix.proposalData(match);
process.stdout.write(JSON.stringify(out));
