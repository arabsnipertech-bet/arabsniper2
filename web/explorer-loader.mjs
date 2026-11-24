// One global bound for archive reads, including nested historical snapshots.
// Immutable hashes share both pending and completed requests. Failures can retry.
export function archiveLoader(fetchObject,limit=12){
 const cache=new Map(),queue=[];let active=0;
 const acquire=()=>active<limit?(active++,Promise.resolve()):new Promise(resolve=>queue.push(resolve));
 const release=()=>{const next=queue.shift();if(next)next();else active--;};
 return function load(hash){
  if(!cache.has(hash)){
   const pending=(async()=>{await acquire();try{return await fetchObject(hash);}finally{release();}})();
   cache.set(hash,pending);pending.catch(()=>{if(cache.get(hash)===pending)cache.delete(hash);});
  }
  return cache.get(hash);
 };
}

// Old publications still load in full. A bad new index must never yield a
// falsely empty or incomplete ranking. Admission/ranking remain in core.
export function currentPageHashes(index,{dates=null,fixture=null}={}){
 const pages=index.pages||[];
 if(!index.page_index_version)return pages;
 const headers=index.page_index;
 if(index.page_index_version!=='fixture-pages-v1'||!Array.isArray(headers)||headers.length!==pages.length||new Set(pages).size!==pages.length||
  headers.some((p,i)=>p.sha256!==pages[i]||!/^[a-f0-9]{64}$/.test(p.sha256)||!Number.isInteger(p.count)||p.count<1||
   !Array.isArray(p.dates)||!p.dates.length||p.dates.some(d=>!/^\d{4}-\d{2}-\d{2}$/.test(d))||
   !Array.isArray(p.fixtures)||!p.fixtures.length||p.fixtures.some(f=>typeof f!=='string'||!/^\d+$/.test(f)))||
  headers.reduce((n,p)=>n+p.count,0)!==index.count)throw Error('Indice Explorer incompleto. Premi Aggiorna dati.');
 return headers.filter(p=>(!dates||p.dates.some(d=>dates.includes(d)))&&(!fixture||p.fixtures.includes(String(fixture)))).map(p=>p.sha256);
}
