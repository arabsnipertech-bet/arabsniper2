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
