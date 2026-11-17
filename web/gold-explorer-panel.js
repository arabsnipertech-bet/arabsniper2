(()=>{
 const host=document.querySelector('.topbar');if(!host)return;
 const launch=document.createElement('button');launch.type='button';launch.className='btn';launch.textContent='Gold Explorer ↗';launch.setAttribute('aria-haspopup','dialog');host.append(launch);
 const panel=document.createElement('dialog');panel.setAttribute('aria-label','Gold Explorer');panel.style.cssText='position:fixed;inset:0 0 0 auto;margin:0;width:min(860px,100vw);max-width:100vw;height:100dvh;max-height:100dvh;padding:0;background:#09131c;color:white;border:1px solid #28404d;';
 const bar=document.createElement('div');bar.style.cssText='display:flex;justify-content:space-between;padding:10px 16px';
 const link=document.createElement('a');link.href='/gold-explorer.html';link.textContent='Apri Explorer completo ↗';link.style.color='#edc66c';
 const close=document.createElement('button');close.type='button';close.textContent='Chiudi ×';close.onclick=()=>panel.close();bar.append(link,close);panel.append(bar);
 const frame=document.createElement('iframe');frame.title='Gold Explorer';frame.style.cssText='border:0;width:100%;height:calc(100% - 52px)';panel.append(frame);document.body.append(panel);
 launch.onclick=()=>{if(!frame.getAttribute('src'))frame.src='/gold-explorer.html?panel=1';panel.showModal();};
})();
