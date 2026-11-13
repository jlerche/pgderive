(()=>{
'use strict';
const wrapper=document.getElementById('historyProcesses');
let sharedEnd=Date.now()/1000;
const renderers=[];
const renderAll=()=>{sharedEnd=Date.now()/1000;renderers.forEach(render=>render())};
function history(root){
const controls=wrapper?.querySelector('.history-shared-controls')||root;
const grid=root.querySelector('.history-grid'),note=root.querySelector('.history-note');
const colors=root.dataset.historyQuery==='top10'?['#9eb9ed','#d6b8e9','#edaa94']:['#a2e8bd','#9eb9ed','#edaa94','#d6b8e9'];
let data=null,range=3600,category='Overview',query='',processName=root.dataset.historyQuery||'totals',loadSequence=0,loadError='';
const hiddenSeries=new Set();
const readable=s=>s.replaceAll('_',' ').replaceAll('bytes','bytes').replace(/\b(io|rss|wal)\b/gi,x=>x.toUpperCase());
const labels={
 'latency.engine':'Engine processing','latency.write_to_sink':'Receipt to sink',
 'latency.source_commit_to_sink':'Source commit to sink','latency.checkpoint':'Checkpoint',
 'latency.phases.inputs':'Input preparation','latency.phases.prepare_including_object_io':'Engine preparation',
 'latency.phases.publication':'Sink publication','worker.rss_bytes':'Worker RSS',
 'engine.cache.bytes':'Decoded cache','engine.cache.live_bytes':'Live decoded allocations',
 'engine.cache.index_bytes':'Index cache','engine.cache.point_bytes':'Logical-key cache',
 'engine.cache.filter_bytes':'Bloom filters','engine.memtable_bytes':'Memtable',
 'state.bytes':'R2 state size','state.objects':'R2 object count','flow.source_bytes':'Postgres source storage',
 'flow.sink_bytes':'Postgres sink storage','flow.retained_wal_bytes':'Retained source WAL',
 'flow.unconfirmed_wal_bytes':'Unconfirmed source WAL','flow.replay_bytes':'Uncheckpointed replay',
 'flow.products':'Products','flow.order_lines':'Line items','flow.net_units':'Net units','flow.net_revenue_cents':'Net revenue cents',
 'flow.changes_per_second':'Source row operations / second','flow.transactions_per_second':'Generator commits / second',
 'flow.inserts':'INSERT operations','flow.updates':'UPDATE operations','flow.deletes':'DELETE operations',
 'flow.transactions':'Generator commits','flow.generated_source_changes':'Generated source changes',
 'flow.engine_epochs':'Engine epochs','health.errors':'Collector errors',
 'latency.observer.pending':'Observer pending','latency.observer.dropped':'Observer dropped',
 'latency.observer.expired':'Observer expired','latency.observer.generator_samples_skipped':'Observer samples skipped',
 'latency.observer_healthy':'Observer healthy (1 = yes)',
 'engine.cache.index_reads':'Index GETs (cumulative)',
 'engine.cache.index_read_bytes':'Index downloaded bytes (cumulative)',
 'engine.cache.index_requested_bytes':'Index requested bytes (cumulative)',
 'engine.cache.filter_rejections':'Bloom-filter rejections (cumulative)',
 'engine.cache.filter_eviction_steps':'Filter eviction steps (cumulative)',
 'engine.cache.decoded_bytes':'Bytes decoded (cumulative)',
 'engine.cache.decoded_rows':'Rows decoded (cumulative)',
 'engine.cache.hits':'Decoded cache hits (cumulative)',
 'engine.cache.misses':'Decoded cache misses (cumulative)',
 'engine.cache.point_hits':'Logical-key hits (cumulative)',
 'engine.foreground.gets':'Foreground GETs (cumulative)',
 'engine.foreground.fetched_bytes':'Foreground downloaded bytes (cumulative)',
 'engine.foreground.requested_bytes':'Foreground requested bytes (cumulative)',
 'engine.scan.gets':'Maintenance GETs (cumulative)',
 'engine.scan.fetched_bytes':'Maintenance downloaded bytes (cumulative)',
 'engine.scan.requested_bytes':'Maintenance requested bytes (cumulative)'
};
function title(key){
 if(labels[key])return labels[key];
 if(key.startsWith('latency.source_commit_to_sink_by_source_changes.'))return 'Source to sink: '+key.split('.').at(-1)+' changes';
 return key.split('.').map(readable).join(' / ');
}
function unit(key){return key.endsWith('_ms')?'ms':key.endsWith('bytes')?'bytes':key.endsWith('_second')?'/s':'count'}
function group(key){
 if(key.includes('observer.')||key.includes('wal_')||key.includes('replay_bytes'))return 'Backlog';
 if(key.startsWith('latency.'))return 'Latency';
 if(key.startsWith('worker.')||key.includes('.cache.')||key.includes('memtable'))return 'Memory';
 if(key.startsWith('engine.object_service.'))return 'Object I/O';
 if(key.startsWith('engine.foreground.')||key.startsWith('engine.scan.'))return 'Object I/O';
 if(key.startsWith('state.')||key.endsWith('_bytes'))return 'Storage';
 if(key.startsWith('flow.'))return 'Flow';
 return 'Health';
}
function format(v,u){
 if(v==null||!Number.isFinite(v))return 'No sample';
 if(u==='bytes'){
  const powers=['B','KiB','MiB','GiB','TiB'];let n=v,i=0;
  while(Math.abs(n)>=1024&&i<powers.length-1){n/=1024;i++}
  return n.toLocaleString(undefined,{maximumFractionDigits:i?1:0})+' '+powers[i];
 }
 return v.toLocaleString(undefined,{maximumFractionDigits:2})+(u==='ms'?' ms':u==='/s'?' /s':'');
}
function axisFormat(v,u){
 if(u==='ms'&&Math.abs(v)>=1000)return (v/1000).toLocaleString(undefined,{maximumFractionDigits:1})+' s';
 if(u==='count'||u==='/s')return v.toLocaleString(undefined,{notation:'compact',maximumFractionDigits:1})+(u==='/s'?' /s':'');
 return format(v,u);
}
function specs(){
 const used=new Set(),out=[],columns=new Set(data.columns);
 const overview=new Set(['latency.engine','latency.write_to_sink','latency.source_commit_to_sink','worker.rss_bytes','flow.changes_per_second','state.bytes']);
 for(const key of data.columns){
  if(used.has(key))continue;
  if(/\.p\d+_ms$/.test(key)){
   const parent=key.replace(/\.p\d+_ms$/,'');
   const fields=['p50_ms','p95_ms','p99_ms'].map(x=>parent+'.'+x).filter(x=>columns.has(x));
   fields.forEach(x=>used.add(x));
   out.push({id:parent,title:title(parent),category:'Latency',unit:'ms',fields,overview:overview.has(parent)});
  }else{
   used.add(key);out.push({id:key,title:title(key),category:group(key),unit:unit(key),fields:[key],overview:overview.has(key)});
  }
 }
 return out;
}
function draw(canvas,spec,rows,hover=null){
 const rect=canvas.getBoundingClientRect(),dpr=Math.min(2,devicePixelRatio||1),w=rect.width,h=180;
 if(!w)return;
 canvas.width=w*dpr;canvas.height=h*dpr;
 const ctx=canvas.getContext('2d');ctx.scale(dpr,dpr);
 const activeFields=spec.fields.filter(key=>!hiddenSeries.has(key));
 const indices=activeFields.map(key=>data.columns.indexOf(key)+1);
 const values=rows.flatMap(row=>indices.map(i=>row[i]).filter(Number.isFinite));
 const low=Math.min(0,...values),high=Math.max(low+1,...values),left=65,right=w-12,top=12,bottom=h-28;
 const end=sharedEnd,start=end-range;
 const x=t=>left+(t-start)/Math.max(60,end-start)*(right-left),y=v=>bottom-(v-low)/(high-low)*(bottom-top);
 ctx.font='10px "IBM Plex Mono",monospace';ctx.lineWidth=1;
 for(let i=0;i<4;i++){
  const v=low+(high-low)*i/3,py=y(v);ctx.strokeStyle='#263337';ctx.beginPath();ctx.moveTo(left,py);ctx.lineTo(right,py);ctx.stroke();
  ctx.fillStyle='#899b9f';ctx.textAlign='right';ctx.fillText(axisFormat(v,spec.unit),left-7,py+3);
 }
 ctx.textAlign='left';ctx.fillText(new Date(start*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}),left,h-7);
 ctx.textAlign='right';ctx.fillText(new Date(end*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}),right,h-7);
 indices.forEach((index,line)=>{
  const colorIndex=spec.fields.indexOf(activeFields[line]);
  ctx.strokeStyle=colors[colorIndex%colors.length];ctx.lineWidth=colorIndex===0?2:1.3;ctx.beginPath();let previous=null;
  for(const row of rows){
   const value=row[index];
   if(!Number.isFinite(value)){previous=null;continue}
   if(!previous||row[0]-previous[0]>150||((/cumulative/.test(spec.title)||(spec.unit==='count'&&/operations|Generator commits$|Generated source changes|Engine epochs/.test(spec.title)))&&value<previous[index]))ctx.moveTo(x(row[0]),y(value));else ctx.lineTo(x(row[0]),y(value));
   previous=row;
  }
  ctx.stroke();
 });
 if(!values.length){ctx.textAlign='center';ctx.fillStyle='#899b9f';ctx.fillText('No samples in this interval',(left+right)/2,90)}
 if(hover!==null){ctx.strokeStyle='#899b9f';ctx.beginPath();ctx.moveTo(x(hover),top);ctx.lineTo(x(hover),bottom);ctx.stroke()}
 return {x,start,end,left,right};
}
function render(){
 if(!data){if(loadError)note.textContent=loadError;return}
 const end=sharedEnd,rows=data.rows.filter(r=>r[0]>=end-range),all=specs();
 grid.replaceChildren();
 const shown=all.filter(s=>(category==='All'||category==='Overview'&&s.overview||s.category===category)&&(!query||(s.title+' '+s.fields.join(' ')).toLowerCase().includes(query)));
 for(const spec of shown){
  const card=document.createElement('article');card.className='history-card';
  const head=document.createElement('div');head.className='history-card-head';
  const h=document.createElement('h3');h.textContent=(wrapper?(processName==='top10'?'Top ten · ':'All products · '):'')+spec.title;
  const value=document.createElement('strong'),last=rows.at(-1),firstIndex=data.columns.indexOf(spec.fields[0])+1;
  value.textContent=(spec.unit==='ms'?'p50 ':'')+format(last?.[firstIndex],spec.unit);head.append(h,value);
  const canvas=document.createElement('canvas');canvas.setAttribute('role','img');canvas.setAttribute('aria-label',h.textContent+' over time');
  const legend=document.createElement('div');legend.className='history-legend';
  spec.fields.forEach((field,i)=>{const span=document.createElement(spec.fields.length>1?'button':'span');span.style.color=colors[i%colors.length];if(spec.fields.length>1){span.type='button';span.setAttribute('aria-pressed',String(!hiddenSeries.has(field)));span.addEventListener('click',()=>{if(hiddenSeries.has(field))hiddenSeries.delete(field);else hiddenSeries.add(field);span.setAttribute('aria-pressed',String(!hiddenSeries.has(field)));draw(canvas,spec,rows)})}span.textContent=spec.fields.length>1?field.split('.').at(-1).replace('_ms',''):spec.unit==='count'?'Count':spec.unit==='bytes'?'Bytes':'Per second';if(spec.unit==='ms'&&spec.fields.length===1)span.textContent='Milliseconds';legend.append(span)});
  const tip=document.createElement('div');tip.className='history-tooltip';tip.hidden=true;
  card.append(head,canvas,legend,tip);grid.append(card);
  draw(canvas,spec,rows);
  canvas.addEventListener('pointermove',event=>{
   const rect=canvas.getBoundingClientRect(),pixel=event.clientX-rect.left;
   const axes=draw(canvas,spec,rows);if(!axes||!rows.length)return;
   const time=axes.start+(pixel-axes.left)/(axes.right-axes.left)*(axes.end-axes.start);
   const nearest=rows.reduce((a,b)=>Math.abs(b[0]-time)<Math.abs(a[0]-time)?b:a);
   if(Math.abs(nearest[0]-time)>150){tip.hidden=true;return}
   draw(canvas,spec,rows,nearest[0]);tip.hidden=false;
   tip.textContent=new Date(nearest[0]*1000).toLocaleString()+' | '+spec.fields.filter(f=>!hiddenSeries.has(f)).map(f=>f.split('.').at(-1)+': '+format(nearest[data.columns.indexOf(f)+1],spec.unit)).join(' | ');
  });
  canvas.addEventListener('pointerleave',()=>{tip.hidden=true;draw(canvas,spec,rows)});
 }
 const age=end-data.updated_at;
 note.textContent=loadError+(age>90?'History is stale. ':'')+rows.length+' minute samples / '+all.length+' metric charts. '+(rows.length?'Available since '+new Date(rows[0][0]*1000).toLocaleString()+'. ':'')+'Blank intervals mean no fresh sample. Latency points summarize the preceding 60 seconds; they are not whole-day percentiles. Counters can reset on restart.';
 if(!shown.length){const p=document.createElement('p');p.textContent='No matching metrics.';grid.append(p)}
}
controls.querySelectorAll('[data-history-range]').forEach(button=>button.addEventListener('click',()=>{range=Number(button.dataset.historyRange);controls.querySelectorAll('[data-history-range]').forEach(b=>b.setAttribute('aria-pressed',String(b===button)));render()}));
controls.querySelector('[name=historyCategory]').addEventListener('change',event=>{category=event.target.value;render()});
controls.querySelector('[name=historySearch]').addEventListener('input',event=>{query=event.target.value.trim().toLowerCase();render()});
root.querySelector('[data-history-download]').addEventListener('click',()=>{
 if(!data)return;
 const rows=[['time',...data.columns],...data.rows.map(row=>[new Date(row[0]*1000).toISOString(),...row.slice(1)])];
 const url=URL.createObjectURL(new Blob([rows.map(row=>row.map(v=>v??'').join(',')).join('\n')],{type:'text/csv'}));
 const a=document.createElement('a');a.href=url;a.download='pgderive-'+processName+'-metrics-24h.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
});
let resize;
window.addEventListener('resize',()=>{clearTimeout(resize);resize=setTimeout(render,150)});
async function load(){
 const sequence=++loadSequence,name=processName;
 try{
  const response=await fetch(name==='totals'?'/metrics-history.json':'/metrics-history-top10.json',{cache:'no-store'});
  if(!response.ok)throw Error('History unavailable');
  const next=await response.json();
  if(next.version!==1||!Array.isArray(next.rows)||!Array.isArray(next.columns))throw Error('Invalid history');
  if(sequence!==loadSequence)return;
  data=next;loadError='';renderAll();
 }catch(error){if(sequence===loadSequence){loadError='History unavailable for this query. Retrying in one minute. ';render()}}
}
root.addEventListener('history-visible',render);
renderers.push(render);
load();setInterval(load,60000);
}
document.querySelectorAll('.metric-history').forEach(history);
wrapper?.querySelector('[name=historyQuery]').addEventListener('change',event=>{
 const name=event.target.value;
 wrapper.querySelector('.history-processes').classList.toggle('history-single',name!=='both');
 wrapper.querySelectorAll('.metric-history').forEach(section=>{
  section.hidden=name!=='both'&&section.dataset.historyQuery!==name;
  if(!section.hidden)section.dispatchEvent(new Event('history-visible'));
 });
});
})();
