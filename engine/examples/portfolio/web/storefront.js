const $=id=>document.getElementById(id);
const config=await(await fetch('/config.json',{cache:'no-store'})).json();
const exact=value=>{if(typeof value==='number'&&!Number.isSafeInteger(value))throw Error('Unsafe integral value');if(!/^-?\d+$/.test(String(value)))throw Error('Expected an exact integer');return BigInt(value)};
const count=value=>exact(value).toLocaleString();
const money=value=>{const n=exact(value),a=n<0n?-n:n;return (n<0n?'−':'')+'$'+(a/100n).toLocaleString()+'.'+String(a%100n).padStart(2,'0')};
const lsn=value=>{const[h,l]=value.split('/');return(BigInt('0x'+h)<<32n)+BigInt('0x'+l)};
const state={rows:[],all:[],nodes:new Map(),errors:{},progress:{},activity:null,events:0,connections:{},lastMetrics:0};
const errors=()=>{$('pageError').textContent=Object.values(state.errors).join(' · ')};
const text=(element,value)=>{element.textContent=value;return element};
const element=(tag,value)=>text(document.createElement(tag),value);
const colors={Workspace:'WS',Audio:'AU',Kitchen:'KT',Travel:'TR',Everyday:'ED',Home:'HM'};
function ranking(rows){
 const old=new Map(state.rows.map((row,index)=>[row.product_id,index]));
 const positions=new Map([...state.nodes].filter(([,node])=>node.isConnected).map(([id,node])=>[id,node.getBoundingClientRect()]));
 const maximum=rows.reduce((value,row)=>{const amount=exact(row.net_revenue_cents);return amount>value?amount:value},1n);
 const nodes=rows.map((row,index)=>{
  const id=row.product_id;let node=state.nodes.get(id);
  if(!node){node=document.createElement('article');node.className='product-card';node.innerHTML='<div class="product-top"><span class="product-icon"></span><div class="product-name"><h3></h3><p></p></div><span class="product-rank"></span></div><div class="product-bottom"><div class="product-value"></div><span class="product-change"></span></div><div class="revenue-track"><span></span></div>';state.nodes.set(id,node)}
  node.dataset.product=id;node.dataset.category=row.category;node.dataset.rank=String(index+1);
  node.querySelector('.product-icon').textContent=colors[row.category]||'PR';
  node.querySelector('h3').textContent=row.name;node.querySelector('h3').title=row.name;
  node.querySelector('.product-name p').textContent=row.category+' · '+count(row.net_units)+' net units';
  node.querySelector('.product-rank').textContent=String(index+1).padStart(2,'0');
  node.querySelector('.product-value').textContent=money(row.net_revenue_cents);
  const movement=old.has(id)?old.get(id)-index:null,change=node.querySelector('.product-change');
  change.className='product-change'+(movement===null&&state.rows.length?' new':movement>0?' up':movement<0?' down':'');
  change.textContent=movement===null&&state.rows.length?'↗ ENTERED':movement>0?'↑ '+movement:movement<0?'↓ '+Math.abs(movement):index===0?'● LEADER':'NET REVENUE';
  node.classList.toggle('entered',movement===null&&state.rows.length>0);
  const amount=exact(row.net_revenue_cents);node.querySelector('.revenue-track span').style.width=Number((amount<0n?0n:amount)*1000n/maximum)/10+'%';
  return node;
 });
 $('ranking').replaceChildren(...nodes);
 if(!matchMedia('(prefers-reduced-motion: reduce)').matches)for(const node of nodes){const before=positions.get(node.dataset.product),after=node.getBoundingClientRect();if(before&&(before.top!==after.top||before.left!==after.left))node.animate([{transform:`translate(${before.left-after.left}px,${before.top-after.top}px)`},{transform:'translate(0,0)'}],{duration:650,easing:'cubic-bezier(.2,.8,.2,1)'})}
 const entered=rows.filter(row=>!old.has(row.product_id));const active=new Set(rows.map(row=>row.product_id)),exited=state.rows.filter(row=>!active.has(row.product_id));
 if(state.rows.length&&(entered.length||exited.length))$('rankChanges').replaceChildren(element('span','IN '+entered.map(row=>row.name).join(', ')),element('span','OUT '+exited.map(row=>row.name).join(', ')));
 state.rows=rows;$('rankCount').textContent=rows.length;
 $('topRevenue').textContent=money(rows.reduce((n,row)=>n+exact(row.net_revenue_cents),0n));
 $('topUnits').textContent=count(rows.reduce((n,row)=>n+exact(row.net_units),0n))+' net units';
 $('rankFresh').textContent='Snapshot '+new Date().toLocaleTimeString()+'.';
}
function allProducts(rows){
 state.all=rows;$('allCount').textContent=rows.length+' products';
 $('allProducts').replaceChildren(...rows.map(row=>{const tr=document.createElement('tr');for(const value of[row.name,row.category,count(row.order_lines),count(row.net_units),money(row.net_revenue_cents)])tr.append(element('td',value));return tr}));
 if(rows.length)$('catalogExample').textContent=`Example: ${rows[0].product_id} · ${rows[0].name} · ${rows[0].category}`;
}
const pending={};
async function refresh(name){
 if(pending[name])return;pending[name]=true;
 const spec=name==='top10'?{table:config.leaderboardTable,schema:config.leaderboardSchema,order:'net_revenue_cents.desc.nullslast,product_id.asc'}:{table:config.table,schema:config.schema,order:'product_id.asc'};
 try{const response=await fetch(`/rest/v1/${spec.table}?select=product_id::text,name,category,order_lines::text,net_units::text,net_revenue_cents::text&order=${spec.order}`,{cache:'no-store',headers:{apikey:config.anonKey,Authorization:`Bearer ${config.anonKey}`,'Accept-Profile':spec.schema}});if(!response.ok)throw Error(`${name} snapshot HTTP${response.status}`);const rows=await response.json();for(const row of rows){exact(row.product_id);exact(row.order_lines);exact(row.net_units);exact(row.net_revenue_cents)}
  if(name==='top10')ranking(rows);else allProducts(rows);delete state.errors[name];errors();
 }catch(error){state.errors[name]=error.message;errors()}finally{pending[name]=false}
}
function sourceFeed(){
 if(!state.activity)return;
 const events=(state.activity.recent||[]).flatMap(tx=>(tx.events||[]).map(event=>({...event,tx}))).reverse().slice(0,6);
 $('sourceFeed').replaceChildren(...events.map(event=>{const li=document.createElement('li');li.dataset.kind=event.kind;const top=document.createElement('div');top.className='source-event-top';top.append(element('b',event.title),element('time',new Date(event.tx.committed_at_unix*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'})));li.append(top,element('p',event.detail+(event.unit_price_cents!==undefined?' at '+money(event.unit_price_cents)+' each':'')));
  const published=name=>{try{return state.progress[name]&&lsn(state.progress[name])>=lsn(event.tx.target_lsn)}catch{return false}};
  li.append(element('small',published('totals')&&published('top10')?'BOTH PUBLICATIONS OBSERVED':published('totals')?'ALL PRODUCTS OBSERVED · TOP TEN NOT YET OBSERVED':published('top10')?'TOP TEN OBSERVED · ALL PRODUCTS NOT YET OBSERVED':'SOURCE COMMITTED · AWAITING SAMPLED QUERY PROGRESS'));return li}));
}
async function activity(){try{const response=await fetch('/activity.json',{cache:'no-store'});if(!response.ok)throw Error('Source activity unavailable');state.activity=await response.json();sourceFeed();delete state.errors.activity;errors()}catch(error){state.errors.activity=error.message;errors()}}
async function progress(){try{const response=await fetch('/metrics.json',{cache:'no-store'});if(!response.ok)throw Error('Query progress unavailable');const sample=await response.json();state.lastMetrics=sample.collected_at;for(const name of['totals','top10']){const query=sample.queries?.[name],fresh=query?.flow&&Date.now()/1000-Number(query.flow.observed_at)<60;if(fresh&&query.flow.end_lsn)state.progress[name]=query.flow.end_lsn;else delete state.progress[name];$(name==='totals'?'allQueryState':'topQueryState').textContent=fresh?`Durable LSN ${query.flow.end_lsn} · ${count(query.flow.engine_epochs)} publication epochs · sampled ${Math.round(Date.now()/1000-query.flow.observed_at)}s ago`:'Progress unavailable or stale'}sourceFeed();delete state.errors.progress;errors()}catch(error){state.errors.progress=error.message;errors()}}
function countdown(){const activity=state.activity,now=Date.now()/1000,age=activity?now-activity.observed_at:Infinity,remaining=activity?.phase_started_at+5-now;
 $('feedStatus').textContent=age<5?'SOURCE COMMITTED':age<15?'SOURCE SAMPLE AGING':'SOURCE STALE';
 $('phaseTimer').textContent=age<5&&Number.isFinite(remaining)&&remaining>0?Math.ceil(remaining)+'s':'…';
 $('phaseLabel').textContent=age<5?'Next bulk order':'Awaiting source';$('phaseProgress').style.transform=`scaleX(${Math.max(0,Math.min(1,(remaining||0)/5))})`;
}
let metricsReady=false,historyReady=false,loading=false,loadAttempt=0;
async function loadMetrics(){
 if(loading)return;loading=true;
 try{
  const retry=loadAttempt++?'&retry='+Date.now():'';
  if(!metricsReady){await import($('metricsPanel').dataset.metricsScript+retry);metricsReady=true}
  if(!historyReady)await new Promise((resolve,reject)=>{const script=document.createElement('script');script.src=$('metricsPanel').dataset.historyScript+retry;script.onload=()=>{historyReady=true;resolve()};script.onerror=()=>{script.remove();reject(Error('History script unavailable'))};document.body.append(script)});
  delete state.errors.metricAssets;
 }catch{state.errors.metricAssets='Metrics could not load. Close and reopen Metrics to retry.'}
 finally{loading=false;errors()}
}
for(const[button,panel]of[['hoodToggle','underHood'],['metricsToggle','metricsPanel']])$(button).addEventListener('click',async()=>{const open=$(panel).hidden;$(panel).hidden=!open;$(button).setAttribute('aria-expanded',String(open));if(open&&panel==='metricsPanel')await loadMetrics();if(open){$(panel).scrollIntoView({behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth',block:'start'});window.dispatchEvent(new Event('resize'))}});
$('allSql').textContent=config.sql;$('topSql').textContent=config.sql+'\nORDER BY net_revenue_cents DESC NULLS LAST,\n         p.product_id ASC\nLIMIT 10;';
const client=supabase.createClient(location.origin,config.anonKey,{auth:{persistSession:false,autoRefreshToken:false}});
for(const[name,schema,table]of[['totals',config.schema,config.table],['top10',config.leaderboardSchema,config.leaderboardTable]])client.channel('storefront-'+name).on('postgres_changes',{event:'*',schema,table},payload=>{if(payload.errors?.length){state.errors['realtime_'+name]=payload.errors.join('; ');errors();return}delete state.errors['realtime_'+name];errors();state.events++;$('eventCount').textContent=count(state.events)+' result events';refresh(name)}).subscribe(status=>{state.connections[name]=status;const live=Object.values(state.connections).length===2&&Object.values(state.connections).every(value=>value==='SUBSCRIBED');$('liveStatus').textContent=live?'● CONNECTED':'RECONNECTING';if(status==='SUBSCRIBED'){delete state.errors['realtime_'+name];refresh(name)}else if(['TIMED_OUT','CHANNEL_ERROR','CLOSED'].includes(status))state.errors['realtime_'+name]='Result notifications reconnecting; snapshots continue polling';errors()});
refresh('totals');refresh('top10');activity();progress();countdown();setInterval(()=>{refresh('totals');refresh('top10')},2000);setInterval(activity,1000);setInterval(progress,5000);setInterval(countdown,250);
