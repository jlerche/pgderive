// Run the actual application module with controlled network failures and a minimal DOM.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
class Element{
 constructor(){this.hidden=false;this.textContent='';this.dataset={};this.style={};this.listeners={};this.children=[]}
 addEventListener(name,fn){this.listeners[name]=fn}
 setAttribute(){} replaceChildren(...nodes){this.children=nodes} append(...nodes){this.children.push(...nodes)}
 scrollIntoView(){} remove(){} getBoundingClientRect(){return{top:0,left:0}}
}
const elements=new Map(),get=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id)};
get('metricsPanel').hidden=true;get('underHood').hidden=true;
get('metricsPanel').dataset={metricsScript:'/metrics.js?v=hash',historyScript:'/history.js?v=hash'};
const callbacks={},statuses={};let imports=0,scripts=0;
const context=vm.createContext({console,Date,Math,BigInt,Number,Error,Set,Map,Promise,Event:class{},
 document:{getElementById:get,createElement:()=>new Element(),body:{append:script=>{scripts++;queueMicrotask(()=>script.onload())}}},
 location:{origin:'https://fixture.invalid'},matchMedia:()=>({matches:true}),window:{dispatchEvent(){}},setInterval(){},
 supabase:{createClient:()=>({channel:name=>({on(_kind,_scope,callback){callbacks[name]=callback;return this},subscribe(callback){statuses[name]=callback}})})},
 fetch:async path=>({ok:true,json:async()=>path==='/config.json'?{table:'all',schema:'all',leaderboardTable:'top',leaderboardSchema:'top',anonKey:'fixture',sql:'SELECT'}:path.startsWith('/rest/')?[]:path==='/activity.json'?{recent:[]}: {queries:{}}})
});
(async()=>{
 const source=fs.readFileSync('examples/portfolio/web/storefront.js','utf8')+'\nexport {money,exact};';
 const module=new vm.SourceTextModule(source,{context,importModuleDynamically:async()=>{
  imports++;if(imports===1)throw Error('temporary module failure');
  const imported=new vm.SourceTextModule('',{context});await imported.link(()=>{});await imported.evaluate();return imported;
 }});
 await module.link(()=>{});await module.evaluate();await new Promise(setImmediate);
 assert.equal(module.namespace.money('9007199254740993'),'$90,071,992,547,409.93');
 assert.equal(module.namespace.money('-1'),'−$0.01');assert.throws(()=>module.namespace.money(Number.MAX_SAFE_INTEGER+1));
 const toggle=get('metricsToggle').listeners.click;
 await toggle();assert.match(get('pageError').textContent,/Metrics could not load/);assert.equal(imports,1);
 await toggle();await toggle();assert.equal(imports,2);assert.equal(scripts,1);assert.equal(get('pageError').textContent,'');
 await toggle();await toggle();assert.equal(imports,2);assert.equal(scripts,1,'successful assets must not be reinitialized');
 callbacks['storefront-totals']({errors:['totals payload failed']});
 callbacks['storefront-top10']({});await new Promise(setImmediate);
 assert.match(get('pageError').textContent,/totals payload failed/,'peer success cannot clear failure');
 callbacks['storefront-totals']({});await new Promise(setImmediate);assert.equal(get('pageError').textContent,'');
 console.log('Production storefront: exact cents, retryable metrics assets and independent Realtime error recovery passed');
})().catch(error=>{console.error(error);process.exitCode=1});
