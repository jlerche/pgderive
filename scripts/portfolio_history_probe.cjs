// Execute the production renderer with independent responses and a minimal DOM.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const now=2000000000,timers=[],canvases=[];
class Element{
 constructor(tag='div'){this.tag=tag;this.children=[];this.dataset={};this.style={};this.listeners={};this.textContent='';this.hidden=false;this.classList={toggle(){}}}
 append(...nodes){this.children.push(...nodes)}
 replaceChildren(...nodes){this.children=nodes}
 setAttribute(){}
 addEventListener(name,fn){(this.listeners[name]??=[]).push(fn)}
 getBoundingClientRect(){return {width:500,left:0}}
 getContext(){const labels=[];this.axisLabels=labels;return new Proxy({fillText(text){labels.push(text)}},{get:(o,k)=>k in o?o[k]:()=>{}})}
}
const ranges=['3600','21600','86400'].map(value=>{const el=new Element('button');el.dataset.historyRange=value;return el});
const category=new Element('select'),search=new Element('input'),select=new Element('select');
const controls={querySelectorAll:()=>ranges,querySelector:key=>key.includes('Category')?category:search};
function root(name){const grid=new Element(),note=new Element(),download=new Element('button'),el=new Element();el.dataset.historyQuery=name;el.querySelector=key=>key==='.history-grid'?grid:key==='.history-note'?note:download;el.grid=grid;el.note=note;return el}
const totals=root('totals'),top=root('top10'),roots=[totals,top];
const wrapper={querySelector:key=>key.includes('shared-controls')?controls:key.includes('historyQuery')?select:new Element(),querySelectorAll:()=>roots};
let failTop=false;
const document={getElementById:()=>wrapper,querySelectorAll:()=>roots,createElement:tag=>{const el=new Element(tag);if(tag==='canvas')canvases.push(el);return el}};
class Clock extends Date{static now(){return now*1000}}
vm.runInNewContext(fs.readFileSync('examples/portfolio/web/history.js','utf8'),{
 document,Date:Clock,devicePixelRatio:1,window:{addEventListener(){}},setInterval:fn=>timers.push(fn),setTimeout(){},clearTimeout(){},Event:class{},
 fetch:async path=>({ok:!(failTop&&path.includes('top10')),json:async()=>({version:1,updated_at:now,columns:['worker.rss_bytes'],rows:[[now-(path.includes('top10')?1800:3600),path.includes('top10')?222:111]]})})
});
const flush=()=>new Promise(resolve=>setImmediate(resolve));
(async()=>{
 await flush();
 assert.equal(totals.grid.children.length,1);assert.equal(top.grid.children.length,1);
 assert.match(totals.grid.children[0].children[0].children[0].textContent,/Totals/);
 assert.match(top.grid.children[0].children[0].children[0].textContent,/Top ten/);
 const axis=el=>el.grid.children[0].children[1].axisLabels.slice(-2);
 assert.deepEqual(axis(totals),axis(top),'different history starts must not change the selected time axis');
 failTop=true;timers.forEach(fn=>fn());await flush();
 assert.match(top.note.textContent,/History unavailable/);assert.doesNotMatch(totals.note.textContent,/History unavailable/);
 // A successful peer refresh must not clear the top-ten failure.
 timers[0]();await flush();assert.match(top.note.textContent,/History unavailable/);
 failTop=false;timers[1]();await flush();assert.doesNotMatch(top.note.textContent,/History unavailable/);
 console.log('Production history: separate query values, shared axes and isolated fetch errors passed');
})().catch(error=>{console.error(error);process.exitCode=1});
