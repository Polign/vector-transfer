import {writeFileSync,mkdirSync} from 'node:fs';
mkdirSync('.deploy',{recursive:true,mode:0o700});
import assert from 'node:assert/strict';
const target=await (await fetch('http://127.0.0.1:23816/json/new?about:blank',{method:'PUT'})).json();
const ws=new WebSocket(target.webSocketDebuggerUrl);
await new Promise((resolve,reject)=>{ws.onopen=resolve;ws.onerror=reject});
let id=0;const pending=new Map();const exceptions=[];
ws.onmessage=e=>{const m=JSON.parse(e.data);if(m.id){const p=pending.get(m.id);pending.delete(m.id);m.error?p.reject(m.error):p.resolve(m.result)}if(m.method==='Runtime.exceptionThrown')exceptions.push(m.params.exceptionDetails.text)};
function call(method,params={}){return new Promise((resolve,reject)=>{const n=++id;pending.set(n,{resolve,reject});ws.send(JSON.stringify({id:n,method,params}))})}
async function evaluate(expression){const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw new Error(JSON.stringify(r.exceptionDetails));return r.result.value}
async function until(expression){for(let i=0;i<80;i++){if(await evaluate(expression))return;await new Promise(r=>setTimeout(r,250))}throw new Error('Timed out waiting for: '+expression)}

// Run with a local preview on :23825 and an isolated Chrome debug port :23816.
try {
 await call('Page.enable');await call('Runtime.enable');await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1050,deviceScaleFactor:1,mobile:false});
 await call('Page.addScriptToEvaluateOnNewDocument',{source:`
 const workerID='a'.repeat(32);
 const pair={source:'production-source',sink:'polign-destination',dimension:1536};
 const manifest={pairs:[pair],connections:[{name:pair.source,kind:'pinecone',can_read:true},{name:pair.sink,kind:'polign',can_write:true}]};
 window.fixture={requests:[],expired:false,failPoll:false,workers:(location.search.includes('worker=')||sessionStorage.getItem('transfer-job-return'))?[{id:workerID,name:'production-transfer',status:'online',manifest}]:[],job:null};
 const originalFetch=window.fetch.bind(window);
 window.fetch=async(path,options={})=>{let data,status=200;const method=options.method||'GET';
 if(path==='/auth/config')data={mode:'polign',account_url:'https://account.polign.com'};
 else if(path==='/auth/session')data={email:'fixture@example.test',csrf_token:'fixture-csrf'};
 else if(path==='/v1/workers'&&method==='GET'){if(fixture.failPoll)throw new Error('Network unavailable');if(fixture.expired){status=401;data={error:'Sign in required'};}else data={enabled:true,workers:fixture.workers};}
 else if(path==='/v1/workers'&&method==='POST'){fixture.requests.push({path,body:JSON.parse(options.body),headers:options.headers});if(fixture.expired){status=401;data={error:'Sign in required'};}else{const worker={id:workerID,name:JSON.parse(options.body).name,status:'awaiting enrollment',manifest:{}};fixture.workers=[worker];data={worker,enrollment_token:workerID+'.'+'b'.repeat(64),expires_in:600};}}
 else if(path==='/v1/connections')data={connections:[],personal_connections_enabled:true,hosted_execution_enabled:true};
 else if(path.startsWith('/v1/jobs?'))data={jobs:fixture.job?[fixture.job]:[],total:fixture.job?1:0};
 else if(path==='/v1/jobs'&&method==='POST'){fixture.requests.push({path,body:JSON.parse(options.body),headers:options.headers});fixture.job={id:'job-fixture',spec:JSON.parse(options.body),state:'queued',records:0,batches:0,retries:0,runs:0,created_at:new Date().toISOString(),updated_at:new Date().toISOString()};data=fixture.job;}
 else if(path==='/v1/jobs/job-fixture')data=fixture.job;
 else if(path.includes('/events?'))data={events:[],next_after:0};
 else return originalFetch(path,options);
 return new Response(JSON.stringify(data),{status,headers:{'Content-Type':'application/json'}});};
 window.readyFixture=()=>{fixture.workers[0].status='online';fixture.workers[0].manifest=manifest;};
 `});
 const base='http://127.0.0.1:23825';
 await call('Page.navigate',{url:base+'/'});await until('!document.querySelector("#new-job")?.disabled');
 await evaluate('document.querySelector("#new-job").click()');
 assert.equal(await evaluate('document.querySelector("#job-settings").hidden'),true);
 assert.equal(await evaluate('document.querySelector("#execution-next").getAttribute("href")'),'/connections/new');
 await call('Page.navigate',{url:base+'/connections/new'});await until('!document.querySelector("#connection-form")?.hidden');
 assert.equal(await evaluate('document.querySelectorAll("input[type=password]").length'),0);
 await evaluate(`const f=document.querySelector('#connection-form');f.elements.dimension.value='1536';f.elements.source_endpoint.value='https://private-source.internal';f.elements.sink_endpoint.value='https://private-polign.internal';f.elements.sink_collection.value='documents';f.requestSubmit()`);
 await until('document.querySelector("#worker-status-title").textContent.includes("Waiting for")');
 assert.equal(await evaluate('document.querySelector("#connection-form").hidden'),true);
 assert.equal(await evaluate('document.querySelector("#continue-transfer").hidden'),true);
 assert.deepEqual(await evaluate('fixture.requests[0].body'),{name:'production-transfer'});
 assert.equal(await evaluate('fixture.requests[0].headers["X-CSRF-Token"]'),'fixture-csrf');
 // A failed status check gives a retry action; it never fabricates readiness.
 await evaluate('fixture.failPoll=true;checkWorker()');await until('!document.querySelector("#setup-error").hidden');
 assert.equal(await evaluate('document.querySelector("#continue-transfer").hidden'),true);
 await evaluate('fixture.failPoll=false;fixture.workers[0].status="enrollment expired";checkWorker()');
 await until('document.querySelector("#worker-status-title").textContent.includes("expired")');
 assert.equal(await evaluate('generatedScript'), '');
 await evaluate('document.querySelector("#restart-setup").click()');
 assert.equal(await evaluate('document.querySelector("[name=source_endpoint]").value'),'https://private-source.internal');
 await evaluate('document.querySelector("#connection-form").requestSubmit()');await until('!document.querySelector("#setup-result").hidden');
 await evaluate('readyFixture();checkWorker()');await until('!document.querySelector("#continue-transfer").hidden');
 assert.equal(await evaluate('generatedScript'), '');
 const next=await evaluate('document.querySelector("#continue-transfer").getAttribute("href")');
 writeFileSync('.deploy/flow-ready-desktop.png',Buffer.from((await call('Page.captureScreenshot')).data,'base64'));
 await call('Page.navigate',{url:base+next});await until('document.querySelector("#job-dialog")?.open');
 assert.equal(await evaluate('document.querySelector("#execution-worker").value'),'a'.repeat(32));
 assert.deepEqual(await evaluate('({source:document.querySelector("[name=source]").value,sink:document.querySelector("[name=sink]").value,dimension:document.querySelector("[name=dimension]").value})'),{source:'production-source',sink:'polign-destination',dimension:'1536'});
 assert.equal(await evaluate('document.querySelector("[name=dimension]").readOnly'),true);
 // Hosted demo defaults must never overwrite an approved worker dimension.
 await evaluate('connections.push({name:"demo-source",kind:"demo",can_read:true});document.querySelector("#close-form").click()');
 await until('document.querySelector("[name=dimension]").value===""');
 await evaluate('openForm("a".repeat(32))');
 assert.equal(await evaluate('document.querySelector("[name=dimension]").value'),'1536');
 assert.ok(await evaluate('document.querySelector("[name=name]").value'));
 await evaluate('fixture.workers[0].status="offline";refresh()');await until('document.querySelector("#job-settings").hidden');
 assert.ok((await evaluate('document.querySelector("#execution-next").getAttribute("href")')).includes('worker='));
 await evaluate('fixture.workers[0].status="online";refresh()');await until('!document.querySelector("#job-settings").hidden');
 await call('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});
 assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true);
 writeFileSync('.deploy/flow-job-mobile.png',Buffer.from((await call('Page.captureScreenshot')).data,'base64'));
 await evaluate('document.querySelector("#job-form").requestSubmit()');await until('!document.querySelector("#detail").hidden && !document.querySelector("#job-dialog").open');
 const requests=await evaluate('fixture.requests');assert.equal(requests.length,1);assert.equal(requests[0].path,'/v1/jobs');assert.deepEqual(Object.keys(requests[0].body).sort(),['batch_size','dimension','name','sink','source','worker_id']);
 // Retiring a worker must not make a completed job look stuck.
 await evaluate('fixture.job.state="succeeded";fixture.workers[0].status="revoked";refresh()');await until('!refreshing');
 assert.equal(await evaluate('document.querySelector("#jobs").textContent.includes("awaiting reconnection")'),false);
 await evaluate('fixture.workers[0].status="online";refresh()');await until('!refreshing');
 // An expired job session returns to its selected worker after sign-in.
 await evaluate('openForm("a".repeat(32));fixture.expired=true;refresh()');await until('document.querySelector("#workspace").hidden');
 assert.equal(await evaluate('sessionStorage.getItem("transfer-job-return")'),'a'.repeat(32));
 await call('Page.navigate',{url:base+'/'});await until('document.querySelector("#job-dialog")?.open');
 assert.equal(await evaluate('document.querySelector("#execution-worker").value'),'a'.repeat(32));
 await evaluate('document.querySelector("#close-form").click()');
 // Hosted fields survive background refresh and remain an explicit choice.
 await evaluate('document.querySelector("#new-job").click();document.querySelector("#execution-worker").value="";document.querySelector("#execution-worker").dispatchEvent(new Event("change"));document.querySelector("[name=source_api_key]").value="fake-browser-key";refresh()');
 await until('!refreshing');assert.equal(await evaluate('document.querySelector("[name=source_api_key]").value'),'fake-browser-key');
 assert.equal(await evaluate('document.querySelector("[name=source_kind]").options.length'),15);
 // Reloading a worker setup URL restores monitoring, not enrollment secrets.
 await call('Page.navigate',{url:base+'/connections/new?worker='+'a'.repeat(32)});await until('!document.querySelector("#continue-transfer")?.hidden');
 assert.equal(await evaluate('document.querySelector("#connection-form").hidden'),true);
 await evaluate('fixture.workers[0].status="offline";checkWorker()');await until('!document.querySelector("#worker-restart").hidden');
 assert.ok((await evaluate('document.querySelector("#worker-restart-command").textContent')).includes('run-worker.sh'));
 // Setup auth expiry clears the script and keeps only a safe return identifier.
 await evaluate('fixture.expired=true;checkWorker()');await until('!document.querySelector("#setup-login").hidden');
 assert.equal(await evaluate('generatedScript'),'');
 await evaluate('document.querySelector("#setup-sign-in").onclick()');
 assert.equal(await evaluate('sessionStorage.getItem("transfer-setup-return")'),'a'.repeat(32));
 await call('Page.navigate',{url:base+'/'});
 await new Promise(r=>setTimeout(r,500));
 await until('location.pathname==="/connections/new" && document.querySelector("#continue-transfer") && !document.querySelector("#continue-transfer").hidden');
 assert.equal(await evaluate('sessionStorage.length'),0);
 assert.equal(await evaluate('localStorage.length'),0);assert.equal(exceptions.length,0);
 console.log(JSON.stringify({emptyState:true,localSettingsOnly:true,expiredSetupRecovery:true,networkRetry:true,workerReadiness:true,prefilledJob:true,offlineRecovery:true,jobSubmitted:true,hostedDraftPreserved:true,reloadRecovery:true,signInReturn:true,mobileOverflow:false,jsExceptions:0}));
}finally{await call('Browser.close');ws.close()}
