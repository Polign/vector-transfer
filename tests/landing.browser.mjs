// Run against a fresh local `serve -demo` process. Chrome must expose CDP on :23836.
import assert from 'node:assert/strict';
import {mkdirSync, writeFileSync} from 'node:fs';
const base = process.env.TRANSFER_PREVIEW_URL || 'http://127.0.0.1:23856';
const debug = process.env.TRANSFER_BROWSER_URL || 'http://127.0.0.1:23836';
mkdirSync('.deploy/landing-release', {recursive:true});
const target = await (await fetch(`${debug}/json/new?about:blank`, {method:'PUT'})).json();
const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((resolve,reject) => { ws.onopen=resolve; ws.onerror=reject; });
let id=0; const pending=new Map(), exceptions=[];
ws.onmessage=e=>{const m=JSON.parse(e.data);if(m.id){const p=pending.get(m.id);pending.delete(m.id);m.error?p.reject(m.error):p.resolve(m.result);}if(m.method==='Runtime.exceptionThrown')exceptions.push(m.params.exceptionDetails.text);};
function call(method,params={}) { return new Promise((resolve,reject)=>{const n=++id;pending.set(n,{resolve,reject});ws.send(JSON.stringify({id:n,method,params}));}); }
async function evaluate(expression) { const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw new Error(JSON.stringify(r.exceptionDetails));return r.result.value; }
async function until(expression) { for(let i=0;i<100;i++){if(await evaluate(expression))return;await new Promise(r=>setTimeout(r,200));}throw new Error(`Timed out: ${expression}`); }
async function screenshot(name) { writeFileSync(`.deploy/landing-release/${name}.png`,Buffer.from((await call('Page.captureScreenshot',{captureBeyondViewport:true})).data,'base64')); }
try {
  await call('Page.enable'); await call('Runtime.enable');
  await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1050,deviceScaleFactor:1,mobile:false});
  const fixture=await call('Page.addScriptToEvaluateOnNewDocument',{source:`const originalFetch=window.fetch.bind(window);window.fetch=(path,options)=>path==='/auth/config'?Promise.resolve(new Response(JSON.stringify({mode:'polign',account_url:'https://account.polign.com'}))):path==='/auth/session'?Promise.resolve(new Response('{}',{status:401})):originalFetch(path,options);`});
  await call('Page.navigate',{url:base+'/'});
  await until('document.querySelector("#access").textContent === "Sign in" && !document.querySelector("#sign-in").hidden');
  assert.equal(await evaluate('document.querySelector("#workspace").hidden'),true);
  assert.equal(await evaluate('document.querySelectorAll(".provider-list li").length'),15);
  assert.equal(await evaluate('document.querySelectorAll(".provider-list li").length === Object.keys(providerNames).length'),true);
  assert.equal(await evaluate('document.querySelector(".landing-actions a").getAttribute("href")'),'#try-demo');
  for(const platform of ['linux-amd64','linux-arm64','darwin-arm64']) {
    await evaluate(`document.querySelector('#demo-platform').value=${JSON.stringify(platform)};document.querySelector('#demo-platform').dispatchEvent(new Event('change'));`);
    const cmd=await evaluate('document.querySelector("#demo-command").textContent');
    assert.ok(cmd.includes(`./vtransfer-${platform} serve -demo -data ./transfer-demo`));
    assert.ok(cmd.includes(platform.startsWith('linux')?'sha256sum':'shasum -a 256'));
    writeFileSync(`.deploy/landing-release/demo-${platform}.sh`,cmd+'\n');
  }
  // A clipboard denial still leaves a usable manual-copy path.
  await evaluate(`Object.defineProperty(navigator,'clipboard',{configurable:true,value:{writeText:async()=>{throw new Error('Denied');}}});document.querySelector('#copy-demo').click();`);
  await until('document.querySelector("#copy-demo-status").textContent.includes("Commands selected")');
  assert.equal(await evaluate('getSelection().toString()'),await evaluate('document.querySelector("#demo-command").textContent'));
  await evaluate('getSelection().removeAllRanges();document.querySelector("#demo-platform").dispatchEvent(new Event("change"))');
  for(const width of [1440,768,390,320]) {
    await call('Emulation.setDeviceMetricsOverride',{width,height:1050,deviceScaleFactor:1,mobile:width<650});
    assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true,`Landing overflow at ${width}`);
    if(width===1440||width===390)await screenshot(`landing-${width}`);
  }
  await call('Page.navigate',{url:base+'/docs.html'});await until('document.querySelector("#databases")');
  assert.equal(await evaluate('document.querySelectorAll("#databases tbody tr").length'),15);
  assert.equal(await evaluate('[...document.querySelectorAll("a[href^=\\"#\\"]")].every(a=>document.querySelector(a.getAttribute("href")))'),true);
  assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true,'Guide overflow');
  await screenshot('guide-mobile');
  // Documentation and the landing pitch remain readable without script execution.
  await call('Emulation.setScriptExecutionDisabled',{value:true});
  await call('Page.navigate',{url:base+'/'});
  await new Promise(r=>setTimeout(r,400));
  const doc=await call('DOM.getDocument');
  const landing=await call('DOM.querySelector',{nodeId:doc.root.nodeId,selector:'#sign-in'});
  const attrs=await call('DOM.getAttributes',{nodeId:landing.nodeId});assert.ok(!attrs.attributes.includes('hidden'));
  await call('Emulation.setScriptExecutionDisabled',{value:false});
  await call('Page.removeScriptToEvaluateOnNewDocument',{identifier:fixture.identifier});
  // Real demo API and transfer engine: no mocked job, connection or worker responses.
  await call('Page.navigate',{url:base+'/'});await until('document.querySelector("#new-job") && !document.querySelector("#new-job").disabled');
  assert.equal(await evaluate('document.querySelector("#sign-in").hidden'),true);
  assert.equal(await evaluate('document.querySelector("#workspace").hidden'),false);
  await evaluate('document.querySelector("#new-job").click()');
  assert.equal(await evaluate('document.querySelector("#execution-worker").selectedOptions[0].textContent'),'This server');
  assert.equal(await evaluate('document.querySelector("[name=source]").value'),'demo-source');
  assert.equal(await evaluate('document.querySelector("[name=sink]").value'),'demo-sink');
  assert.equal(await evaluate('document.querySelector("[name=dimension]").value'),'4');
  await evaluate('document.querySelector("#job-form").requestSubmit()');
  await until('document.querySelector("#progress-text").textContent.includes("Transfer complete.")');
  const jobs=await (await fetch(base+'/v1/jobs')).json();
  const selected=await evaluate('selected');
  const latest=jobs.jobs.find(j=>j.id===selected);
  assert.equal(latest.state,'succeeded');assert.equal(latest.records,1000);
  assert.equal(exceptions.length,0);
  console.log(JSON.stringify({publicLanding:true,providers:15,publicDocs:true,platformCommands:3,clipboardFallback:true,noJavaScriptPitch:true,responsiveWidths:[320,390,768,1440],realDemoRecords:1000,jsExceptions:0}));
} finally { await call('Browser.close'); ws.close(); }
