const setup=id=>document.getElementById(id);
let setupCSRF='',setupReady=false,enrollment=null,generatedScript='',setupBusy=false,checkingWorker=false;
let trackedWorker=new URLSearchParams(location.search).get('worker')||'';
if(!/^[a-f0-9]{32}$/.test(trackedWorker))trackedWorker='';
function setupStep(step){for(const name of ['configure','connect','transfer']){const el=setup('step-'+name);if(name===step)el.setAttribute('aria-current','step');else el.removeAttribute('aria-current');}}
function trackWorker(id){trackedWorker=id;history.replaceState(null,'',id?'/connections/new?worker='+encodeURIComponent(id):'/connections/new');}

function setupError(message){setup('setup-error').textContent=message;setup('setup-error').hidden=!message;}
function clearGenerated(){generatedScript='';setup('setup-result').hidden=true;setup('setup-script-preview').textContent='';setup('copy-script').textContent='Copy script';}
function renderSetupConnection(role){
  const section=setup('setup-'+role);section.replaceChildren();
  const provider=addSelect(section,role+'_kind','Database',Object.entries(providerNames));provider.value=role==='source'?'pinecone':'polign';
  const name=addInput(section,role+'_name','Connection name',{value:role==='source'?'production-source':'polign-destination'});name.pattern='[A-Za-z0-9_.\\-]+';name.maxLength=128;
  const fields=node('div');section.append(fields);
  provider.onchange=()=>{
    fields.replaceChildren();clearGenerated();const prefix=role+'_',kind=provider.value;
    if(kind==='s3vectors'){
      addInput(fields,prefix+'region','AWS region',{placeholder:'us-east-1'});addInput(fields,prefix+'bucket','Vector bucket');addInput(fields,prefix+'index','Vector index');
      fields.append(node('p','Uses the worker’s AWS workload identity or locally configured AWS credentials.','hint'));return;
    }
    const scheme=({pgvector:'postgresql://host:5432',redis:'rediss://host:6379',mongodb:'mongodb+srv://cluster-host'})[kind];
    addInput(fields,prefix+'endpoint','Database endpoint (no credentials)',{placeholder:scheme||'https://your-database-host'});
    if(providerFields[kind]){
      for(const [key,title,value='',required=true]of providerFields[kind])addInput(fields,prefix+key,title,{value,required});
      if(kind==='mongodb')addSelect(fields,prefix+'id_type','Record ID type',[['objectid','ObjectID'],['string','String'],['int64','64-bit integer']]);
      fields.append(node('p',kind==='opensearch'?'Uses basic authentication. Credentials are prompted on the worker.':providerHints[kind],'hint'));
    }else if(kind==='polign')addInput(fields,prefix+'collection','Collection');
    else{
      addInput(fields,prefix+'namespace',kind==='pinecone'?'Namespace (optional)':'Namespace',{required:kind!=='pinecone'});
      if(kind==='turbopuffer'){
        addSelect(fields,prefix+'distance_metric','Distance metric',[['cosine_distance','Cosine'],['euclidean_squared','Squared Euclidean']]);
        addSelect(fields,prefix+'id_type','Record ID type',[['string','String'],['uint','Unsigned integer'],['uuid','UUID']]);
      }
    }
    fields.append(node('p',credentialKeys(kind).includes('password')?'Username and password are prompted on the worker machine.':'The API key is prompted on the worker machine.','hint'));
  };provider.onchange();
}
function collectLocalConnection(role){
  const form=setup('connection-form'),get=key=>form.elements[role+'_'+key]?.value.trim()||'';
  const kind=get('kind'),config={kind};
  const keys=providerFields[kind]?['endpoint',...providerFields[kind].map(f=>f[0]),...(kind==='mongodb'?['id_type']:[])]:kind==='s3vectors'?['region','bucket','index']:kind==='polign'?['endpoint','collection']:kind==='pinecone'?['endpoint','namespace']:['endpoint','namespace','distance_metric','id_type'];
  for(const key of keys)config[key]=get(key);
  if(config.endpoint){
    let u;try{u=new URL(config.endpoint);}catch{throw new Error('Enter a valid '+role+' endpoint.');}
    const allowed=({pgvector:['postgresql:'],redis:['rediss:'],mongodb:['mongodb:','mongodb+srv:']})[kind]||['https:'];
    if(!allowed.includes(u.protocol)||u.username||u.password||u.search||u.hash||(u.pathname&&u.pathname!=='/'))throw new Error('Use a TLS '+role+' endpoint without credentials, path, or query.');
  }
  const prefix=role==='source'?'SOURCE':'DESTINATION',label=role==='source'?'Source':'Destination',prompts=[];
  if(kind!=='s3vectors'){
    if(credentialKeys(kind).includes('password')){
      config.username_env=prefix+'_USERNAME';config.password_env=prefix+'_PASSWORD';
      prompts.push({name:config.username_env,label:label+' username',required:kind!=='redis'},{name:config.password_env,label:label+' password',required:true});
    }else{config.api_key_env=prefix+'_API_KEY';prompts.push({name:config.api_key_env,label:label+' API key',required:true});}
  }
  config[role==='source'?'read_only':'write_only']=true;
  const name=get('name');if(!/^[A-Za-z0-9_.-]{1,128}$/.test(name))throw new Error('Use letters, digits, dots, underscores or hyphens in connection names.');
  return{name,config,prompts};
}
async function setupRequest(path,options={}){
  const headers={'Content-Type':'application/json',...options.headers};if(setupCSRF)headers['X-CSRF-Token']=setupCSRF;
  const r=await fetch(path,{...options,headers}),body=await r.json();
  if(!r.ok){if(r.status===401){setupReady=false;enrollment=null;clearGenerated();setup('connection-form').hidden=true;setup('worker-status').hidden=true;setup('setup-login').hidden=false;}throw new Error(body.error||'Request failed.');}return body;
}
setup('connection-form').oninput=clearGenerated;
setup('connection-form').onsubmit=async event=>{
  event.preventDefault();if(!setupReady||setupBusy)return;setupBusy=true;setupError('');clearGenerated();
  try{
    const form=event.target,source=collectLocalConnection('source'),sink=collectLocalConnection('sink');
    if(source.name===sink.name)throw new Error('Source and destination need different connection names.');
    const sourceResource={...source.config},sinkResource={...sink.config};
    for(const c of[sourceResource,sinkResource])for(const k of['read_only','write_only','api_key_env','username_env','password_env'])delete c[k];
    if(JSON.stringify(sourceResource)===JSON.stringify(sinkResource))throw new Error('Source and destination must be different database resources.');
    const workerName=form.elements.worker_name.value.trim();if(!/^[A-Za-z0-9_.-]{1,128}$/.test(workerName))throw new Error('Enter a valid worker name.');
    const dimension=Number(form.elements.dimension.value),platform=form.elements.platform.value;
    if(!Number.isInteger(dimension)||dimension<1||dimension>65536)throw new Error('Dimension must be between 1 and 65536.');
    setup('setup-fields').disabled=true;
    const response=await fetch('/downloads/SHA256SUMS');if(!response.ok)throw new Error('Worker downloads are unavailable. Try again shortly.');
    const checksums={};for(const line of(await response.text()).trim().split('\n')){const m=line.match(/^([a-f0-9]{64})\s+(vtransfer-(?:linux-amd64|linux-arm64|darwin-arm64))$/);if(m)checksums[m[2]]=m[1];}
    for(const name of['vtransfer-linux-amd64','vtransfer-linux-arm64','vtransfer-darwin-arm64'])if(!checksums[name])throw new Error('Worker download checksums are incomplete.');
    // Reuse only an enrollment that has not been consumed by an installed worker.
    if(enrollment){const current=(await setupRequest('/v1/workers')).workers.find(w=>w.id===enrollment.id);if(!current||current.status!=='awaiting enrollment')enrollment=null;}
    // Only the worker name is submitted.
    if(!enrollment||enrollment.name!==workerName||Date.now()>=enrollment.expiresAt){
      const result=await setupRequest('/v1/workers',{method:'POST',body:JSON.stringify({name:workerName})});
      enrollment={name:workerName,id:result.worker.id,token:result.enrollment_token,expiresAt:Date.now()+result.expires_in*1000};
    }
    if(!setupReady)return;
    generatedScript=generateWorkerScript({origin:location.origin,workerID:enrollment.id,enrollmentToken:enrollment.token,platform,checksums,connections:{[source.name]:source.config,[sink.name]:sink.config},pair:{source:source.name,sink:sink.name,dimension},credentials:[...source.prompts,...sink.prompts]});
    setup('setup-script-preview').textContent=generatedScript;setup('setup-result').hidden=false;
    setup('setup-worker-location').textContent='Installs in ~/.polign-transfer/'+enrollment.id+'. Restart later with: bash ~/.polign-transfer/'+enrollment.id+'/run-worker.sh';
    setup('setup-expiry').textContent='Enrollment expires at '+new Date(enrollment.expiresAt).toLocaleTimeString()+'. If it expires or a download checksum changes, generate a new script.';
    trackWorker(enrollment.id);setup('connection-form').hidden=true;setup('edit-setup').hidden=false;setupStep('connect');
    setup('setup-result').scrollIntoView({behavior:'smooth',block:'start'});
  }catch(err){setupError(err.message);}finally{setupBusy=false;setup('setup-fields').disabled=false;if(trackedWorker)checkWorker();}
};
setup('download-script').onclick=()=>{
  if(!generatedScript)return;const a=node('a');a.href=URL.createObjectURL(new Blob([generatedScript],{type:'text/x-shellscript'}));a.download='setup-worker.sh';a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000);
};
setup('copy-script').onclick=async()=>{try{await navigator.clipboard.writeText(generatedScript);setup('copy-script').textContent='Copied';}catch{setupError('Clipboard unavailable. Download the script instead.');}};
window.addEventListener('pagehide',()=>{setupReady=false;enrollment=null;clearGenerated();setup('connection-form').reset();});
setup('setup-sign-in').onclick=()=>{try{sessionStorage.setItem('transfer-setup-return',trackedWorker||'new');}catch{}};
setup('setup-retry').onclick=initializeSetup;
setup('edit-setup').onclick=()=>{trackWorker('');clearGenerated();setup('worker-status').hidden=true;setup('connection-form').hidden=false;setupStep('configure');setup('connection-form').scrollIntoView({block:'start'});};
setup('restart-setup').onclick=()=>{enrollment=null;setupError('');setup('edit-setup').onclick();};
setup('check-worker').onclick=checkWorker;
async function checkWorker(){
  if(!setupReady||!trackedWorker||checkingWorker||setupBusy)return;
  const id=trackedWorker;checkingWorker=true;setup('check-worker').disabled=true;
  try{
    const result=await setupRequest('/v1/workers');if(!setupReady||id!==trackedWorker)return;
    const worker=result.workers.find(w=>w.id===id);setupError('');setup('worker-status').hidden=false;
    setup('continue-transfer').hidden=true;setup('worker-restart').hidden=true;setup('restart-setup').hidden=true;setup('worker-pair-summary').textContent='';
    const status=worker?.status,ready=status==='online'&&worker.manifest.pairs?.length;
    setupStep(ready?'transfer':'connect');
    if(ready){
      setup('worker-status-title').textContent=worker.name+' is connected';
      setup('worker-status-message').textContent='Your worker is ready to accept a transfer. Continue with its connections and dimensions already filled in.';
      setup('worker-pair-summary').textContent=worker.manifest.pairs.map(p=>p.source+' → '+p.sink+' · '+p.dimension+' dimensions').join('; ');
      setup('continue-transfer').href='/?new=1&worker='+encodeURIComponent(id);setup('continue-transfer').hidden=false;
      clearGenerated();enrollment=null;
    }else if(status==='awaiting enrollment'){
      setup('worker-status-title').textContent='Waiting for '+worker.name;
      setup('worker-status-message').textContent=generatedScript?'Run the downloaded script and finish the credential prompts. This page checks automatically and will show when the worker connects.':'Run the script you downloaded earlier. If you no longer have it, generate a new setup script; enrollment tokens are not stored in the browser.';
      setup('restart-setup').hidden=!!generatedScript;
    }else if(status==='offline'||status==='online'){
      clearGenerated();enrollment=null;
      setup('worker-status-title').textContent=status==='offline'?worker.name+' is offline':'Waiting for database configuration';
      setup('worker-status-message').textContent='Start the worker and finish any credential prompts. If it exits, check the terminal error and correct the local connection settings. This page will update when it reconnects.';
      setup('worker-restart').hidden=false;setup('worker-restart-command').textContent='bash ~/.polign-transfer/'+id+'/run-worker.sh';
    }else{
      clearGenerated();enrollment=null;
      setup('worker-status-title').textContent=status==='enrollment expired'?'Setup script expired':status==='revoked'?'Worker access was revoked':'Worker not found';
      setup('worker-status-message').textContent=status==='enrollment expired'?'Generate a new script and run it within 10 minutes. Your database fields are still available in this tab.':'Create a new worker setup, or return to the dashboard to choose another worker.';
      setup('restart-setup').hidden=false;
    }
  }catch(err){if(setupReady&&id===trackedWorker){setup('continue-transfer').hidden=true;setupError('Could not check the worker. '+err.message+' Retrying automatically; you can also choose Check again.');}}
  finally{checkingWorker=false;setup('check-worker').disabled=false;}
}

async function initializeSetup(){
  if(!setup('setup-source').children.length){renderSetupConnection('source');renderSetupConnection('sink');}
  setupError('');setup('setup-retry').hidden=true;setup('setup-loading').hidden=false;
  try{
    const response=await fetch('/auth/config');if(!response.ok)throw new Error('Could not load sign-in settings.');const config=await response.json();
    if(config.mode==='polign'){
      const response=await fetch('/auth/session');if(response.status===401){setup('setup-login').hidden=false;return;}if(!response.ok)throw new Error('Sign-in is unavailable. Refresh to retry.');
      const session=await response.json();setupCSRF=session.csrf_token;setup('setup-identity').textContent=session.email||(session.subject?`Account · ${session.subject.slice(0,8)}`:'Signed in');setup('setup-identity').title=session.email||session.subject||'Signed in';
    }
    const workers=await setupRequest('/v1/workers');if(!workers.enabled)throw new Error('Customer worker setup is disabled on this server.');
    setupReady=true;setup('setup-login').hidden=true;setup('connection-form').hidden=!!trackedWorker;if(trackedWorker)await checkWorker();
  }catch(err){setupError(err.message);setup('setup-retry').hidden=false;}finally{setup('setup-loading').hidden=true;}
}
initializeSetup();setInterval(checkWorker,2000);
