const $ = (id) => document.getElementById(id);
let accountMode = false, signedIn = false, csrf = '', personalConnectionsEnabled = false;
let credentialConnection = null;
let workers = [], workersEnabled = false, hostedEnabled = true, submitting = false, manifestSignature = '';
let pendingTransfer = new URLSearchParams(location.search).get('new') === '1';
const connectionRequestKeys = {};
let token = '', connections = [], selected = null, audit = [], after = 0, refreshing = false, action = '', submissionKey = '';
const number = (v) => new Intl.NumberFormat().format(v);
const date = (v) => new Date(v).toLocaleString();

function notice(message) { $('notice').textContent = message; $('notice').hidden = !message; }
async function api(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...options.headers };
  if (accountMode && csrf) headers['X-CSRF-Token'] = csrf;
  if (token) headers.Authorization = `Bearer ${token}`;
  const response = await fetch(`/v1${path}`, { ...options, headers });
  const result = await response.json();
  if (!response.ok) { if (response.status === 401) { if (accountMode) showSignIn(); else if (!$('token-dialog').open) $('token-dialog').showModal(); } throw new Error(result.error || `Request failed (${response.status})`); }
  return result;
}
function openForm(preferredWorker) {
  submissionKey = crypto.randomUUID();
  $('form-error').hidden = true;
  const form = $('job-form');
  fillWorkerChoices(typeof preferredWorker === 'string' ? preferredWorker : undefined);
  for (const role of ['source', 'sink']) fillConnectionChoices(role);
  suggestWorkerDimension();
  updateExecutionHint();suggestJobName();manifestSignature = '';
  $('job-dialog').showModal();
}
async function refresh() {
  if (refreshing || !signedIn) return;
  refreshing = true;
  try {
    const [cs, result, ws] = await Promise.all([api('/connections'), api('/jobs?limit=1000'), api('/workers')]); if (!signedIn) return; connections = cs.connections; personalConnectionsEnabled = accountMode && cs.personal_connections_enabled === true;
    hostedEnabled = cs.hosted_execution_enabled !== false; workers = ws.workers || []; workersEnabled = ws.enabled === true; renderWorkers();syncOpenForm();
    const canSubmit = workersEnabled || personalConnectionsEnabled || (connections.some(c => c.can_read) && connections.some(c => c.can_write));
    $('new-job').disabled = $('empty-new').disabled = !canSubmit;
    const jobs = result.jobs;
    $('stat-total').textContent = number(result.total);
    $('stat-active').textContent = number(jobs.filter(j => ['queued', 'running', 'retry_wait', 'cancel_requested'].includes(j.state)).length);
    $('stat-done').textContent = number(jobs.filter(j => j.state === 'succeeded').length);
    $('stat-records').textContent = number(jobs.reduce((sum, j) => sum + j.records, 0));
    const visible = jobs.filter(j => !$('filter').value || j.state === $('filter').value);
    $('jobs').replaceChildren();
    visible.forEach(j => {
      const tr = node('tr'), name = node('td'), button = node('button', j.spec.name, 'job-link'); button.onclick = () => selectJob(j.id);
      name.append(button, node('span', j.id.slice(0, 12), 'job-id'));
      const state = node('td'); state.append(node('span', j.state.replaceAll('_', ' '), `badge ${j.state}`)); if (j.state === 'running' && j.phase) state.append(node('small', phaseLabel(j.phase), 'job-id')); if (j.spec.worker_id && ['queued', 'running', 'retry_wait', 'cancel_requested'].includes(j.state) && workerOffline(j.spec.worker_id)) state.append(node('small', 'Worker offline · awaiting reconnection', 'job-id')); if (j.next_run_at) state.append(node('small', `Retry ${date(j.next_run_at)}`, 'job-id'));
      tr.append(name, node('td', `${connectionLabel(j.spec.source)} → ${connectionLabel(j.spec.sink)}`), state, node('td', number(j.records)), node('td', date(j.created_at)));
      $('jobs').append(tr);
    });
    $('empty').hidden = jobs.length !== 0;
    $('list-note').textContent = result.total > jobs.length ? `Showing the most recent ${jobs.length} of ${result.total} jobs; summary counts reflect loaded jobs. Use the API to page through all jobs.` : visible.length === 0 && jobs.length > 0 ? 'No jobs match this status.' : '';
    if (selected) await loadDetail();
    if(pendingTransfer){pendingTransfer=false;openForm(new URLSearchParams(location.search).get('worker') || undefined);history.replaceState(null,'','/');}
    notice(canSubmit ? '' : 'Connection setup is unavailable on this server. Refresh after the service update completes.');
  } catch (err) { notice(err.message); } finally { refreshing = false; }
}
async function selectJob(id) { selected = id; audit = []; after = 0; $('detail').hidden = false; try { await loadDetail(); $('detail').scrollIntoView({ behavior: 'smooth', block: 'start' }); } catch (err) { notice(err.message); } }
async function loadDetail() {
  const id = selected; if (!id) return;
  const [j, result] = await Promise.all([api(`/jobs/${id}`), api(`/jobs/${id}/events?after=${after}&limit=100`)]);
  if (selected !== id) return;
  $('detail-name').textContent = j.spec.name;
  const info = $('detail-info'); info.replaceChildren();
  for (const [label, value] of [['Status', j.state.replaceAll('_', ' ')], ['Records / batches', `${number(j.records)} / ${number(j.batches)}`], ['Retries / runs', `${j.retries} / ${j.runs}`], ['Submitted by', j.created_by], ['Source', connectionLabel(j.spec.source)], ['Destination', connectionLabel(j.spec.sink)], ['Dimensions', j.spec.dimension], ['Last activity', date(j.updated_at)], ['Last checkpoint', j.last_checkpoint_at ? date(j.last_checkpoint_at) : 'No batch acknowledged yet'], ['Automatic restarts', `${j.restarts || 0} / ${Math.max(0, j.spec.max_restarts || 0)}`]]) { const d = node('div'); d.append(node('span', label), node('strong', String(value))); info.append(d); }
  if (j.spec.worker_id) { const worker = workers.find(w => w.id === j.spec.worker_id); const d = node('div'); d.append(node('span', 'Customer worker'), node('strong', `${worker?.name || j.spec.worker_id} · ${worker?.status || 'unavailable'}`)); info.append(d); }
  const help=$('job-worker-help');help.replaceChildren();help.hidden=!j.spec.worker_id||!workerOffline(j.spec.worker_id)||['succeeded','canceled'].includes(j.state);
  if(!help.hidden){help.append(node('span','The worker is offline. Reconnect it to continue from the saved checkpoint. '));const link=node('a','Reconnect worker →');link.href='/connections/new?worker='+encodeURIComponent(j.spec.worker_id);help.append(link);}
  if (j.error) info.append(node('div', j.error, 'detail-error'));
  action = ['failed', 'canceled'].includes(j.state) ? 'resume' : ['queued', 'running', 'retry_wait'].includes(j.state) ? 'cancel' : '';
  $('job-action').hidden = !action; $('job-action').textContent = action === 'resume' ? 'Resume transfer' : 'Cancel transfer';
  for (const role of ['source', 'sink']) { const b = connections.find(c => c.name === j.spec[role]); $(role + '-credentials').hidden = !!j.spec.worker_id || !b?.personal; $(role + '-credentials').onclick = () => editCredentials(j.spec[role]); }
  $('retry-now').hidden = j.state !== 'retry_wait';
  const progress = $('progress'); progress.hidden = !['running', 'retry_wait', 'queued', 'cancel_requested', 'succeeded'].includes(j.state);
  if (j.state === 'succeeded') { progress.max = 1; progress.value = 1; } else { progress.removeAttribute('value'); }
  $('progress-text').textContent = `${number(j.records)} records saved in ${number(j.batches)} batches. ` + (j.state === 'succeeded' ? 'Transfer complete.' : j.next_run_at ? `Automatic retry ${date(j.next_run_at)} from the last checkpoint.` : j.state === 'running' ? `${phaseLabel(j.phase)}. Total source size is unknown.` : ['failed', 'canceled'].includes(j.state) ? 'Resume continues from the last saved checkpoint.' : 'Waiting for execution.');
  const known = new Set(audit.map(e => e.sequence)); audit.push(...result.events.filter(e => !known.has(e.sequence))); after = Math.max(after, result.next_after);
  $('events').replaceChildren();
  audit.forEach(e => { const li = node('li'), content = node('div', e.message); content.append(node('small', `actor: ${e.actor} · records: ${number(e.records)} · event #${e.sequence}`)); if (e.batch_sha256) content.append(node('small', `batch SHA-256: ${e.batch_sha256}`)); li.append(node('time', date(e.at)), node('strong', e.kind), content); $('events').append(li); });
  $('more-events').hidden = result.events.length < 100;
}
$('new-job').onclick = $('empty-new').onclick = () => openForm();
$('close-form').onclick = () => $('job-dialog').close();
$('job-dialog').addEventListener('close', () => { if ($('job-dialog').open) return; $('job-form').reset(); for (const role of ['source', 'sink']) $(role + '-setup').replaceChildren(); });
$('close-detail').onclick = () => { selected = null; $('detail').hidden = true; };
$('refresh').onclick = $('filter').onchange = refresh;
$('access').onclick = async () => {
  if (!accountMode) { $('token-dialog').showModal(); return; }
  if (!signedIn) { location.assign('/auth/login'); return; }
  try {
    const response = await fetch('/auth/logout', { method: 'POST', headers: { 'X-CSRF-Token': csrf } });
    const result = await response.json(); if (!response.ok) throw new Error(result.error || 'Sign out failed');
    showSignIn(); location.assign(result.logout_url);
  } catch (err) { notice(err.message); }
};
$('close-token').onclick = () => $('token-dialog').close();
$('token-form').onsubmit = (event) => { event.preventDefault(); token = event.target.elements.token.value; $('token-dialog').close(); refresh(); };
$('job-form').onsubmit = async (event) => {
  event.preventDefault(); if(submitting)return;const form = event.target, button = $('submit-job');
  if(!executionReady()){updateExecutionHint();return;}
  submitting=true;button.disabled = true;button.textContent='Starting transfer…';$('form-error').hidden=true;
  $('source-fields').disabled = $('sink-fields').disabled = $('execution-worker').disabled = true;
  try {
    if ($('execution-worker').value === '__choose__') throw new Error('Select a configured worker or Polign hosted execution.');
    const source = await resolveConnection('source');
    const sink = await resolveConnection('sink');
    const data = { name: form.elements.name.value, source, sink, dimension: Number(form.elements.dimension.value), batch_size: Number(form.elements.batch_size.value) };
    if ($('execution-worker').value) data.worker_id = $('execution-worker').value;
    const job = await api('/jobs', { method: 'POST', headers: { 'Idempotency-Key': submissionKey }, body: JSON.stringify(data) });
    if (!signedIn) return;
    $('job-dialog').close(); form.reset(); await selectJob(job.id); await refresh();
  } catch (err) { $('form-error').textContent = err.message; $('form-error').hidden = false; }
  finally { submitting=false;button.textContent='Start transfer';$('source-fields').disabled = $('sink-fields').disabled = $('execution-worker').disabled = false;updateExecutionHint(); }
};
$('job-action').onclick = async () => { const id = selected, requested = action; if (!id || !requested) return; $('job-action').disabled = true; try { await api(`/jobs/${id}/${requested}`, { method: 'POST', body: '{}' }); await refresh(); } catch (err) { notice(err.message); } finally { $('job-action').disabled = false; } };
$('more-events').onclick = () => loadDetail().catch(err => notice(err.message));
$('export-events').onclick = () => { const blob = new Blob([JSON.stringify(audit, null, 2)], { type: 'application/json' }); const a = node('a'); a.href = URL.createObjectURL(blob); a.download = `${selected}-events.json`; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1000); };
function phaseLabel(phase) { return ({ read: 'Reading source', write: 'Writing destination', backoff: 'Waiting before retry', checkpointed: 'Batch saved', starting: 'Starting' })[phase] || 'Working'; }
function showSignIn() {
  if(pendingTransfer||$('job-dialog').open){const id=$('job-dialog').open?$('execution-worker').value:new URLSearchParams(location.search).get('worker');try{sessionStorage.setItem('transfer-job-return',/^[a-f0-9]{32}$/.test(id||'')?id:'new');}catch{}}
  signedIn = false; csrf = ''; selected = null; connections = []; audit = []; after = 0;
  $('workspace').hidden = true; $('sign-in').hidden = false; $('access').textContent = 'Sign in'; $('identity').textContent = 'Account settings'; $('account-link').removeAttribute('title'); $('account-caption').hidden = true;
  $('jobs').replaceChildren(); $('events').replaceChildren(); $('detail-info').replaceChildren(); $('detail').hidden = true;
  $('job-dialog').close(); $('credential-dialog').close(); workers = [];
}
$('retry-now').onclick = async () => { if (!selected) return; $('retry-now').disabled = true; try { await api(`/jobs/${selected}/resume`, { method: 'POST', body: '{}' }); await refresh(); } catch (err) { notice(err.message); } finally { $('retry-now').disabled = false; } };
async function initialize() {
  $('new-job').disabled = true;
  try {
    const response = await fetch('/auth/config'); if (!response.ok) throw new Error('Could not load sign-in configuration');
    const config = await response.json(); accountMode = config.mode === 'polign';
    if (accountMode) {
      $('access').textContent = 'Sign in';
      $('account-link').href = config.account_url; $('account-link').hidden = false;
      const session = await fetch('/auth/session');
      if (session.status === 401) { showSignIn(); return; }
      if (!session.ok) throw new Error('Polign sign-in is temporarily unavailable. Refresh to try again.');
      const identity = await session.json(); csrf = identity.csrf_token;
      const subject = identity.subject || '';
      const accountLabel = identity.email || (subject ? `Account · ${subject.slice(0, 8)}` : 'Signed in');
      $('identity').textContent = accountLabel;
      $('account-link').title = `${identity.email || subject || 'Signed in'} — Account settings`;
      $('account-caption').hidden = false; $('access').textContent = 'Sign out';
    }
    signedIn = true; $('workspace').hidden = false; $('sign-in').hidden = true;
    try{const next=sessionStorage.getItem('transfer-setup-return');sessionStorage.removeItem('transfer-setup-return');if(next==='new'||/^[a-f0-9]{32}$/.test(next||'')){location.replace('/connections/new'+(next==='new'?'':'?worker='+next));return;}}catch{}
    try{const next=sessionStorage.getItem('transfer-job-return');sessionStorage.removeItem('transfer-job-return');if(next==='new'||/^[a-f0-9]{32}$/.test(next||'')){pendingTransfer=true;history.replaceState(null,'','/?new=1'+(next==='new'?'':'&worker='+next));}}catch{}
    await refresh();
  } catch (err) { notice(err.message); } finally { if (!signedIn) $('new-job').disabled = $('empty-new').disabled = true; }
}


function connectionLabel(id) { const c = connections.find(c => c.name === id); return c?.label || c?.name || id; }
function collectCredentials(form, prefix, kind) {
  const keys = credentialKeys(kind);
  return Object.fromEntries(keys.map(key => [key, form.elements[prefix + key].value]));
}
function fillConnectionChoices(role, chosen) {
  const remote = $('execution-worker').value !== '';
  const available = executionConnections();
  const select = $('job-form').elements[role]; select.replaceChildren();
  const first = node('option', !remote && personalConnectionsEnabled ? 'Set up a new connection' : `Choose ${role === 'source' ? 'source' : 'destination'}`); first.value = !remote && personalConnectionsEnabled ? '__new__' : ''; select.append(first);
  for (const c of available.filter(c => role === 'source' ? c.can_read : c.can_write)) { const o = node('option', `${c.label || c.name} · ${providerNames[c.kind] || c.kind}`); o.value = c.name; select.append(o); }
  if (chosen && [...select.options].some(o => o.value === chosen)) select.value = chosen; else if (select.options.length === 2) select.selectedIndex = 1;
  renderConnectionSetup(role);
}
function renderConnectionSetup(role) {
  const choice = $('job-form').elements[role].value, setup = $(role + '-setup'); setup.replaceChildren(); setup.hidden = choice !== '__new__';
  const existing = connections.find(c => c.name === choice); $(role + '-edit').hidden = !!$('execution-worker').value || !existing?.personal;
  if (choice !== '__new__') return;
  connectionRequestKeys[role] = crypto.randomUUID(); setup.oninput = () => { connectionRequestKeys[role] = crypto.randomUUID(); };
  const provider = addSelect(setup, role + '_kind', 'Database', Object.entries(providerNames));
  addInput(setup, role + '_label', 'Connection name (optional)', {required: false, placeholder: role === 'source' ? 'Production source' : 'Migration destination'}).maxLength = 128;
  const fields = node('div'); setup.append(fields);
  const renderFields = () => {
    fields.replaceChildren(); const prefix = role + '_', kind = provider.value;
    if (kind === 's3vectors') {
      addInput(fields, prefix + 'region', 'AWS region', {placeholder: 'us-east-1'});
      addInput(fields, prefix + 'bucket', 'Vector bucket', {placeholder: 'my-vector-bucket'});
      addInput(fields, prefix + 'index', 'Vector index', {placeholder: 'documents'});
    } else if (providerFields[kind]) {
      const scheme = ({pgvector: 'postgresql://host:5432', redis: 'rediss://host:6379', mongodb: 'mongodb+srv://cluster-host'})[kind];
      addInput(fields, prefix + 'endpoint', scheme ? 'Database endpoint (without credentials)' : 'Public HTTPS endpoint', {type: scheme ? 'text' : 'url', placeholder: scheme || 'https://your-database-host'});
      for (const [key, title, value = '', required = true] of providerFields[kind]) addInput(fields, prefix + key, title, {value, required});
      if (kind === 'mongodb') addSelect(fields, prefix + 'id_type', 'Record ID type', [['objectid', 'ObjectID'], ['string', 'String'], ['int64', '64-bit integer']]);
      fields.append(node('p', providerHints[kind], 'hint'));
    } else {
      addInput(fields, prefix + 'endpoint', 'Public HTTPS endpoint', {type: 'url', placeholder: 'https://your-database-host'});
      if (kind === 'polign') addInput(fields, prefix + 'collection', 'Collection', {placeholder: 'documents'});
      else addInput(fields, prefix + 'namespace', kind === 'pinecone' ? 'Namespace (optional)' : 'Namespace', {required: kind !== 'pinecone', placeholder: 'documents'});
      if (kind === 'turbopuffer') {
        addSelect(fields, prefix + 'distance_metric', 'Distance metric', [['cosine_distance', 'Cosine'], ['euclidean_squared', 'Squared Euclidean']]);
        addSelect(fields, prefix + 'id_type', 'Record ID type', [['string', 'String'], ['uint', 'Unsigned integer'], ['uuid', 'UUID']]);
      }
    }
    credentialFields(fields, prefix, kind);
    fields.append(node('p', 'Polign stores these credentials encrypted, outside job history.', 'hint'));
  };
  provider.onchange = renderFields; renderFields();
}
async function resolveConnection(role) {
  const form = $('job-form'), selected = form.elements[role].value;
  if (selected !== '__new__') return selected;
  const kind = form.elements[role + '_kind'].value, config = { kind };
  const keys = providerFields[kind] ? ['endpoint', ...providerFields[kind].map(f => f[0]), ...(kind === 'mongodb' ? ['id_type'] : [])] : kind === 's3vectors' ? ['region', 'bucket', 'index'] : kind === 'polign' ? ['endpoint', 'collection'] : kind === 'pinecone' ? ['endpoint', 'namespace'] : ['endpoint', 'namespace', 'distance_metric', 'id_type'];
  for (const key of keys) config[key] = form.elements[role + '_' + key].value.trim();
  const label = form.elements[role + '_label'].value.trim() || `${providerNames[kind]} · ${config.collection || config.namespace || config.index || (role === 'source' ? 'source' : 'destination')}`.slice(0, 128);
  const definition = { label, direction: role, config, credentials: collectCredentials(form, role + '_', kind) };
  const connection = await api('/connections', {method: 'POST', headers: {'Idempotency-Key': connectionRequestKeys[role]}, body: JSON.stringify(definition)});
  if (!signedIn) throw new Error('Sign in again to continue.');
  connections = connections.filter(c => c.name !== connection.name); connections.push(connection);
  fillConnectionChoices(role, connection.name); // Removes credential fields as soon as saving succeeds.
  return connection.name;
}
for (const role of ['source', 'sink']) {
  $('job-form').elements[role].onchange = () => { renderConnectionSetup(role); if ($('execution-worker').value && role === 'source') fillConnectionChoices('sink'); suggestWorkerDimension();suggestJobName(); };
  $(role + '-edit').onclick = () => editCredentials($('job-form').elements[role].value);
}
function editCredentials(id) {
  const connection = connections.find(c => c.name === id && c.personal); if (!connection) return;
  credentialConnection = connection; $('credential-title').textContent = `Credentials: ${connection.label || connection.name}`;
  $('credential-error').hidden = true; $('credential-fields').replaceChildren(); credentialFields($('credential-fields'), 'credential_', connection.kind); $('credential-dialog').showModal();
}
$('close-credentials').onclick = () => $('credential-dialog').close();
$('credential-dialog').addEventListener('close', () => { $('credential-form').reset(); $('credential-fields').replaceChildren(); credentialConnection = null; });
$('credential-form').onsubmit = async event => {
  event.preventDefault(); const connection = credentialConnection; if (!connection) return; $('save-credentials').disabled = true;
  try {
    await api(`/connections/${connection.name}/credentials`, {method: 'PUT', body: JSON.stringify(collectCredentials(event.target, 'credential_', connection.kind))});
    $('credential-dialog').close(); notice('Credentials updated. You can resume the transfer from its saved checkpoint.');
  } catch (err) { $('credential-error').textContent = err.message; $('credential-error').hidden = false; }
  finally { $('save-credentials').disabled = false; }
};
function workerOffline(id) { return workers.find(w => w.id === id)?.status !== 'online'; }
function executionConnections() {
  const id = $('execution-worker').value;
  if (!id) return connections;
  const worker = workers.find(w => w.id === id); if (!worker) return [];
  const source = $('job-form').elements.source.value;
  const pairs = worker.manifest?.pairs || [];
  return (worker.manifest?.connections || []).map(c => ({...c,
    can_read: c.can_read && pairs.some(p => p.source === c.name),
    can_write: c.can_write && pairs.some(p => p.sink === c.name && (!source || p.source === source))
  }));
}
function fillWorkerChoices(preferred) {
  const select = $('execution-worker'); select.replaceChildren();
  if (workersEnabled) { const o = node('option', 'Your infrastructure · set up a worker'); o.value = '__choose__'; select.append(o); }
  const available = workers.filter(w => w.status !== 'revoked');
  for (const w of available) { const o = node('option', `${w.name} · ${w.status}`); o.value = w.id; select.append(o); }
  if (hostedEnabled) { const o = node('option', accountMode ? 'Polign hosted' : 'This server'); o.value = ''; select.append(o); }
  if(preferred!==undefined && ![...select.options].some(o=>o.value===preferred)){const o=node('option','Worker unavailable');o.value=preferred;select.append(o);}
  select.value = preferred !== undefined ? preferred : available.find(w=>w.status==='online'&&w.manifest.pairs?.length)?.id || (workersEnabled && (accountMode || !hostedEnabled) ? '__choose__' : '');
}
function executionReady() {
  const id=$('execution-worker').value;
  if(!id)return hostedEnabled;
  const worker=workers.find(w=>w.id===id);
  return worker?.status==='online' && !!worker.manifest.pairs?.length;
}
function updateExecutionHint() {
  const id = $('execution-worker').value,worker=workers.find(w=>w.id===id),ready=executionReady();
  $('execution-hint').textContent = id === '__choose__' ? 'Run a worker on a machine that can reach both databases. Credentials and checkpoints stay there.' : id ? 'This worker supplies its approved connections and vector dimensions. Credentials stay on your machine.' : accountMode ? 'Polign runs this transfer, stores credentials encrypted, and processes your vectors.' : 'This server process runs the transfer using its configured connections. In the local demo, data stays on your machine.';
  $('execution-blocked').hidden=!!ready;$('job-settings').hidden=!ready;$('job-settings').disabled=!ready;$('submit-job').hidden=!ready;$('submit-job').disabled=!ready||submitting;
  if(!ready){
    $('execution-blocked-title').textContent=id==='__choose__'?'Connect a worker to get started':worker?.status==='offline'?'Reconnect your worker':worker?.status==='awaiting enrollment'?'Finish worker setup':'Worker is not ready';
    $('execution-blocked-message').textContent=id==='__choose__'?'Configure your source and destination once, run the setup script, then return with everything filled in. You can also choose Polign hosted above.':worker?.status==='offline'?'Start the worker on its original machine. This form updates automatically when it reconnects.':'Open setup to see its status and the next action. Your job will be created after the worker is connected.';
    $('execution-next').href=worker?'/connections/new?worker='+encodeURIComponent(id):'/connections/new';$('execution-next').textContent=worker?'Continue worker setup →':'Set up a worker →';
  }
}
function suggestWorkerDimension() {
  const form = $('job-form'), worker = workers.find(w => w.id === $('execution-worker').value);
  const pairs = worker?.manifest.pairs?.filter(p => p.source === form.elements.source.value && p.sink === form.elements.sink.value) || [];
  const dimensions=[...new Set(pairs.map(p=>p.dimension))],select=$('worker-dimension'),previous=Number(form.elements.dimension.value);
  select.replaceChildren();for(const dimension of dimensions){const option=node('option',String(dimension));option.value=dimension;select.append(option);}
  if(dimensions.includes(previous))select.value=previous;
  if(dimensions.length)form.elements.dimension.value=select.value;
  else if(!worker&&connections.find(c=>c.name===form.elements.source.value)?.kind==='demo')form.elements.dimension.value=4;
  form.elements.dimension.readOnly=!!worker;
  $('worker-dimension-label').hidden=dimensions.length<2;form.elements.dimension.parentElement.hidden=dimensions.length>1;
}
$('worker-dimension').onchange=()=>{$('job-form').elements.dimension.value=$('worker-dimension').value;submissionKey=crypto.randomUUID();};
function suggestJobName(){const form=$('job-form');if(!form.elements.name.value&&form.elements.source.value&&form.elements.sink.value&&!['__new__'].includes(form.elements.source.value)&&form.elements.sink.value!=='__new__')form.elements.name.value=(form.elements.source.selectedOptions[0].textContent.split(' · ')[0]+' → '+form.elements.sink.selectedOptions[0].textContent.split(' · ')[0]).slice(0,160);}
function syncOpenForm(){
  if(!$('job-dialog').open||submitting)return;
  const id=$('execution-worker').value;fillWorkerChoices(id);updateExecutionHint();
  if(!id)return;
  const signature=JSON.stringify(workers.find(w=>w.id===id)?.manifest||null);
  if(signature===manifestSignature)return;manifestSignature=signature;
  const form=$('job-form'),source=form.elements.source.value,sink=form.elements.sink.value;
  fillConnectionChoices('source',source);fillConnectionChoices('sink',sink);suggestWorkerDimension();suggestJobName();
}
$('execution-worker').onchange = () => { updateExecutionHint(); for (const role of ['source', 'sink']) fillConnectionChoices(role); suggestWorkerDimension();suggestJobName();manifestSignature='';submissionKey = crypto.randomUUID(); };
$('job-form').addEventListener('input',()=>{if(!submitting)submissionKey=crypto.randomUUID();});
function renderWorkers() {
  $('workers-panel').hidden = !workersEnabled; const list = $('workers-list'); list.replaceChildren();
  if (!workers.length) list.append(node('p', 'Set up a worker once, then create transfers using its connected databases.', 'muted'));
  for (const w of workers) {
    const row = node('div', undefined, 'worker-row'), detail = node('div');
    detail.append(node('strong', w.name), node('small', `${w.status} · ${w.manifest.pairs?.length || 0} approved pairs${w.last_seen ? ' · last seen ' + date(w.last_seen) : ''}`, 'job-id')); row.append(detail);
    if(w.status!=='revoked'){
      const next=node('a',w.status==='online'&&w.manifest.pairs?.length?'Create transfer':'Continue setup','quiet');next.href='/connections/new?worker='+encodeURIComponent(w.id);
      if(w.status==='online'&&w.manifest.pairs?.length){next.href='/?new=1&worker='+encodeURIComponent(w.id);next.onclick=event=>{event.preventDefault();openForm(w.id);};}row.append(next);
    }
    if (w.status !== 'revoked') { const revoke = node('button', 'Revoke', 'quiet'); revoke.onclick = async () => { if (!confirm(`Revoke ${w.name}? It will lose control-plane access. Running transfers stop when the worker detects revocation or its lease expires.`)) return; revoke.disabled = true; try { await api(`/workers/${w.id}/revoke`, {method:'POST',body:'{}'}); await refresh(); } catch (err) { notice(err.message); revoke.disabled = false; } }; row.append(revoke); }
    list.append(row);
  }
}
initialize(); setInterval(refresh, 2000);
