import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,existsSync,readdirSync,statSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {execFileSync,spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
const scope={URL};vm.createContext(scope);vm.runInContext(readFileSync('control/web/worker-script.js','utf8'),scope);
function fixture(t){
 const dir=mkdtempSync(join(tmpdir(),'transfer-script-'));t.after(()=>rmSync(dir,{recursive:true,force:true}));const bin=join(dir,'bin');mkdirSync(bin);
 const fake=join(dir,'fake-worker');writeFileSync(fake,`#!/usr/bin/env bash
set -eu
if [ "$2" = enroll ]; then
  while [ "$#" -gt 0 ]; do
    case "$1" in -token-file) shift; test -s "$1" ;; -data) shift; mkdir -p "$1"; printf '{}' > "$1/identity.json" ;; esac
    shift
  done
  exit "\${FIXTURE_ENROLL_STATUS:-0}"
fi
test "$2" = run
test "$SOURCE_API_KEY" = fixture-source-secret
test "$DESTINATION_API_KEY" = fixture-sink-secret
printf 'started' > "$VECTOR_TRANSFER_WORKER_DIR/fixture-started"
`,{mode:0o700});
 writeFileSync(join(bin,'curl'),'#!/usr/bin/env bash\nset -eu\ncp "$FIXTURE_BINARY" "${@: -1}"\n',{mode:0o700});
 const digest=createHash('sha256').update(readFileSync(fake)).digest('hex'),id='a'.repeat(32);
 const injection="collection'; $(touch "+join(dir,'injected')+"); `touch "+join(dir,'injected-2')+"`\nnext-line";
 const options={origin:'https://transfer.example',workerID:id,enrollmentToken:id+'.'+'b'.repeat(64),platform:'auto',checksums:Object.fromEntries(['linux-amd64','linux-arm64','darwin-arm64'].map(p=>['vtransfer-'+p,digest])),connections:{source:{kind:'pinecone',endpoint:'https://source.example',namespace:injection,api_key_env:'SOURCE_API_KEY',read_only:true},sink:{kind:'polign',endpoint:'https://sink.example',collection:'docs',api_key_env:'DESTINATION_API_KEY',write_only:true}},pair:{source:'source',sink:'sink',dimension:2},credentials:[{name:'SOURCE_API_KEY',label:'Source API key',required:true},{name:'DESTINATION_API_KEY',label:'Destination API key',required:true}]};
 const script=join(dir,'setup.sh'),workerDir=join(dir,'worker-data');
 const env={...process.env,PATH:bin+':'+process.env.PATH,FIXTURE_BINARY:fake,VECTOR_TRANSFER_WORKER_DIR:workerDir,SOURCE_API_KEY:'fixture-source-secret',DESTINATION_API_KEY:'fixture-sink-secret'};
 return{dir,options,script,workerDir,env};
}
test('installer quotes settings, verifies downloads, enrolls, starts, and preserves state on rerun',t=>{
 const f=fixture(t),script=scope.generateWorkerScript(f.options);writeFileSync(f.script,script,{mode:0o600});
 execFileSync('bash',['-n',f.script]);execFileSync('bash',[f.script],{env:f.env});
 assert.ok(existsSync(join(f.workerDir,'fixture-started')));assert.ok(existsSync(join(f.workerDir,'state/identity.json')));
 assert.equal(existsSync(join(f.dir,'injected')),false);assert.equal(existsSync(join(f.dir,'injected-2')),false);
 assert.deepEqual(JSON.parse(readFileSync(join(f.workerDir,'connections.json'))),{connections:f.options.connections});
 assert.deepEqual(JSON.parse(readFileSync(join(f.workerDir,'worker.json'))),{connections_file:'connections.json',pairs:[f.options.pair]});
 assert.equal(statSync(join(f.workerDir,'connections.json')).mode&0o777,0o600);
 const runner=readFileSync(join(f.workerDir,'run-worker.sh'),'utf8');execFileSync('bash',['-n',join(f.workerDir,'run-worker.sh')]);
 assert.ok(runner.includes('read -r -s'));assert.ok(!runner.includes('fixture-source-secret'));assert.ok(!runner.includes(f.options.enrollmentToken));
 assert.ok(!readdirSync(f.workerDir).some(n=>n.startsWith('.download.')));
 const identity=readFileSync(join(f.workerDir,'state/identity.json'),'utf8');execFileSync('bash',[f.script],{env:f.env});assert.equal(readFileSync(join(f.workerDir,'state/identity.json'),'utf8'),identity);
});
test('checksum failure prevents enrollment and removes temporary token files',t=>{
 const f=fixture(t);for(const name of Object.keys(f.options.checksums))f.options.checksums[name]='0'.repeat(64);
 writeFileSync(f.script,scope.generateWorkerScript(f.options));const r=spawnSync('bash',[f.script],{env:f.env});assert.notEqual(r.status,0);assert.ok(!existsSync(join(f.workerDir,'state/identity.json')));assert.ok(!readdirSync(f.workerDir).some(n=>n.startsWith('.download.')));
});
test('failed enrollment never starts the worker and can be retried',t=>{
 const f=fixture(t);writeFileSync(f.script,scope.generateWorkerScript(f.options));let r=spawnSync('bash',[f.script],{env:{...f.env,FIXTURE_ENROLL_STATUS:'42'}});assert.equal(r.status,42);assert.ok(!existsSync(join(f.workerDir,'fixture-started')));assert.ok(!existsSync(join(f.workerDir,'setup.complete')));assert.ok(!readdirSync(f.workerDir).some(n=>n.startsWith('.download.')));execFileSync('bash',[f.script],{env:f.env});assert.ok(existsSync(join(f.workerDir,'fixture-started')));
});
test('rejects invalid origins, enrollment and environment references',t=>{
 const f=fixture(t);for(const overrides of [{origin:'http://remote.example'},{enrollmentToken:'bad'},{platform:'other'},{credentials:[{name:'BASH_ENV',label:'bad'}]}])assert.throws(()=>scope.generateWorkerScript({...f.options,...overrides}));
});
