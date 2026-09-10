// All dynamic shell values are single-quoted, including generated JSON.
function shellLiteral(value) { return "'" + String(value).replaceAll("'", "'\"'\"'") + "'"; }
function generateWorkerScript(options) {
  const {origin,workerID,enrollmentToken,platform,checksums,connections,pair,credentials}=options;
  const base=new URL(origin),local=['localhost','127.0.0.1','[::1]'].includes(base.hostname);
  if((base.protocol!=='https:'&&!(base.protocol==='http:'&&local))||base.origin!==origin)throw new Error('Invalid control-plane origin.');
  if(!/^[a-f0-9]{32}$/.test(workerID)||enrollmentToken.slice(0,33)!==workerID+'.'||!/^[a-f0-9]{64}$/.test(enrollmentToken.slice(33)))throw new Error('Invalid enrollment response.');
  if(!['auto','linux-amd64','linux-arm64','darwin-arm64'].includes(platform))throw new Error('Invalid platform.');
  for(const name of ['vtransfer-linux-amd64','vtransfer-linux-arm64','vtransfer-darwin-arm64'])if(!/^[a-f0-9]{64}$/.test(checksums[name]||''))throw new Error('Download checksums unavailable.');
  if(!Number.isInteger(pair.dimension)||pair.dimension<1||pair.dimension>65536||pair.source===pair.sink||!connections[pair.source]||!connections[pair.sink])throw new Error('Choose two connections and a valid dimension.');
  const prompts=[];
  for(const c of credentials){
    if(!/^(SOURCE|DESTINATION)_(API_KEY|USERNAME|PASSWORD)$/.test(c.name))throw new Error('Invalid credential reference.');
    prompts.push('if [ -z "${'+c.name+':-}" ]; then',
      '  IFS= read -r -s -p '+shellLiteral(c.label+': ')+' '+c.name+' < /dev/tty',
      '  printf "\\n" >&2','fi');
    if(c.required)prompts.push('if [ -z "${'+c.name+':-}" ]; then printf "%s\\n" '+shellLiteral(c.label+' is required.')+' >&2; exit 1; fi');
    prompts.push('export '+c.name);
  }
  const localFlag=base.protocol==='http:'?' -allow-http-loopback':'';
  const runner=['#!/usr/bin/env bash','set -euo pipefail','set +x','umask 077',
    'worker_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"',...prompts,
    'exec "$worker_dir/vtransfer" worker run -data "$worker_dir/state" -config "$worker_dir/worker.json"'+localFlag,''].join('\n');
  const lines=['#!/usr/bin/env bash','# Contains a single-use enrollment token. Review before running; delete after setup.',
    'set -euo pipefail','set +x','umask 077','worker_dir="${VECTOR_TRANSFER_WORKER_DIR:-${HOME}/.polign-transfer/'+workerID+'}"',
    'if [ -e "$worker_dir/setup.complete" ]; then',
    '  printf "Worker already installed. Start it with: bash \\"%s/run-worker.sh\\"\\n" "$worker_dir"','  exit 0','fi',
    'command -v curl >/dev/null || { printf "curl is required.\\n" >&2; exit 1; }',
    'platform='+shellLiteral(platform),
    'if [ "$platform" = auto ]; then','  case "$(uname -s)/$(uname -m)" in',
    '    Linux/x86_64) platform=linux-amd64 ;;','    Linux/aarch64|Linux/arm64) platform=linux-arm64 ;;',
    '    Darwin/arm64) platform=darwin-arm64 ;;','    *) printf "Unsupported platform. Use Linux x64, Linux ARM64, or macOS Apple Silicon.\\n" >&2; exit 1 ;;','  esac','fi',
    'case "$platform" in',
    '  linux-amd64) expected='+shellLiteral(checksums['vtransfer-linux-amd64'])+' ;;',
    '  linux-arm64) expected='+shellLiteral(checksums['vtransfer-linux-arm64'])+' ;;',
    '  darwin-arm64) expected='+shellLiteral(checksums['vtransfer-darwin-arm64'])+' ;;','esac',
    'mkdir -p "$worker_dir/state"','chmod 700 "$worker_dir" "$worker_dir/state"',
    'download_dir=$(mktemp -d "$worker_dir/.download.XXXXXX")','enrollment_file="$download_dir/enrollment.token"',
    "trap 'rm -rf \"$download_dir\"' EXIT",
    'curl --fail --silent --show-error --max-time 120 '+shellLiteral(origin+'/downloads/vtransfer-')+'"$platform" -o "$download_dir/vtransfer"',
    'if command -v sha256sum >/dev/null; then','  actual=$(sha256sum "$download_dir/vtransfer")',
    'elif command -v shasum >/dev/null; then','  actual=$(shasum -a 256 "$download_dir/vtransfer")',
    'else printf "sha256sum or shasum is required.\\n" >&2; exit 1; fi','actual=${actual%% *}',
    'if [ "$actual" != "$expected" ]; then printf "Checksum mismatch. Generate a new setup script.\\n" >&2; exit 1; fi',
    'chmod 700 "$download_dir/vtransfer"','mv "$download_dir/vtransfer" "$worker_dir/vtransfer"',
    'printf "%s\\n" '+shellLiteral(JSON.stringify({connections},null,2))+' > "$worker_dir/connections.json"',
    'printf "%s\\n" '+shellLiteral(JSON.stringify({connections_file:'connections.json',pairs:[pair]},null,2))+' > "$worker_dir/worker.json"',
    'printf "%s\\n" '+shellLiteral(runner)+' > "$worker_dir/run-worker.sh"',
    'chmod 600 "$worker_dir/connections.json" "$worker_dir/worker.json" "$worker_dir/run-worker.sh"',
    'printf "%s\\n" '+shellLiteral(enrollmentToken)+' > "$enrollment_file"',
    '"$worker_dir/vtransfer" worker enroll -url '+shellLiteral(origin)+' -data "$worker_dir/state" -token-file "$enrollment_file"'+localFlag,
    'rm -rf "$download_dir"','trap - EXIT','touch "$worker_dir/setup.complete"',
    'printf "Worker installed in %s. Keep this process running and submit transfers in the dashboard.\\n" "$worker_dir"',
    'exec bash "$worker_dir/run-worker.sh"',''];
  return lines.join('\n');
}
