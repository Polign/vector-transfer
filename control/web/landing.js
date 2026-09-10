// The public quickstart also works without JavaScript using the default platform.
(() => {
  const platform = document.getElementById('demo-platform');
  const command = document.getElementById('demo-command');
  const copy = document.getElementById('copy-demo');
  const status = document.getElementById('copy-demo-status');
  if (!platform || !command || !copy || !status) return;
  platform.disabled = false; copy.hidden = false;
  platform.addEventListener('change', () => {
    const allowed = ['darwin-arm64', 'linux-amd64', 'linux-arm64'];
    if (!allowed.includes(platform.value)) return;
    const binary = `vtransfer-${platform.value}`;
    const check = platform.value === 'darwin-arm64' ? 'shasum -a 256' : 'sha256sum';
    command.textContent = `curl -fSLO https://transfer.polign.com/downloads/${binary} &&\ncurl -fSLO https://transfer.polign.com/downloads/SHA256SUMS &&\n${check} --ignore-missing --check SHA256SUMS &&\nchmod +x ${binary} &&\n./${binary} serve -demo -data ./transfer-demo`;
    status.textContent = 'Run in an empty folder. Requires curl; no Go installation needed.';
  });
  copy.addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(command.textContent);
      status.textContent = 'Commands copied. Paste into a terminal in an empty folder.';
    } catch {
      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(command); selection.removeAllRanges(); selection.addRange(range);
      status.textContent = 'Commands selected. Copy them with your keyboard or selection menu.';
    }
  });
})();
