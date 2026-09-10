#!/usr/bin/env python3
"""Package the prebuilt Linux/arm64 binaries and an explicit public-file allowlist."""
import hashlib
from pathlib import Path
import tarfile

root = Path(__file__).resolve().parent.parent
state = root / ".deploy"
state.mkdir(mode=0o700, exist_ok=True)
public_files = ["account-config.json", "connections.initial.json", "cloudflared.yml",
                "cloudflared.service", "vector-transfer.service", "install.sh"]
with tarfile.open(state / "release.tar.gz", "w:gz") as archive:
    for name in ["vtransfer", "cloudflared"]:
        archive.add(state / name, arcname=name)
    for name in ["vtransfer-linux-amd64", "vtransfer-linux-arm64", "vtransfer-darwin-arm64", "SHA256SUMS"]:
        archive.add(state / "downloads" / name, arcname="downloads/" + name)
    for name in public_files:
        archive.add(root / "deploy" / name, arcname="deploy/" + name)
sha = hashlib.sha256((state / "release.tar.gz").read_bytes()).hexdigest()
(state / "release.sha256").write_text(sha + "\n")
(state / "release-key").write_text("releases/" + sha + ".tar.gz\n")
print("Release SHA-256:", sha)
print("Credential files are excluded from the release bundle.")
