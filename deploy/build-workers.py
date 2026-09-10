#!/usr/bin/env python3
"""Cross-compile public customer worker binaries from this source checkout."""
import hashlib
import os
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent.parent
out = root / ".deploy" / "downloads"
out.mkdir(parents=True, exist_ok=True)
sums = []
for system, arch in [("linux", "amd64"), ("linux", "arm64"), ("darwin", "arm64")]:
    name = f"vtransfer-{system}-{arch}"
    env = dict(os.environ, CGO_ENABLED="0", GOOS=system, GOARCH=arch)
    subprocess.run(["go", "build", "-trimpath", "-o", str(out / name), "./cmd/vtransfer"],
                   cwd=root, env=env, check=True)
    sums.append(f"{hashlib.sha256((out / name).read_bytes()).hexdigest()}  {name}")
(out / "SHA256SUMS").write_text("\n".join(sums) + "\n")
print("Built three worker binaries and SHA256SUMS; private state is excluded.")
