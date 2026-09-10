#!/usr/bin/env python3
"""Preserve the live public client's settings while adding transfer callbacks.

Run without --apply to review the two URL-list changes. The matching IaC change
lives in polign_account/template.yaml and its TransferBaseURL SAM parameter.
"""
import argparse
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--apply", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent
state = root / ".deploy"
state.mkdir(mode=0o700, exist_ok=True)
base = ["aws", "--profile", "polign", "--region", "us-east-1"]

def aws(*arguments):
    return json.loads(subprocess.check_output(base + list(arguments), text=True))

identity = aws("sts", "get-caller-identity")
if identity["Account"] != "298123941500":
    raise SystemExit("Refusing to modify a different AWS account")
before = aws("cognito-idp", "describe-user-pool-client", "--user-pool-id",
             "us-east-1_eDB5WomyV", "--client-id", "2u8blo7ld5ft0hvp222tamill9")["UserPoolClient"]
if before.get("ClientSecret"):
    raise SystemExit("Expected the existing public app client, not a secret-bearing client")
supported = aws("cognito-idp", "update-user-pool-client", "--generate-cli-skeleton", "input")
request = {key: value for key, value in before.items() if key in supported}
for key, url in {"CallbackURLs": "https://transfer.polign.com/auth/callback",
                 "LogoutURLs": "https://transfer.polign.com/"}.items():
    request[key] = list(before.get(key, []))
    if url not in request[key]:
        request[key].append(url)
    print(f"{key}: {json.dumps(before.get(key, []))} -> {json.dumps(request[key])}")

path = state / "cognito-update.json"
path.write_text(json.dumps(request, indent=2) + "\n")
path.chmod(0o600)
if args.apply:
    backup = state / "cognito-before.json"
    if not backup.exists():
        backup.write_text(json.dumps({k: v for k, v in before.items() if k in supported}, indent=2) + "\n")
        backup.chmod(0o600)
    aws("cognito-idp", "update-user-pool-client", "--cli-input-json", "file://" + str(path))
    after = aws("cognito-idp", "describe-user-pool-client", "--user-pool-id",
                "us-east-1_eDB5WomyV", "--client-id", "2u8blo7ld5ft0hvp222tamill9")["UserPoolClient"]
    for key, value in before.items():
        if key in ("CallbackURLs", "LogoutURLs", "LastModifiedDate"):
            continue
        if after.get(key) != value:
            raise SystemExit(f"Unexpected change to {key}; inspect before proceeding")
    for key in ("CallbackURLs", "LogoutURLs"):
        if set(after[key]) != set(request[key]):
            raise SystemExit(f"Failed to verify {key}")
    print("Verified: callbacks added; all other existing client settings preserved.")
else:
    print("Review only; no Cognito settings changed. Run with --apply to apply this extension.")
