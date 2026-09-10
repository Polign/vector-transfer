# Deployment

`https://transfer.polign.com` runs on one ARM64 EC2 host behind a dedicated
Cloudflare Tunnel. It uses the existing Polign account service and Cognito client.
Resource IDs, installed release, bootstrap release, and verification results are
in [live.json](live.json).

The service listens on `127.0.0.1:23005`; there are no public inbound ports.
Use AWS Systems Manager with profile `polign`, region `us-east-1` for host access.

## Build a release

From the repository root:

```sh
python3 deploy/build-workers.py
cp .deploy/downloads/vtransfer-linux-arm64 .deploy/vtransfer
python3 deploy/package-release.py
```

Packaging also requires `.deploy/cloudflared`. The deployment pins Linux ARM64
version `2026.9.0`, SHA-256:

```text
98aca3173f73248fad6180fc75dade2d186a6e54fa807e088108cb4345de8efe
```

The bundle contains the service, tunnel binary, worker downloads and checksums,
unit files, and public configuration. Credentials, identities, and job data are
excluded. Upload it to the artifact bucket from `live.json`, using
`.deploy/release-key` as the object key. Verify `.deploy/release.sha256` before installation.

## First host

1. Create the stack from [host.yaml](host.yaml) with `VpcId`, `SubnetId`, and
   `ImageId`, initially leaving `BundleKey` empty.
2. Provision the tunnel credential and a base64-encoded, random 32-byte vault key
   in their SSM SecureString parameters listed below.
3. Upload the release. Set `BundleKey` and `BundleSHA256` in a reviewed change set.
4. Cloud-init verifies and extracts the bundle, then runs [install.sh](install.sh).
5. Verify sign-in, service health, and a small transfer.

The host role reads only the release prefix, required SSM parameters, and
Systems Manager channels. It has no database permissions by default.

## Update an existing host

Use SSM to:

1. Download the bundle to a private staging directory and verify its SHA-256.
2. Back up the current binary, unit files, and public downloads for rollback.
3. Stop `vector-transfer`. Replace the binary, `deploy/` files, and `downloads/`.
4. Run `/opt/vector-transfer/deploy/install.sh` and check both services.
5. Verify `/healthz`, sign-in, protected APIs, and download checksums. Record the
   installed release in `live.json`.

Preserve `/etc/vector-transfer/` credentials and configuration and all of
`/var/lib/vector-transfer/`. A restart clears browser sessions. Hosted jobs
recover from checkpoints; customer workers reconnect using their existing identity.
Check journal compatibility before rolling back a binary.

SSM updates do not change CloudFormation's bootstrap bundle. Update bootstrap
parameters before replacing the host, and restore its data before starting work.
Customer workers do not auto-update; customers install their reviewed releases.

## Configuration and secrets

| Item | Location |
|---|---|
| Operator connections | `/etc/vector-transfer/connections.json`, root:vtransfer 0640 |
| Optional operator credentials | `/etc/vector-transfer/credentials.env`, root 0600 |
| Hosted vault key | `/etc/vector-transfer/credential.key`, root:vtransfer 0640 |
| Vault key backup | SSM `/polign/vector-transfer/credential-key` |
| Tunnel credentials | SSM `/polign/vector-transfer/tunnel` and restricted `/etc/cloudflared/vector-transfer.json` |
| Jobs, encrypted connections, worker registrations | `/var/lib/vector-transfer/`, vtransfer 0700 |
| Public worker binaries | `/opt/vector-transfer/downloads/` |

Never regenerate the vault key while encrypted connections exist. The installer
fetches it into a temporary file before replacing the host copy. Fetch secrets
on the host; do not include values in SSM commands, shell history, or logs.

Operator credentials and grants load at startup. Restart after changing them.
Private hosted connections can rotate credentials through the UI. Customer
workers keep their credentials and checkpoints in their own environment.

`-worker-downloads` serves only the three binaries and `SHA256SUMS`.
`-workers 0` disables hosted execution. See [account setup](../docs/accounts-and-security.md)
for configuration and [customer worker setup](../docs/customer-workers.md) for installation.

## Cognito callbacks

`extend-cognito.py` previews callback changes; `--apply` adds the transfer callback
and sign-out URLs while preserving existing client settings. Keep the matching
`TransferBaseURL=https://transfer.polign.com` in the sibling `polign_account`
template and SAM configuration so future account deployments retain those URLs.
Do not change unrelated account, billing, or licensing resources.

## Health checks

```sh
systemctl status vector-transfer vector-transfer-tunnel
journalctl -u vector-transfer
```

Public `/healthz` checks the stores, not database connectivity. `/v1/jobs`,
`/v1/workers`, and `/auth/session` must return 401 without account authentication.
Worker protocol requests need a separate worker session.

## Back up and restore

Back up the job journal, encrypted connections, worker registrations, operator
configuration, and the vault key. Encrypted connection files cannot be restored
without their original key.

Stop the service for a filesystem backup or take an EBS snapshot. The encrypted
root volume has `DeleteOnTermination: false`; retention is not a backup or an
automatic restore. Restore data and configuration before enabling a replacement
host. Review any instance replacement in the CloudFormation change set.

Stack deletion retains the artifact bucket and root disk. Tunnel/DNS records and
SSM parameters are managed separately. Remove only transfer-specific resources
when decommissioning; preserve the account portal and its Cognito configuration.
