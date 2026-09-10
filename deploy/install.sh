#!/bin/bash
set -euo pipefail
umask 077

# Executed as root by cloud-init after checking the release bundle's SHA-256.
# Tunnel credentials are fetched directly to a private file, never logged.
cd /opt/vector-transfer
chmod 0755 /opt/vector-transfer
id vtransfer >/dev/null 2>&1 || useradd --system --home-dir /var/lib/vector-transfer --shell /sbin/nologin vtransfer
id cloudflared >/dev/null 2>&1 || useradd --system --no-create-home --shell /sbin/nologin cloudflared
install -d -m 0750 -o root -g vtransfer /etc/vector-transfer
install -d -m 0700 -o vtransfer -g vtransfer /var/lib/vector-transfer
install -d -m 0750 -o root -g cloudflared /etc/cloudflared
install -m 0640 -o root -g vtransfer deploy/account-config.json /etc/vector-transfer/account-config.json
if [ ! -f /etc/vector-transfer/connections.json ]; then
  install -m 0640 -o root -g vtransfer deploy/connections.initial.json /etc/vector-transfer/connections.json
fi
# Download before replacing the existing key so a failed fetch cannot truncate it.
key_tmp=$(mktemp /etc/vector-transfer/.credential-key.XXXXXX)
trap 'rm -f "$key_tmp"' EXIT
aws ssm get-parameter --region us-east-1 --name /polign/vector-transfer/credential-key --with-decryption --query Parameter.Value --output text > "$key_tmp"
chown root:vtransfer "$key_tmp"
chmod 0640 "$key_tmp"
mv "$key_tmp" /etc/vector-transfer/credential.key
aws ssm get-parameter --region us-east-1 --name /polign/vector-transfer/tunnel --with-decryption --query Parameter.Value --output text > /etc/cloudflared/vector-transfer.json
chown root:cloudflared /etc/cloudflared/vector-transfer.json
chmod 0640 /etc/cloudflared/vector-transfer.json
install -m 0640 -o root -g cloudflared deploy/cloudflared.yml /etc/cloudflared/vector-transfer.yml
chmod 0755 /opt/vector-transfer/vtransfer /opt/vector-transfer/cloudflared
if [ -d /opt/vector-transfer/downloads ]; then
  chmod 0755 /opt/vector-transfer/downloads
  chmod 0644 /opt/vector-transfer/downloads/vtransfer-linux-amd64 /opt/vector-transfer/downloads/vtransfer-linux-arm64 /opt/vector-transfer/downloads/vtransfer-darwin-arm64 /opt/vector-transfer/downloads/SHA256SUMS
fi
install -m 0644 deploy/vector-transfer.service /etc/systemd/system/vector-transfer.service
install -m 0644 deploy/cloudflared.service /etc/systemd/system/vector-transfer-tunnel.service
systemctl daemon-reload
systemctl enable --now vector-transfer.service
systemctl enable --now vector-transfer-tunnel.service
