#!/usr/bin/env bash
set -euo pipefail
project=$(cd "$(dirname "$0")/.." && pwd)
source "$project/versions.env"
line=${1:-1.13}
key=${line//./_}
version_var=SB_${key}_VERSION
hash_var=SB_${key}_HASH
version=${!version_var}
expected=${!hash_var}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
curl -fsSL --retry 3 "https://codeload.github.com/SagerNet/sing-box/tar.gz/v$version" -o "$work/source.tar.gz"
printf '%s  %s\n' "$expected" "$work/source.tar.gz" | sha256sum -c - >/dev/null
mkdir "$work/source"
tar -xzf "$work/source.tar.gz" --strip-components=1 -C "$work/source"
for patch_file in "$project/patches/v$line"/*.patch; do
  patch -d "$work/source" -p1 --batch < "$patch_file" >/dev/null
done
cd "$work/source"
tags=with_quic,with_utls,with_clash_api,podkop_slim,badlinkname,tfogo_checklinkname0
go vet -tags "$tags" ./protocol/group ./protocol/vless/... ./cmd/sing-box
# Existing network, download and ordinary URLTest tests must pass with the option omitted.
go test -ldflags=-checklinkname=0 -tags "$tags" ./protocol/group ./protocol/vless/... ./cmd/sing-box
if [[ ${TEST_RACE:-0} == 1 ]]; then
  go test -race -ldflags=-checklinkname=0 -tags "$tags" -run 'FallbackRecovery|URLTest.*(Fallback|Network|Failure|Download)' ./protocol/group
fi
