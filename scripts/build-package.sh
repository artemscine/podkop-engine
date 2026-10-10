#!/usr/bin/env bash
# Pure-Go package build; payload follows openwrt/podkop-engine/Makefile.
set -euo pipefail
project=$(cd "$(dirname "$0")/.." && pwd)
source "$project/versions.env"
architecture=${1:?Usage: build-package.sh x86_64|aarch64_cortex-a53 OUTPUT}
output=${2:?Output directory required}
case "$architecture" in x86_64) goarch=amd64;; aarch64_cortex-a53) goarch=arm64;; *) exit 2;; esac
mkdir -p "$output"; output=$(cd "$output" && pwd)
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
version=$SB_1_13_VERSION; revision=$SB_1_13_RELEASE
curl -fsSL --retry 3 "https://codeload.github.com/SagerNet/sing-box/tar.gz/v$version" -o "$work/source.tar.gz"
printf '%s  %s\n' "$SB_1_13_HASH" "$work/source.tar.gz" | sha256sum -c - >/dev/null
mkdir "$work/source"; tar -xzf "$work/source.tar.gz" --strip-components=1 -C "$work/source"
for patch_file in "$project/patches/v1.13"/*.patch; do patch -d "$work/source" -p1 --batch < "$patch_file" >/dev/null; done
(cd "$work/source" && CGO_ENABLED=0 GOOS=linux GOARCH=$goarch go build -trimpath \
 -tags with_quic,with_utls,with_clash_api,podkop_slim,badlinkname,tfogo_checklinkname0 \
 -ldflags "-s -w -buildid= -checklinkname=0 -X github.com/sagernet/sing-box/constant.Version=$version-pdk-r$revision" \
 -o "$output/sing-box-$architecture" ./cmd/sing-box)
payload=$work/data; control=$work/control; mkdir -p "$payload" "$control"
install -Dm755 "$output/sing-box-$architecture" "$payload/usr/bin/sing-box"
files=$project/openwrt/podkop-engine/files
install -Dm644 "$files/config.json" "$payload/etc/sing-box/config.json"
install -Dm644 "$files/sing-box.conf" "$payload/etc/config/sing-box"
for name in sing-box podkop-engine; do install -Dm755 "$files/$name.init" "$payload/etc/init.d/$name"; done
install -Dm755 "$files/podkop-engine.uci-defaults" "$payload/etc/uci-defaults/99-podkop-engine"
for name in lists-recovered failsafe; do install -Dm755 "$files/$name" "$payload/usr/libexec/podkop-engine/$name"; done
install -Dm644 "$project/LICENSE" "$payload/usr/share/licenses/podkop-engine/LICENSE"
name=podkop-engine_${version}-r${revision}_openwrt_${architecture}
mkdir -p "$work/apk-root/etc"
printf 'root:x:%s:%s:root:/root:/bin/sh\n' "$(id -u)" "$(id -g)" > "$work/apk-root/etc/passwd"
printf 'root:x:%s:root\n' "$(id -g)" > "$work/apk-root/etc/group"
apk --root "$work/apk-root" mkpkg --info name:podkop-engine --info "version:$version-r$revision" \
 --info "arch:$architecture" --info 'description:Proxy engine for OpenWrt Podkop' \
 --info license:GPL-3.0-or-later --info maintainer:artemscine \
 --info 'url:https://github.com/artemscine/podkop-engine' --info depends:ca-bundle \
 --info provides:sing-box --info 'depends:!sing-box-tiny' --info provider-priority:100 \
 --files "$payload" --output "$output/$name.apk"
cat > "$control/control" <<CONTROL
Package: podkop-engine
Version: $version-r$revision
Architecture: $architecture
Maintainer: artemscine
Section: net
Priority: optional
License: GPL-3.0-or-later
Depends: ca-bundle
Provides: sing-box
Conflicts: sing-box, sing-box-tiny
Description: Proxy engine for OpenWrt Podkop.
CONTROL
printf '/etc/config/sing-box\n/etc/sing-box/config.json\n' > "$control/conffiles"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf - -C "$payload" . | gzip -n > "$work/data.tar.gz"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf - -C "$control" . | gzip -n > "$work/control.tar.gz"
printf '2.0\n' > "$work/debian-binary"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -cf - -C "$work" ./debian-binary ./control.tar.gz ./data.tar.gz | gzip -n > "$output/$name.ipk"
(cd "$output" && sha256sum "$name.apk" "$name.ipk" "sing-box-$architecture")
