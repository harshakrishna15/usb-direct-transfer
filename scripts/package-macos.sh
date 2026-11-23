#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
binary="${1:-target/release/usb-transfer-gui}"
if [[ "$(uname -s)" != Darwin ]]; then
    echo "Run this script on macOS." >&2
    exit 1
fi
if [[ ! -f "$binary" || ! -x "$binary" ]]; then
    echo "Build the GUI first: cargo build --locked --release --features gui --bin usb-transfer-gui" >&2
    exit 1
fi

release="$PWD/target/release"
app="$release/USB Direct Transfer.app"
version="$(awk -F '"' '/^version = / { print $2; exit }' Cargo.toml)"
mkdir -p "$release"
scratch="$(mktemp -d "$release/.gui-package.XXXXXX")"
prepared="$scratch/USB Direct Transfer.app"
previous="$scratch/previous.app"

cleanup() {
    if [[ ( -e "$previous" || -L "$previous" ) && ! -e "$app" && ! -L "$app" ]]; then
        mv "$previous" "$app" || return
    fi
    rm -rf "$scratch"
}
trap cleanup EXIT

mkdir -p "$prepared/Contents/MacOS"
install -m 755 "$binary" "$prepared/Contents/MacOS/usb-transfer-gui"
cat > "$prepared/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>USB Direct Transfer</string>
    <key>CFBundleExecutable</key>
    <string>usb-transfer-gui</string>
    <key>CFBundleIdentifier</key>
    <string>local.usb-direct-transfer</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleShortVersionString</key>
    <string>$version</string>
    <key>CFBundleVersion</key>
    <string>$version</string>
    <key>NSLocalNetworkUsageDescription</key>
    <string>Find and connect to the other computer over the direct USB cable.</string>
</dict>
</plist>
PLIST

/usr/bin/plutil -lint "$prepared/Contents/Info.plist" >/dev/null
/usr/bin/codesign --force --sign - --timestamp=none "$prepared"
/usr/bin/codesign --verify --strict "$prepared"
if [[ -e "$app" || -L "$app" ]]; then
    mv "$app" "$previous"
fi
mv "$prepared" "$app"
printf '%s\n' "$app"
