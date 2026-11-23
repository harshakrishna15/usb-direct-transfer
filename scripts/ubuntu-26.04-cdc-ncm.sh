#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: ubuntu-26.04-cdc-ncm.sh SOURCE_DIR [OUTPUT_DIR]

Build an unsigned cdc_ncm module for the running Ubuntu 26.04 kernel.
SOURCE_DIR must be the unpacked Ubuntu kernel source package matching the
installed package that contains the stock module. The running kernel's
headers must be installed. The script writes a build artifact only.
OUTPUT_DIR defaults to target/ubuntu-26.04-cdc-ncm/<kernel release>.
EOF
}

fail() {
    printf '%s\n' "$*" >&2
    exit 1
}

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    usage
    exit 0
fi
if (( $# < 1 || $# > 2 )); then
    usage >&2
    exit 2
fi
[[ $(uname -s) == Linux ]] || fail 'Run this helper on your Ubuntu 26.04 computer.'
ID= VERSION_ID=
. /etc/os-release
[[ $ID == ubuntu && $VERSION_ID == 26.04 ]] || fail 'This helper targets Ubuntu 26.04.'

for tool in dpkg-query dpkg-parsechangelog make modinfo python3; do
    command -v "$tool" >/dev/null || fail "Missing $tool. Check build-essential, dpkg-dev, kmod, and python3."
done

kernel=$(uname -r)
[[ $kernel =~ ^[a-zA-Z0-9._+-]+$ ]] || fail 'The running kernel release has an unexpected format.'
headers="/lib/modules/$kernel/build"
[[ -f $headers/Makefile && -s $headers/Module.symvers ]] || fail "Install headers for the running kernel ($kernel)."
grep -Fxq "#define UTS_RELEASE \"$kernel\"" "$headers/include/generated/utsrelease.h" ||
    fail "Kernel headers do not match $kernel."

stock=$(dpkg-query -S "*/modules/$kernel/kernel/drivers/net/usb/cdc_ncm.ko*" 2>/dev/null) ||
    fail "Could not find a packaged cdc_ncm module for $kernel."
[[ $stock != *$'\n'* && $stock == *': /'* ]] || fail 'Expected one packaged stock cdc_ncm module.'
stock_package=${stock%%: /*}
stock_file=${stock#*: }
[[ -f $stock_file ]] || fail "Stock module is missing at $stock_file."
stock_vermagic=$(modinfo -F vermagic "$stock_file")
[[ $stock_vermagic == "$kernel "* ]] || fail 'Stock module does not match the running kernel.'
package_info=$(dpkg-query -W -f='${db:Status-Status}\n${source:Package}\n${source:Version}\n' "$stock_package")
{
    read -r package_status
    read -r source_package
    read -r source_version
} <<< "$package_info" || fail 'Could not read the stock module package metadata.'
[[ $package_status == installed && -n $source_package && -n $source_version ]] ||
    fail 'Could not identify the installed kernel source package and version.'
dkms_version="${source_version//:/-}-$kernel"
[[ $dkms_version =~ ^[a-zA-Z0-9._+~-]+$ ]] || fail 'The kernel source version has an unexpected format.'

source_dir=$1
source_file="$source_dir/drivers/net/usb/cdc_ncm.c"
[[ -f $source_file ]] || fail "Could not find $source_file."
matching_source=false
for changelog in "$source_dir"/debian*/changelog; do
    [[ -f $changelog ]] || continue
    changelog_source=$(dpkg-parsechangelog -l "$changelog" -S Source)
    changelog_version=$(dpkg-parsechangelog -l "$changelog" -S Version)
    if [[ $changelog_source == "$source_package" && $changelog_version == "$source_version" ]]; then
        matching_source=true
        break
    fi
done
[[ $matching_source == true ]] ||
    fail "Expected exact Ubuntu kernel source $source_package=$source_version for $kernel."

output=${2:-target/ubuntu-26.04-cdc-ncm/$kernel}
build=$(mktemp -d)
trap 'rm -rf -- "$build"' EXIT
python3 - "$source_file" "$build/cdc_ncm.c" <<'PY'
import pathlib
import re
import sys

source, destination = map(pathlib.Path, sys.argv[1:])
text = source.read_text()
private = re.search(r"^static const struct driver_info apple_private_interface_info = \{\n(.*?)^};", text, re.M | re.S)
if not private or "FLAG_LINK_INTR" in private[1]:
    sys.exit("Unexpected Apple private NCM driver definition in the Ubuntu source.")

table = re.search(r"^static const struct usb_device_id cdc_devs\[\] = \{\n(.*?)^};", text, re.M | re.S)
anchor = "\t/* Generic CDC-NCM devices */\n"
if not table or table[1].count(anchor) != 1:
    sys.exit("Could not locate the generic CDC-NCM device match in the Ubuntu source.")

entry = re.compile(
    r"\{\s*USB_DEVICE_INTERFACE_NUMBER\(0x05ac,\s*0x(190[235]),\s*([02])\),"
    r"\s*\.driver_info = \(unsigned long\)&apple_private_interface_info,\s*\}",
    re.I,
)
before, after = table[1].split(anchor)
if re.search(r"\b0x190[235]\b", entry.sub("", before) + after, re.I):
    sys.exit("Unexpected Mac USB ID entry. Verify the Ubuntu kernel source before building.")

existing = set(entry.findall(before))
missing = [(pid, interface) for pid in ("1902", "1903", "1905")
           for interface in ("0", "2") if (pid, interface) not in existing]
if missing:
    # Upstream a5148bc2fa27, 746fc0787f61, and a8d0451967fc use this same driver info.
    entries = "\t/* Apple Mac USB-C direct networking */\n" + "".join(
        f"\t{{ USB_DEVICE_INTERFACE_NUMBER(0x05ac, 0x{pid}, {interface}),\n"
        "\t\t.driver_info = (unsigned long)&apple_private_interface_info,\n\t},\n"
        for pid, interface in missing
    )
    offset = table.start(1) + len(before)
    text = text[:offset] + entries + text[offset:]
destination.write_text(text)
PY
printf '%s\n' 'obj-m := cdc_ncm.o' > "$build/Makefile"
make -C "$headers" M="$build" modules

module="$build/cdc_ncm.ko"
[[ $(modinfo -F vermagic "$module") == "$stock_vermagic" ]] || fail 'Built module vermagic differs from the stock module.'
aliases=$(modinfo -F alias "$module")
for product in 1902 1903 1905; do
    for interface in 00 02; do
        grep -Eiq "^usb:v05acp${product}d.*in${interface}\*$" <<< "$aliases" ||
            fail "Built module is missing the 05ac:$product interface $interface alias."
    done
done

kernel_pattern=${kernel//./[.]}
kernel_pattern=${kernel_pattern//+/[+]}
cat > "$build/dkms.conf" <<EOF
PACKAGE_NAME="usb-direct-transfer-cdc-ncm"
PACKAGE_VERSION="$dkms_version"
BUILT_MODULE_NAME[0]="cdc_ncm"
DEST_MODULE_LOCATION[0]="/kernel/drivers/net/usb"
AUTOINSTALL="no"
BUILD_EXCLUSIVE_KERNEL="^${kernel_pattern}\$"
EOF

mkdir -p -- "$output"
install -m 0644 -- "$module" "$output/cdc_ncm.ko"
install -m 0644 -- "$build/cdc_ncm.c" "$build/Makefile" "$build/dkms.conf" "$output/"
printf 'Built unsigned module at %s/cdc_ncm.ko\n' "$output"
printf 'Prepared DKMS source in %s for usb-direct-transfer-cdc-ncm/%s\n' "$output" "$dkms_version"
