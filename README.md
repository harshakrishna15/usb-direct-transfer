# USB Direct Transfer

Send files and folders in either direction between two computers over a direct USB-C network link. The CLI and desktop app use the same encrypted protocol and can pair with each other.

The first hardware target is an Apple silicon Mac and a Linux PC connected by an ordinary Apple USB-C charging cable. Either computer can send or receive once the operating systems bring up the cable interfaces. The app pins connections to recognized direct USB or Thunderbolt/USB4 interfaces and ends the session if the selected link disappears. Windows charging-cable testing follows once the target PC has a qualified driver.

## Build

Use a current stable Rust toolchain and a C compiler on each machine.

```sh
cargo build --locked --release --bin usb-transfer
cargo build --locked --release --features gui --bin usb-transfer-gui
```

The desktop app shares the **Receive**, **Send**, and **Settings** layout on macOS and Linux. After building the GUI on a Mac, package and open it with:

```sh
./scripts/package-macos.sh
open "target/release/USB Direct Transfer.app"
```

The [macOS desktop workflow](https://github.com/harshakrishna15/usb-direct-transfer/actions/workflows/macos.yml) also builds a ZIP for Apple silicon Macs. Download it from the successful run’s **Artifacts** section. The bundle uses an ad hoc signature for testing. Building locally is an option if macOS blocks the downloaded bundle.

On Linux, run `./target/release/usb-transfer` or `./target/release/usb-transfer-gui`. The prebuilt Linux x64 test binaries require glibc 2.34 or newer. The Linux app uses `ip` from `iproute2`. The Linux GUI uses OpenGL and the XDG Desktop Portal file chooser provided by your desktop session. Run the app as a regular user.

## Check the Linux cable driver

Plug the Mac directly into the Linux computer using the Apple cable. Keep the Mac awake and unlock it to respond to a USB accessory prompt if one appears. On Linux, check the USB device and its driver.

```sh
uname -r
lsusb -d 05ac:
lsusb -t
./target/release/usb-transfer doctor
```

Look for the Mac's Apple USB ID in `lsusb`, `Driver=cdc_ncm` in `lsusb -t`, and a ready cable interface in `doctor`. A rate of `480M` in the USB tree is USB 2.0 High-Speed. Upstream Linux 7.3 includes the Mac `05ac:1902` and `05ac:1905` entries. The `05ac:1903` entry was added separately. Distribution kernels can backport these entries, so check the driver on the actual computer. [Linux 7.3 driver](https://github.com/torvalds/linux/blob/v7.3/drivers/net/usb/cdc_ncm.c) · [MacBook Air 1903 support](https://github.com/torvalds/linux/commit/a8d0451967fc0a34c4b0c21368986a81b4e14862)

If Linux lists the USB device but the cable adapter is missing, `modinfo -F alias cdc_ncm` can show which Apple IDs the installed module matches. An alias includes both the vendor and product, such as `v05ACp1902`. If the adapter exists but is down, enable it in the Linux network settings. Once both computers have usable addresses on the cable, begin pairing. The manual address setup below also works for Mac ↔ Linux.

### Live connection diagnostics

The desktop app shows cable status in **Send** and **Receive**. Open **Settings → Connection diagnostics** for live checks, **Connection activity**, and **Copy report**. On Linux, the checks cover Mac and Thunderbolt detection, driver binding, the network adapter, carrier, and usable IP addresses.

For the same diagnostic in a terminal:

```sh
./target/release/usb-transfer doctor --watch --seconds 60 --report /tmp/usb-connection-report.txt
```

Omit `--seconds` to watch until Ctrl+C. `--report` saves the latest snapshot and up to 80 activity entries whenever the report changes. A peer address supplied with `--watch` is probed once at startup. On Linux, USB detection works even when network inspection fails; unavailable checks are marked **Unknown**. The report includes recent kernel USB/Thunderbolt history when accessible, omits USB serial-number lines, and distinguishes phones from the known Mac USB product IDs. A historical phone connection is evidence of that earlier USB data connection, rather than detection of the currently connected Mac. Changes shorter than the one-second sampling interval may only appear in kernel history. Idle controller suspension is reported as context and does not by itself establish the cause of a connection problem.

### Ubuntu 26.04 LTS with the generic 7.0 kernel

If your Mac enumerates as `05ac:1902`, `05ac:1903`, or `05ac:1905` but your running kernel has no matching `cdc_ncm` alias, the build helper can backport the upstream device entries to the matching Canonical module. This keeps your existing Ubuntu install and kernel. The helper builds locally as a regular user. Canonical's TPM-backed full-disk encryption installation uses a kernel snap that restricts out-of-tree modules. [Canonical module rebuild guide](https://ubuntu.com/kernel/docs/how-to/develop-customise/build-kernel-module/) · [Ubuntu 26.04 kernel snap limitations](https://documentation.ubuntu.com/release-notes/26.04/changes-since-previous-interim/)

For the standard Ubuntu generic kernel, install the build dependencies and fetch source for the installed module package. Run `apt source` with Ubuntu source repositories enabled. Use the extracted directory reported by `apt source` in the final command.

```bash
kernel=$(uname -r)
sudo apt update
sudo apt install build-essential dpkg-dev dkms kmod python3 "linux-headers-$kernel"
source_version=$(dpkg-query -W -f='${source:Version}' "linux-modules-$kernel")
source_work=$(mktemp -d)
(cd "$source_work" && apt source "linux=$source_version")
bash scripts/ubuntu-26.04-cdc-ncm.sh "$source_work/linux-7.0.0"
```

The helper checks that the Ubuntu source version and headers match the running kernel. Its output is under `target/ubuntu-26.04-cdc-ncm/$kernel` unless you supply a destination. The output includes the patched module source, a DKMS configuration locked to that kernel, and a build of `cdc_ncm.ko` with all six Mac control-interface aliases.

To install the backport, let Ubuntu DKMS build and sign it. If Ubuntu prompts you to enroll its local Machine Owner Key (MOK), complete enrollment at the next reboot with Secure Boot enabled. [Ubuntu DKMS manual](https://manpages.ubuntu.com/manpages/resolute/man8/dkms.8.html) · [Ubuntu Secure Boot module signing](https://documentation.ubuntu.com/security/security-features/platform-protections/secure-boot/)

```bash
module_dir="$PWD/target/ubuntu-26.04-cdc-ncm/$kernel"
sudo dkms install "$module_dir" -k "$kernel"
modinfo -k "$kernel" -F filename cdc_ncm
modinfo -k "$kernel" -F signer cdc_ncm
```

After rebooting, plug in the Mac and check `lsusb -t` and `usb-transfer doctor`. For rollback, use `dkms status -m usb-direct-transfer-cdc-ncm` to find the installed package version, then run `sudo dkms remove -m usb-direct-transfer-cdc-ncm -v VERSION -k "$(uname -r)"` and reboot. DKMS restores the stock module. Kernel updates have their own module directory. Check the installed driver again after an update, and rebuild against the new kernel's matching source if the Apple ID is still missing.

## Connect and transfer

1. Connect both computers directly using the cable. Run `usb-transfer doctor` or open **Settings → Connection diagnostics** in the app if the cable status needs attention.
2. Start `usb-transfer pair --to "/path/to/receive-folder"` on both machines, or open both apps and choose **Pair**. The CLI folder must already exist. In the app, choose the destination when reviewing an incoming batch.
3. Compare the pairing code on both screens. Approve on both computers only if the codes match. Pair again after unplugging or restarting.
4. On either side, choose the files or folders to send. In the CLI use `send PATH...` with double quotes around paths containing spaces. In the app, drop items into the window or use **Add items** on the **Send** screen to choose files and folders together.
5. Review and approve the batch on the receiving side. The result lists renamed and skipped items. Existing items are preserved.

For manual pairing, open **Settings → Manual pairing**, select the cable interface, and enter the other computer's cable IP on **both** sides. In the CLI, use `pair --interface NAME --peer ADDRESS --to DIR`. You can omit the suffix from an IPv6 link-local address because the app uses your local cable interface to scope it. `doctor --interface NAME --peer ADDRESS` can probe the link while the other computer is waiting to pair.

If the cable adapters are up but lack addresses on a shared subnet, assign distinct unused private IPv4 addresses to the cable adapters, for example `192.168.77.1` and `192.168.77.2` with subnet mask `255.255.255.0`. Leave the gateway blank on those adapters and use the assigned peer addresses for manual pairing. Allow Local Network access on the Mac if prompted.

Discovery uses link-local multicast on UDP 48394. Pairing and the diagnostics probe use TCP 48393. If discovery stalls on Linux, the app can try an outbound connection to a peer the system already knows on that cable. If a firewall exception is still needed, scope it to the app and the direct cable interface.

## Retry or discard

The transfer bar shows the bytes copied and estimates the time remaining once data starts moving. **Cancel transfer** stops the cable session and retains verified staged files on the destination drive, just as unplugging does. Pair again, send the same unchanged paths in the same order, and approve the same destination. The receiver verifies staged files before reusing them and restarts only files that were unfinished or no longer match. A received folder only appears in the destination after all of its files verify.

Open **Settings → Saved transfers** to review interrupted transfers for the selected save folder. Use **Retry last transfer** on the sending computer to resend the last batch from the open app. In the CLI, use `pending` and `discard ID` within a paired session, or run `usb-transfer pending --to DIR` and `usb-transfer discard ID --to DIR` separately. Discard removes the local staging data for that batch.

## Verification

```sh
cargo test --locked --all-features
cargo test --locked --release encrypted_loopback_transfers_a_file_larger_than_four_gib -- --ignored
```

The encrypted integration tests use localhost. The large-file test uses a 4 GiB sparse source and writes a full copy to the temporary directory.

On the Mac and Linux hardware, verify CLI transfers in both directions over the actual charging cable first. Include a batch of files and folders, conflicting and Unicode names, and a file larger than 4 GiB. Leave Wi-Fi on during an unplug test, confirm the session ends, and check that a retry after restarting the app reuses verified staged data. For desktop verification, repeat the workflow with both apps and pair the GUI with the CLI in both directions.

For a raw TCP baseline, run `usb-transfer bench --mib 512` on both peers with the same size, adding the same interface and peer options as for pairing if needed. Compare the directional MiB/s with a large encrypted, verified file copy on the same cable, using fast local storage. Use 80% of the raw baseline as a tuning reference.
