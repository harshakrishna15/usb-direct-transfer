# USB Direct Transfer

Send files and folders in either direction between two computers over a direct USB-C network link. The CLI and desktop app use the same encrypted protocol and can pair with each other.

The first hardware target is an Apple silicon Mac and a Windows 11 x64 PC connected by an ordinary Apple USB-C charging cable. The cable and OS drivers must expose a working network interface on each computer. The app pins connections to recognized direct USB or Thunderbolt/USB4 interfaces and ends the session if the selected link disappears.

## Build

Use a current stable Rust toolchain on macOS or Windows.

```sh
cargo build --locked --release --bin usb-transfer
cargo build --locked --release --features gui --bin usb-transfer-gui
```

On macOS, run `./scripts/package-macos.sh` to create the desktop app bundle in `target/release`. On Windows, the two executables are in `target\release`. The app runs as a regular user. A separately qualified driver installation, if needed, is an OS setup step.

## Connect and transfer

1. Connect both computers directly using the cable. Run `usb-transfer doctor` on each machine or open **Connection diagnostics** in the app. Check for a ready cable interface and a usable address.
2. Start `usb-transfer pair --to "/path/to/receive-folder"` on both machines, or open both apps and choose **Pair**. The CLI folder must already exist. In the app, choose the destination when reviewing an incoming batch.
3. Compare the pairing code on both screens. Approve on both computers only if the codes match. Pair again after unplugging or restarting.
4. On either side, choose the files or folders to send. In the CLI use `send PATH...` with double quotes around paths containing spaces. In the app, drop items into the window or select them with the file and folder pickers.
5. Review and approve the batch on the receiving side. The result lists renamed and skipped items. Existing items are preserved.

For manual pairing, select the cable interface and enter the other computer's cable IP on **both** sides. In the CLI, use `pair --interface NAME --peer ADDRESS --to DIR`. You can omit the suffix from an IPv6 link-local address because the app uses your local cable interface to scope it. `doctor --interface NAME --peer ADDRESS` can probe the link while the other computer is waiting to pair.

If the cable adapters are up but lack addresses on a shared subnet, assign distinct unused private IPv4 addresses to the cable adapters, for example `192.168.77.1` and `192.168.77.2` with subnet mask `255.255.255.0`. Leave the gateway blank on those adapters and use the assigned peer addresses for manual pairing. Allow Local Network access on the Mac if prompted.

Discovery uses link-local multicast on UDP 48394. Pairing and the diagnostics probe use TCP 48393. If the firewall blocks discovery or pairing, scope any exception to the app and the direct cable interface.

## Retry or discard

Unplugging retains verified staged files on the destination drive. Pair again, send the same unchanged paths in the same order, and approve the same destination. The receiver verifies staged files before reusing them and restarts only files that were unfinished or no longer match. A received folder only appears in the destination after all of its files verify.

The desktop app shows pending transfers for the selected destination and can resend the last batch from the open app. In the CLI, use `pending` and `discard ID` within a paired session, or run `usb-transfer pending --to DIR` and `usb-transfer discard ID --to DIR` separately. Discard removes the local staging data for that batch.

## Verification

```sh
cargo test --locked --all-features
cargo test --locked --release encrypted_loopback_transfers_a_file_larger_than_four_gib -- --ignored
```

The encrypted integration tests use localhost. The large-file test uses a 4 GiB sparse source and writes a full copy to the temporary directory.

On the Mac and Windows hardware, verify transfers in both directions over the actual charging cable. Include a batch of files and folders, conflicting and Unicode names, and a file larger than 4 GiB. Leave Wi-Fi on during an unplug test, confirm the session ends, and check that a retry after restarting the app reuses verified staged data. For desktop verification, also pair the GUI with the CLI and test sending and receiving in both directions.

For a raw TCP baseline, run `usb-transfer bench --mib 512` on both peers with the same size, adding the same interface and peer options as for pairing if needed. Compare the directional MiB/s with a large encrypted, verified file copy on the same cable, using fast local storage. Use 80% of the raw baseline as a tuning reference.
