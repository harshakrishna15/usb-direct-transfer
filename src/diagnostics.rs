use crate::Interface;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckState {
    Ready,
    Waiting,
    Blocked,
    Unknown,
}

impl CheckState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Ready => "OK",
            Self::Waiting => "Waiting",
            Self::Blocked => "Needs attention",
            Self::Unknown => "Unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionCheck {
    pub name: &'static str,
    pub state: CheckState,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionReport {
    pub summary: String,
    pub checks: Vec<ConnectionCheck>,
    pub next_steps: Vec<String>,
    pub interfaces: Vec<Interface>,
    pub details: Vec<String>,
    pub activity: Vec<String>,
    observations: BTreeMap<String, String>,
    inspected: BTreeSet<&'static str>,
}

impl ConnectionReport {
    pub fn text(&self) -> String {
        let mut lines = vec![
            "USB Direct Transfer — connection report".into(),
            self.summary.clone(),
        ];
        for check in &self.checks {
            lines.push(format!(
                "[{}] {}: {}",
                check.state.label(),
                check.name,
                check.detail
            ));
        }
        lines.push("\nNext steps:".into());
        lines.extend(self.next_steps.iter().map(|step| format!("- {step}")));
        if !self.activity.is_empty() {
            lines.push("\nConnection activity (elapsed since monitoring started):".into());
            lines.extend(self.activity.iter().cloned());
        }
        if !self.details.is_empty() {
            lines.push("\nDetails:".into());
            lines.extend(self.details.iter().cloned());
        }
        lines.join("\n")
    }
}

pub struct ConnectionMonitor {
    started: Instant,
    previous: Option<ConnectionReport>,
    activity: VecDeque<String>,
    #[cfg(target_os = "linux")]
    cache: linux::Cache,
}

impl Default for ConnectionMonitor {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            previous: None,
            activity: VecDeque::new(),
            #[cfg(target_os = "linux")]
            cache: linux::Cache::default(),
        }
    }
}

impl ConnectionMonitor {
    /// Samples hardware and network state. This never changes system settings or pairs.
    pub fn poll(&mut self) -> ConnectionReport {
        #[cfg(target_os = "linux")]
        let report = linux::collect(&mut self.cache);
        #[cfg(not(target_os = "linux"))]
        let report = network_report(crate::network::diagnostic_interfaces());
        self.record(report)
    }

    fn record(&mut self, mut report: ConnectionReport) -> ConnectionReport {
        let mut changes = Vec::new();
        if let Some(previous) = &self.previous {
            for (key, detail) in &report.observations {
                match previous.observations.get(key) {
                    None => changes.push(format!("Appeared: {detail}")),
                    Some(old) if old != detail => changes.push(format!("Changed: {detail}")),
                    _ => {}
                }
            }
            for (key, detail) in &previous.observations {
                if !report.observations.contains_key(key) {
                    if report
                        .inspected
                        .contains(key.split('/').next().unwrap_or_default())
                    {
                        changes.push(format!("Disappeared: {detail}"));
                    } else {
                        // Keep the comparison baseline through an unavailable check.
                        // A failed inspection is not a physical disconnection.
                        report.observations.insert(key.clone(), detail.clone());
                    }
                }
            }
            if previous.summary != report.summary {
                changes.push(report.summary.clone());
            }
        } else {
            changes.push(format!("Monitoring started. {}", report.summary));
        }
        for change in changes {
            self.activity
                .push_back(format!("+{}s {change}", self.started.elapsed().as_secs()));
        }
        while self.activity.len() > 80 {
            self.activity.pop_front();
        }
        report.activity = self.activity.iter().cloned().collect();
        self.previous = Some(report.clone());
        report
    }
}

fn check(name: &'static str, state: CheckState, detail: impl Into<String>) -> ConnectionCheck {
    ConnectionCheck {
        name,
        state,
        detail: detail.into(),
    }
}

fn network_report(result: anyhow::Result<(Vec<Interface>, Vec<String>)>) -> ConnectionReport {
    let mut report = ConnectionReport {
        summary: String::new(),
        checks: Vec::new(),
        next_steps: Vec::new(),
        interfaces: Vec::new(),
        details: Vec::new(),
        activity: Vec::new(),
        observations: BTreeMap::new(),
        inspected: BTreeSet::new(),
    };
    match result {
        Ok((interfaces, notes)) => {
            report.inspected.insert("net");
            for interface in &interfaces {
                report.observations.insert(
                    format!("net/{}", interface.name),
                    format!(
                        "{}: {} ({})",
                        interface.name, interface.status, interface.address
                    ),
                );
            }
            let has_adapter = !interfaces.is_empty();
            let up = interfaces.iter().any(|interface| interface.up);
            let ready = interfaces.iter().any(|interface| interface.ready);
            report.checks.push(check(
                "Network adapter",
                if has_adapter {
                    CheckState::Ready
                } else {
                    CheckState::Waiting
                },
                if has_adapter {
                    "Direct cable network adapter detected."
                } else {
                    "No recognized direct cable network adapter."
                },
            ));
            report.checks.push(check(
                "Link",
                if up {
                    CheckState::Ready
                } else if has_adapter {
                    CheckState::Blocked
                } else {
                    CheckState::Waiting
                },
                if up {
                    "A cable adapter has carrier and is enabled."
                } else if has_adapter {
                    "Cable adapters are down or have no carrier."
                } else {
                    "Waiting for a cable network adapter."
                },
            ));
            report.checks.push(check(
                "IP address",
                if ready {
                    CheckState::Ready
                } else if up {
                    CheckState::Blocked
                } else {
                    CheckState::Waiting
                },
                if ready {
                    "A connected adapter has a usable IP address."
                } else if up {
                    "The cable link is up but has no usable IP address."
                } else {
                    "Waiting for the cable link."
                },
            ));
            if ready {
                report.summary = "Cable interface ready for pairing.".into();
                report.next_steps.push("Open the app on both computers, choose Pair, and compare the pairing codes. Readiness does not confirm peer reachability.".into());
            } else if up {
                report.summary = "Cable connected; an IP address is needed.".into();
                report.next_steps.push("Configure addresses on the cable adapters on both computers, for example 192.168.77.1/24 and 192.168.77.2/24, with no gateway. Then enter the other computer's cable address.".into());
            } else if has_adapter {
                report.summary = "Cable adapter found; waiting for the link.".into();
                report.next_steps.push("Enable the cable adapter in network settings on both computers and keep the Mac awake and unlocked.".into());
            } else {
                report.summary = "Waiting for a direct cable network adapter.".into();
                report.next_steps.push("Reconnect the cable directly between the computers while the Mac is unlocked, and approve any accessory prompt.".into());
            }
            for interface in &interfaces {
                report.details.push(format!(
                    "{}: {} ({})",
                    interface.name,
                    interface.status,
                    if interface.address.is_empty() {
                        "no address"
                    } else {
                        &interface.address
                    }
                ));
            }
            report.details.extend(notes);
            report.interfaces = interfaces;
        }
        Err(error) => {
            report.summary = "Network inspection is unavailable.".into();
            let detail = format!("{error:#}");
            for name in ["Network adapter", "Link", "IP address"] {
                report.checks.push(check(
                    name,
                    CheckState::Unknown,
                    "Network inspection failed; adapter state is unknown.",
                ));
            }
            report.details.push(detail);
            report.next_steps.push("Run the app in your normal desktop session. On Linux, ensure iproute2 is installed and network inspection is permitted.".into());
        }
    }
    report
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::{fs, io, path::Path, process::Command, time::Duration};

    const MAC_IDS: [&str; 3] = ["1902", "1903", "1905"];

    #[derive(Debug, Clone)]
    struct UsbDevice {
        port: String,
        vendor: String,
        product: String,
        name: String,
        speed: String,
        number: String,
        controller: String,
        drivers: Vec<String>,
    }

    impl UsbDevice {
        fn is_mac(&self) -> bool {
            self.vendor == "05ac" && MAC_IDS.contains(&self.product.as_str())
        }

        fn description(&self) -> String {
            let kind = if self.is_mac() {
                "Mac"
            } else if self.vendor == "05ac" {
                "Other Apple device"
            } else {
                "USB device"
            };
            format!(
                "{kind} {}:{} {} on {} · {} Mbit/s · controller {} · device {} · drivers {}",
                self.vendor,
                self.product,
                self.name,
                self.port,
                self.speed,
                self.controller,
                self.number,
                if self.drivers.is_empty() {
                    "none".into()
                } else {
                    self.drivers.join(", ")
                }
            )
        }
    }

    pub(super) struct Cache {
        refreshed: Option<Instant>,
        aliases: Result<String, String>,
        logs: Vec<String>,
    }

    impl Default for Cache {
        fn default() -> Self {
            Self {
                refreshed: None,
                aliases: Err("Not checked yet".into()),
                logs: Vec::new(),
            }
        }
    }

    fn run(program: &str, arguments: &[&str]) -> Result<String, String> {
        let output = Command::new(program)
            .args(arguments)
            .env("LC_ALL", "C")
            .output()
            .map_err(|error| format!("{program}: {error}"))?;
        // journalctl --grep returns 1 when no entries match. That is a valid
        // empty history, not an inspection or permission failure.
        if program == "journalctl"
            && output.status.code() == Some(1)
            && String::from_utf8_lossy(&output.stderr).trim().is_empty()
            && matches!(
                String::from_utf8_lossy(&output.stdout).trim(),
                "" | "-- No entries --"
            )
        {
            return Ok("No recent matching USB or Thunderbolt kernel events.\n".into());
        }
        if !output.status.success() {
            return Err(format!(
                "{program}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn read(path: impl AsRef<Path>) -> String {
        fs::read_to_string(path)
            .map(|value| {
                value
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .chars()
                    .take(180)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn usb_devices(root: &Path) -> io::Result<Vec<UsbDevice>> {
        let mut devices = Vec::new();
        for entry in fs::read_dir(root)?.flatten() {
            let path = entry.path();
            let vendor = read(path.join("idVendor")).to_ascii_lowercase();
            let product = read(path.join("idProduct")).to_ascii_lowercase();
            if vendor.is_empty()
                || product.is_empty()
                || entry.file_name().to_string_lossy().starts_with("usb")
            {
                continue;
            }
            let mut drivers: Vec<_> = fs::read_dir(&path)
                .into_iter()
                .flatten()
                .flatten()
                .filter_map(|entry| fs::read_link(entry.path().join("driver")).ok())
                .filter_map(|driver| {
                    driver
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                })
                .collect();
            drivers.sort();
            drivers.dedup();
            let controller = fs::canonicalize(&path)
                .ok()
                .and_then(|path| {
                    path.ancestors().find_map(|parent| {
                        let name = parent.file_name()?.to_str()?;
                        (name.len() == 12
                            && name.as_bytes()[4] == b':'
                            && name.as_bytes()[7] == b':'
                            && name.as_bytes()[10] == b'.')
                            .then(|| name.to_owned())
                    })
                })
                .unwrap_or_else(|| "unknown".into());
            devices.push(UsbDevice {
                port: entry.file_name().to_string_lossy().into_owned(),
                vendor,
                product,
                name: read(path.join("product")),
                speed: read(path.join("speed")),
                number: read(path.join("devnum")),
                controller,
                drivers,
            });
        }
        devices.sort_by(|a, b| a.port.cmp(&b.port));
        Ok(devices)
    }

    fn aliases_match(aliases: &str, product: &str) -> bool {
        let prefix = format!("USB:V05ACP{}D", product.to_ascii_uppercase());
        aliases
            .lines()
            .any(|alias| alias.trim().to_ascii_uppercase().starts_with(&prefix))
    }

    fn recent_logs(output: &str) -> Vec<String> {
        output
            .lines()
            .filter(|line| !line.contains("SerialNumber") && !line.contains("serial="))
            .map(|line| {
                line.chars()
                    .filter(|ch| !ch.is_control())
                    .take(500)
                    .collect()
            })
            .collect()
    }

    pub(super) fn collect(cache: &mut Cache) -> ConnectionReport {
        if cache
            .refreshed
            .is_none_or(|last| last.elapsed() >= Duration::from_secs(5))
        {
            let modinfo = ["/usr/sbin/modinfo", "/sbin/modinfo"]
                .into_iter()
                .find(|path| Path::new(path).is_file())
                .unwrap_or("modinfo");
            cache.aliases = run(modinfo, &["-F", "alias", "cdc_ncm"]);
            cache.logs = match run(
                "journalctl",
                &[
                    "-k",
                    "-b",
                    "--since=-15min",
                    "--no-pager",
                    "-o",
                    "short-monotonic",
                    "-n",
                    "30",
                    "--grep=usb.*(new .*USB|New USB device found|Product:|disconnect|error|unable|failed)|cdc_ncm|thunderbolt",
                ],
            ) {
                Ok(logs) => recent_logs(&logs),
                Err(error) => vec![format!("Kernel event history unavailable: {error}")],
            };
            cache.refreshed = Some(Instant::now());
        }
        let usb = usb_devices(Path::new("/sys/bus/usb/devices")).map_err(|error| error.to_string());
        let peers_result = fs::read_dir("/sys/bus/thunderbolt/devices");
        let peers_error = peers_result
            .as_ref()
            .err()
            .filter(|error| error.kind() != io::ErrorKind::NotFound)
            .map(ToString::to_string);
        let peers: Vec<_> = peers_result
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                let (_, suffix) = name.split_once('-')?;
                if suffix == "0" || name.contains(':') {
                    return None;
                }
                Some((
                    name,
                    format!(
                        "Thunderbolt peer {} {}",
                        read(entry.path().join("vendor_name")),
                        read(entry.path().join("device_name"))
                    ),
                ))
            })
            .collect();
        let mut report = diagnose(
            usb,
            &peers,
            &cache.aliases,
            crate::network::diagnostic_interfaces(),
        );
        if let Some(error) = peers_error {
            report
                .details
                .push(format!("Thunderbolt inspection unavailable: {error}"));
            if report
                .checks
                .first()
                .is_some_and(|check| check.state == CheckState::Waiting)
            {
                report.checks[0].state = CheckState::Unknown;
                report.checks[0].detail =
                    "No Mac USB device detected; Thunderbolt inspection is unavailable.".into();
                report.summary = "Hardware inspection is incomplete.".into();
            }
        } else {
            report.inspected.insert("thunderbolt");
        }
        report.details.insert(
            0,
            format!("Linux kernel: {}", read("/proc/sys/kernel/osrelease")),
        );
        if let Ok(entries) = fs::read_dir("/sys/bus/pci/devices") {
            for entry in entries.flatten() {
                let driver = fs::read_link(entry.path().join("driver")).unwrap_or_default();
                if matches!(
                    driver.file_name().and_then(|name| name.to_str()),
                    Some("xhci_hcd" | "thunderbolt")
                ) {
                    report.details.push(format!(
                        "Controller {} ({}): {} / {}",
                        entry.file_name().to_string_lossy(),
                        driver.file_name().unwrap().to_string_lossy(),
                        read(entry.path().join("power_state")),
                        read(entry.path().join("power/runtime_status"))
                    ));
                }
            }
            report.details.push("An idle controller may suspend normally. Its power state alone does not identify a connection fault.".into());
        }
        if !cache.logs.is_empty() {
            if report
                .checks
                .first()
                .is_some_and(|check| check.state == CheckState::Waiting)
                && cache
                    .logs
                    .iter()
                    .any(|line| line.contains("Product: iPhone"))
                && cache
                    .logs
                    .iter()
                    .any(|line| line.contains("idVendor=05ac, idProduct=12a8"))
            {
                report.next_steps.insert(0, "Recent kernel history shows an iPhone USB connection. If that was the same-cable/port test, USB data worked then; focus on getting the Mac to enumerate.".into());
            }
            report.details.push("Recent kernel USB/Thunderbolt history (may include other devices and earlier connections):".into());
            report.details.extend(cache.logs.clone());
        }
        report
    }

    fn diagnose(
        usb: Result<Vec<UsbDevice>, String>,
        peers: &[(String, String)],
        aliases: &Result<String, String>,
        network: anyhow::Result<(Vec<Interface>, Vec<String>)>,
    ) -> ConnectionReport {
        let mut report = network_report(network);
        for (name, detail) in peers {
            report
                .observations
                .insert(format!("thunderbolt/{name}"), detail.clone());
            report.details.push(detail.clone());
        }
        let devices = match usb {
            Ok(devices) => {
                report.inspected.insert("usb");
                devices
            }
            Err(error) => {
                let other_transport = !peers.is_empty() || !report.interfaces.is_empty();
                report.checks.insert(
                    0,
                    check(
                        "Mac / Thunderbolt detection",
                        if other_transport {
                            CheckState::Ready
                        } else {
                            CheckState::Unknown
                        },
                        format!(
                            "{}USB inspection unavailable: {error}",
                            if other_transport {
                                "A direct cable transport is visible. "
                            } else {
                                ""
                            }
                        ),
                    ),
                );
                report
                    .details
                    .push(format!("USB device inspection unavailable: {error}"));
                if !other_transport {
                    report.summary = "Hardware inspection is incomplete.".into();
                    report.next_steps.insert(0, "Run the diagnostic in a normal desktop session with access to USB device information.".into());
                } else if report.interfaces.is_empty() {
                    report.summary =
                        "Thunderbolt peer detected; USB inspection is unavailable.".into();
                }
                return report;
            }
        };
        for device in &devices {
            report
                .observations
                .insert(format!("usb/{}", device.port), device.description());
        }
        let macs: Vec<_> = devices.iter().filter(|device| device.is_mac()).collect();
        let transport = !macs.is_empty() || !peers.is_empty() || !report.interfaces.is_empty();
        let hardware = if !macs.is_empty() {
            macs.iter()
                .map(|device| {
                    format!(
                        "Mac 05ac:{} on {} at {} Mbit/s",
                        device.product, device.port, device.speed
                    )
                })
                .collect::<Vec<_>>()
                .join("; ")
        } else if !peers.is_empty() {
            "A remote Thunderbolt/USB4 peer is present; confirm it is the intended computer.".into()
        } else if !report.interfaces.is_empty() {
            "A recognized direct cable network adapter is present.".into()
        } else {
            "No known Mac USB device or remote Thunderbolt/USB4 peer is present. Charging does not confirm data detection.".into()
        };
        report.checks.insert(
            0,
            check(
                "Mac / Thunderbolt detection",
                if transport {
                    CheckState::Ready
                } else {
                    CheckState::Waiting
                },
                hardware,
            ),
        );
        for device in devices.iter().filter(|device| device.vendor == "05ac") {
            report.details.push(device.description());
        }
        let missing: Vec<_> = aliases
            .as_ref()
            .map(|aliases| {
                MAC_IDS
                    .into_iter()
                    .filter(|id| !aliases_match(aliases, id))
                    .collect()
            })
            .unwrap_or_default();
        match aliases {
            Ok(_) if !missing.is_empty() => report.details.push(format!("Installed cdc_ncm has no explicit Mac match for: {}. This check concerns driver binding after USB detection.", missing.iter().map(|id| format!("05ac:{id}")).collect::<Vec<_>>().join(", "))),
            Ok(_) => report.details.push("Installed cdc_ncm has explicit entries for the known Mac product IDs. A matching entry alone does not prove successful binding.".into()),
            Err(error) => report.details.push(format!("Mac driver support could not be checked: {error}")),
        }
        let bound = macs
            .iter()
            .any(|mac| mac.drivers.iter().any(|driver| driver == "cdc_ncm"));
        let driver_check = if bound
            || report
                .interfaces
                .iter()
                .any(|interface| interface.status.starts_with("USB ·"))
        {
            check(
                "USB driver support",
                CheckState::Ready,
                "The Mac's cdc_ncm driver is bound.",
            )
        } else if !macs.is_empty() {
            match aliases {
                Ok(_)
                    if macs
                        .iter()
                        .any(|mac| missing.contains(&mac.product.as_str())) =>
                {
                    check(
                        "USB driver support",
                        CheckState::Blocked,
                        "The installed cdc_ncm driver lacks an explicit match for the detected Mac.",
                    )
                }
                Ok(_) => check(
                    "USB driver support",
                    CheckState::Waiting,
                    "An explicit driver entry exists; waiting for successful binding.",
                ),
                Err(_) => check(
                    "USB driver support",
                    CheckState::Unknown,
                    "Could not inspect installed Mac driver support.",
                ),
            }
        } else if !peers.is_empty() || !report.interfaces.is_empty() {
            check(
                "USB driver support",
                CheckState::Ready,
                "The Thunderbolt/USB4 path uses a separate network driver.",
            )
        } else {
            check(
                "USB driver support",
                CheckState::Waiting,
                if missing.is_empty() {
                    "Waiting for Mac USB detection; see driver preflight in the details."
                } else {
                    "Waiting for Mac USB detection. Preflight also found missing Mac driver entries; these affect networking after detection."
                },
            )
        };
        report.checks.insert(1, driver_check);
        if !transport {
            report.summary = "Waiting for the Mac to appear over USB or Thunderbolt.".into();
            let inspection_steps =
                if report.checks.iter().any(|check| {
                    check.name == "Network adapter" && check.state == CheckState::Unknown
                }) {
                    report.next_steps.clone()
                } else {
                    Vec::new()
                };
            report.next_steps = vec!["Keep the Mac awake and unlocked. Reconnect directly and approve its accessory connection.".into(), "Use this live view while reconnecting. A phone or another Apple USB device appearing is useful cable/port evidence, but does not count as detecting the Mac.".into(), "If a phone is detected on the same cable and port but the Mac stays absent, try the Mac's other USB-C port and inspect its accessory settings. A different known data cable can help isolate USB role negotiation.".into()];
            report.next_steps.extend(inspection_steps);
            if devices.iter().any(|device| device.vendor == "05ac") {
                report.next_steps.insert(
                    0,
                    "Another Apple USB device is visible. Reconnect the Mac to continue this test."
                        .into(),
                );
            }
        } else if report.interfaces.is_empty() {
            let network_unknown = report
                .checks
                .iter()
                .any(|check| check.name == "Network adapter" && check.state == CheckState::Unknown);
            if !network_unknown {
                report.summary = "Computer detected; a cable network driver is needed.".into();
                if let Some(adapter) = report
                    .checks
                    .iter_mut()
                    .find(|check| check.name == "Network adapter")
                {
                    adapter.state = CheckState::Blocked;
                    adapter.detail = "The computer is detected, but no supported cable network adapter appeared.".into();
                }
                report.next_steps.clear();
                if let Some(mac) = macs
                    .iter()
                    .find(|mac| missing.contains(&mac.product.as_str()))
                {
                    report.next_steps.push(format!("The installed cdc_ncm driver lacks a match for this Mac (05ac:{}). Use a distribution kernel containing the Mac fix, or the documented Ubuntu module backport for the running kernel.", mac.product));
                } else if !macs.is_empty() {
                    report.next_steps.push("Inspect the kernel history for cdc_ncm binding errors. If driver support is unknown, check modinfo -F alias cdc_ncm; an explicit Mac entry must use Apple's private-interface handling.".into());
                } else {
                    report.next_steps.push("Enable Thunderbolt Bridge on the Mac and check that Linux's thunderbolt_net module is available. Watch for a direct cable network adapter to appear.".into());
                }
            }
        }
        report
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn usb(product: &str) -> UsbDevice {
            UsbDevice {
                port: "3-7".into(),
                vendor: "05ac".into(),
                product: product.into(),
                name: "Apple device".into(),
                speed: "480".into(),
                number: "18".into(),
                controller: "0000:80:14.0".into(),
                drivers: vec![],
            }
        }

        #[test]
        fn an_iphone_does_not_count_as_a_mac_or_prove_driver_failure() {
            let report = diagnose(
                Ok(vec![usb("12a8")]),
                &[],
                &Ok(String::new()),
                Ok((vec![], vec![])),
            );
            assert_eq!(report.checks[0].state, CheckState::Waiting);
            assert!(report.summary.starts_with("Waiting for the Mac"));
            assert!(
                report
                    .details
                    .iter()
                    .any(|line| line.contains("Other Apple device 05ac:12a8"))
            );
            assert!(
                !report
                    .next_steps
                    .iter()
                    .any(|line| line.contains("backport"))
            );
        }

        #[test]
        fn an_enumerated_mac_with_a_missing_alias_gets_the_driver_fix() {
            let report = diagnose(
                Ok(vec![usb("1902")]),
                &[],
                &Ok("usb:v05ACp12A8d*".into()),
                Ok((vec![], vec![])),
            );
            assert_eq!(report.checks[0].state, CheckState::Ready);
            assert!(report.next_steps[0].contains("05ac:1902"));
            assert!(report.next_steps[0].contains("backport"));
        }

        #[test]
        fn failed_network_inspection_preserves_usb_evidence_and_unknown_state() {
            let report = diagnose(
                Ok(vec![usb("1902")]),
                &[],
                &Err("modinfo unavailable".into()),
                Err(anyhow::anyhow!("netlink permission denied")),
            );
            assert_eq!(report.checks[0].state, CheckState::Ready);
            assert_eq!(
                report
                    .checks
                    .iter()
                    .find(|check| check.name == "Network adapter")
                    .unwrap()
                    .state,
                CheckState::Unknown
            );
            assert!(report.text().contains("netlink permission denied"));
            assert!(
                !report
                    .next_steps
                    .iter()
                    .any(|line| line.contains("backport"))
            );
        }

        #[test]
        fn failed_usb_inspection_keeps_visible_thunderbolt_evidence() {
            let report = diagnose(
                Err("permission denied".into()),
                &[("0-1".into(), "Thunderbolt peer".into())],
                &Err("unavailable".into()),
                Ok((vec![], vec![])),
            );
            assert_eq!(report.checks[0].state, CheckState::Ready);
            assert!(report.observations.contains_key("thunderbolt/0-1"));
            assert!(!report.inspected.contains("usb"));
            let unknown = diagnose(
                Err("permission denied".into()),
                &[],
                &Err("unavailable".into()),
                Ok((vec![], vec![])),
            );
            assert_eq!(unknown.checks[0].state, CheckState::Unknown);
            assert_eq!(unknown.summary, "Hardware inspection is incomplete.");
        }

        #[test]
        fn sysfs_scan_reads_mac_binding_and_the_actual_parent_controller() {
            use std::os::unix::fs::symlink;
            let fixture = tempfile::tempdir().unwrap();
            let device = fixture.path().join("0000:80:14.0/usb3/3-7");
            fs::create_dir_all(device.join("3-7:1.0")).unwrap();
            for (key, value) in [
                ("idVendor", "05ac"),
                ("idProduct", "1902"),
                ("product", "MacBook Pro"),
                ("speed", "480"),
                ("devnum", "7"),
            ] {
                fs::write(device.join(key), value).unwrap();
            }
            symlink(
                "/sys/bus/usb/drivers/cdc_ncm",
                device.join("3-7:1.0/driver"),
            )
            .unwrap();
            let root = fixture.path().join("devices");
            fs::create_dir(&root).unwrap();
            symlink(&device, root.join("3-7")).unwrap();
            let found = usb_devices(&root).unwrap();
            assert_eq!(found.len(), 1);
            assert!(found[0].is_mac());
            assert_eq!(found[0].controller, "0000:80:14.0");
            assert_eq!(found[0].drivers, ["cdc_ncm"]);
        }

        #[test]
        fn aliases_require_the_actual_vendor_and_product_and_logs_omit_serials() {
            let aliases = "usb:v05ACp1902d*dc*dsc*dp*ic*isc*ip*in00*\nusb:v1234p1903d*";
            assert!(aliases_match(aliases, "1902"));
            assert!(!aliases_match(aliases, "1903"));
            assert!(!aliases_match(aliases, "1905"));
            assert_eq!(
                recent_logs("Product: iPhone\nSerialNumber: secret\nusb: disconnected"),
                ["Product: iPhone", "usb: disconnected"]
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter(up: bool, ready: bool) -> Interface {
        Interface {
            name: "enx-mac".into(),
            address: if ready {
                "fe80::1".into()
            } else {
                String::new()
            },
            status: "USB · cdc_ncm".into(),
            up,
            ready,
        }
    }

    #[test]
    fn unavailable_inspection_does_not_invent_disconnects_and_reconnects() {
        let mut monitor = ConnectionMonitor::default();
        monitor.record(network_report(Ok((vec![adapter(true, true)], vec![]))));
        let unknown = monitor.record(network_report(Err(anyhow::anyhow!(
            "inspection unavailable"
        ))));
        assert!(
            !unknown
                .activity
                .iter()
                .any(|event| event.contains("Disappeared:"))
        );
        let recovered = monitor.record(network_report(Ok((vec![adapter(true, true)], vec![]))));
        assert!(
            !recovered
                .activity
                .iter()
                .any(|event| event.contains("Appeared:"))
        );
        let unplugged = monitor.record(network_report(Ok((vec![], vec![]))));
        assert!(
            unplugged
                .activity
                .iter()
                .any(|event| event.contains("Disappeared: enx-mac"))
        );
    }

    #[test]
    fn distinguishes_a_missing_address_from_a_ready_interface() {
        let needs_address = network_report(Ok((vec![adapter(true, false)], vec![])));
        assert_eq!(needs_address.checks[1].state, CheckState::Ready);
        assert_eq!(needs_address.checks[2].state, CheckState::Blocked);
        let ready = network_report(Ok((vec![adapter(true, true)], vec![])));
        assert!(
            ready
                .checks
                .iter()
                .all(|check| check.state == CheckState::Ready)
        );
        assert!(ready.next_steps[0].contains("does not confirm peer reachability"));
    }

    #[test]
    fn activity_is_bounded_during_repeated_link_changes() {
        let mut monitor = ConnectionMonitor::default();
        for index in 0..100 {
            monitor.record(network_report(Ok((
                vec![adapter(true, index % 2 == 0)],
                vec![],
            ))));
        }
        assert_eq!(monitor.activity.len(), 80);
    }
}
