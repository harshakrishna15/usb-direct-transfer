use anyhow::{Context, Result, bail, ensure};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use std::{
    io::{self, Read, Write},
    net::{
        IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, SocketAddrV6, TcpListener, TcpStream,
        UdpSocket,
    },
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

pub const SESSION_PORT: u16 = 48393;
const DISCOVERY_PORT: u16 = 48394;
const DISCOVERY_V4: Ipv4Addr = Ipv4Addr::new(239, 255, 77, 77);
const DISCOVERY_V6: Ipv6Addr = Ipv6Addr::new(0xff12, 0, 0, 0, 0, 0, 0x8ce0, 0xe1bd);
const DISCOVERY: &[u8; 8] = b"UDT1HERE";
const AUTOMATIC: &[u8; 8] = b"UDT1AUTO";
const MANUAL: &[u8; 8] = b"UDT1PAIR";
const BENCH_DISCOVERY: &[u8; 8] = b"UDB1HERE";
const BENCH_AUTOMATIC: &[u8; 8] = b"UDB1AUTO";
const BENCH_MANUAL: &[u8; 8] = b"UDB1PAIR";
const PROBE: &[u8; 8] = b"UDT1PING";
const ACCEPTED: &[u8; 8] = b"UDT1OKAY";
const CONNECT_WAIT: Duration = Duration::from_millis(450);
const PREFACE_WAIT: Duration = Duration::from_secs(2);
type NodeId = [u8; 16];

#[derive(Clone, Copy)]
enum Mode {
    Pair,
    Benchmark,
}

impl Mode {
    fn discovery(self) -> &'static [u8; 8] {
        match self {
            Self::Pair => DISCOVERY,
            Self::Benchmark => BENCH_DISCOVERY,
        }
    }

    fn automatic(self) -> &'static [u8; 8] {
        match self {
            Self::Pair => AUTOMATIC,
            Self::Benchmark => BENCH_AUTOMATIC,
        }
    }

    fn manual(self) -> &'static [u8; 8] {
        match self {
            Self::Pair => MANUAL,
            Self::Benchmark => BENCH_MANUAL,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Options {
    pub interface: Option<String>,
    pub peer: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interface {
    pub name: String,
    pub address: String,
    pub status: String,
    pub up: bool,
    pub ready: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Address {
    ip: IpAddr,
    prefix: u8,
    index: u32,
}

impl Address {
    fn socket(self, port: u16) -> SocketAddr {
        self.peer_socket(self.ip, port)
    }

    fn peer_socket(self, ip: IpAddr, port: u16) -> SocketAddr {
        match ip {
            IpAddr::V6(ip) if ip.is_unicast_link_local() => {
                SocketAddrV6::new(ip, port, 0, self.index).into()
            }
            _ => SocketAddr::new(ip, port),
        }
    }

    fn directly_reaches(self, peer: IpAddr) -> bool {
        if !usable(peer) || self.ip == peer {
            return false;
        }
        match (self.ip, peer) {
            (IpAddr::V4(local), IpAddr::V4(peer)) if (1..=31).contains(&self.prefix) => {
                let mask = u32::MAX << (32 - self.prefix);
                let host = u32::from(peer) & !mask;
                (self.prefix == 31 || (host != 0 && host != !mask))
                    && u32::from(local) & mask == u32::from(peer) & mask
            }
            (IpAddr::V6(local), IpAddr::V6(peer)) if (1..=127).contains(&self.prefix) => {
                let mask = u128::MAX << (128 - self.prefix);
                local.is_unicast_link_local() == peer.is_unicast_link_local()
                    && u128::from(local) & mask == u128::from(peer) & mask
            }
            _ => false,
        }
    }
}

fn usable(ip: IpAddr) -> bool {
    !ip.is_unspecified()
        && !ip.is_loopback()
        && !ip.is_multicast()
        && match ip {
            IpAddr::V4(ip) => ip.octets()[0] != 0 && !ip.is_broadcast(),
            IpAddr::V6(ip) => ip.to_ipv4_mapped().is_none(),
        }
}

#[derive(Debug)]
struct Cable {
    name: String,
    kind: &'static str,
    driver: String,
    up: bool,
    addresses: Vec<Address>,
}

impl Cable {
    fn view(&self) -> Interface {
        let address = self
            .addresses
            .iter()
            .map(|a| a.ip.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let state = if !self.up {
            "link down"
        } else if address.is_empty() {
            "link up, no usable address"
        } else {
            "interface ready"
        };
        Interface {
            name: self.name.clone(),
            address,
            status: format!("{} · {} · {state}", self.kind, self.driver),
            up: self.up,
            ready: self.up && !self.addresses.is_empty(),
        }
    }
}

#[derive(Default)]
struct Scan {
    cables: Vec<Cable>,
    devices: Vec<String>,
}

pub fn interfaces() -> Result<Vec<Interface>> {
    Ok(scan()?.cables.iter().map(Cable::view).collect())
}

pub(crate) fn diagnostic_interfaces() -> Result<(Vec<Interface>, Vec<String>)> {
    let scan = scan()?;
    Ok((scan.cables.iter().map(Cable::view).collect(), scan.devices))
}

fn parse_peer(text: &str, name: &str, index: u32) -> Result<IpAddr> {
    let input = text.trim();
    let input = input
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(input);
    let (literal, zone) = input
        .split_once('%')
        .map_or((input, None), |(ip, zone)| (ip, Some(zone)));
    let ip: IpAddr = literal
        .parse()
        .context("Enter the other computer's cable IP address")?;
    if let Some(zone) = zone {
        ensure!(
            matches!(ip, IpAddr::V6(ip) if ip.is_unicast_link_local()),
            "An interface scope is only valid on a link-local IPv6 address"
        );
        ensure!(
            zone.eq_ignore_ascii_case(name) || zone.parse::<u32>().ok() == Some(index),
            "Use the local interface scope %{name}, or omit the scope from the peer address"
        );
    }
    ensure!(usable(ip), "Enter a unicast address on the cable");
    Ok(ip)
}

fn select(scan: &Scan, options: &Options) -> Result<(String, Vec<Address>, Option<IpAddr>)> {
    let raw_peer = options.peer.as_deref().filter(|s| !s.trim().is_empty());
    let selected = options
        .interface
        .as_deref()
        .filter(|s| !s.trim().is_empty());
    if let Some(name) = selected {
        ensure!(
            scan.cables
                .iter()
                .any(|cable| cable.name.eq_ignore_ascii_case(name)),
            "{name} is not a recognized direct USB or Thunderbolt interface"
        );
    }
    let mut last_error = None;
    let mut candidates: Vec<_> = scan
        .cables
        .iter()
        .filter(|cable| {
            cable.up && selected.is_none_or(|name| cable.name.eq_ignore_ascii_case(name))
        })
        .collect();
    candidates.sort_by_key(|cable| cable.kind != "Thunderbolt/USB4");
    for cable in candidates {
        let mut addresses = cable.addresses.clone();
        addresses.sort_by_key(|address| match address.ip {
            IpAddr::V6(ip) if ip.is_unicast_link_local() => 0,
            IpAddr::V4(ip) if ip.is_link_local() => 1,
            IpAddr::V6(_) => 2,
            IpAddr::V4(_) => 3,
        });
        if let Some(text) = raw_peer {
            for address in addresses {
                match parse_peer(text, &cable.name, address.index) {
                    Ok(peer) if address.directly_reaches(peer) => {
                        return Ok((cable.name.clone(), vec![address], Some(peer)));
                    }
                    Ok(_) => {}
                    Err(error) => last_error = Some(error),
                }
            }
        } else if !addresses.is_empty() {
            let mut ipv4 = false;
            let mut ipv6 = false;
            addresses.retain(|address| {
                let used = if address.ip.is_ipv4() {
                    &mut ipv4
                } else {
                    &mut ipv6
                };
                !std::mem::replace(used, true)
            });
            return Ok((cable.name.clone(), addresses, None));
        }
    }
    if let Some(error) = last_error {
        return Err(error);
    }
    if raw_peer.is_some() {
        bail!(
            "The peer address is outside the selected cable's subnet, or the cable has no usable address"
        );
    }
    bail!("No ready direct USB or Thunderbolt interface. Connect the cable and run doctor.");
}

pub fn probe_peer(options: &Options) -> Result<()> {
    let (_, addresses, peer) = select(&scan()?, options)?;
    let peer = peer.context("A peer address is required for the probe")?;
    establish(addresses[0], peer, PROBE, &[]).map(|_| ())
}

pub fn doctor(options: &Options) -> Result<String> {
    let mut lines = vec![
        crate::diagnostics::ConnectionMonitor::default()
            .poll()
            .text(),
    ];
    if options
        .peer
        .as_ref()
        .is_some_and(|peer| !peer.trim().is_empty())
    {
        lines.push(match probe_peer(options) {
            Ok(()) => "Peer reachable over the cable.".into(),
            Err(error) => format!("Peer probe failed: {error:#}. Open pairing on the peer and check its firewall if necessary."),
        });
    }
    Ok(lines.join("\n"))
}

fn socket(address: Address, kind: Type, protocol: Protocol) -> io::Result<Socket> {
    let domain = if address.ip.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, kind, Some(protocol))?;
    if address.ip.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    pin(&SockRef::from(&socket), address, kind == Type::DGRAM)?;
    Ok(socket)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn pin(socket: &SockRef<'_>, address: Address, _datagram: bool) -> io::Result<()> {
    let index = std::num::NonZeroU32::new(address.index).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "Missing cable interface index")
    })?;
    if address.ip.is_ipv4() {
        socket.bind_device_by_index_v4(Some(index))
    } else {
        socket.bind_device_by_index_v6(Some(index))
    }
}

#[cfg(target_os = "linux")]
fn pin_accepted(stream: &TcpStream, address: Address) -> io::Result<()> {
    // Linux TCP inherits the listener's binding. Setting an already-bound interface
    // requires CAP_NET_RAW, even when setting the same interface index again.
    let socket = SockRef::from(stream);
    let index = if address.ip.is_ipv4() {
        socket.device_index_v4()?
    } else {
        socket.device_index_v6()?
    };
    if index.is_some_and(|index| index.get() == address.index) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Accepted socket did not inherit the cable interface binding",
        ))
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_accepted(stream: &TcpStream, address: Address) -> io::Result<()> {
    pin(&SockRef::from(stream), address, false)
}

#[cfg(windows)]
fn pin(socket: &SockRef<'_>, address: Address, datagram: bool) -> io::Result<()> {
    use std::os::windows::io::AsRawSocket;

    #[link(name = "Ws2_32")]
    unsafe extern "system" {
        fn setsockopt(socket: usize, level: i32, option: i32, value: *const u8, size: i32) -> i32;
        fn WSAGetLastError() -> i32;
    }
    // IP*_UNICAST_IF selects egress. TCP ingress relies on a bound local address
    // and Windows strong-host receive, checked for that address family in scan().
    let level = if address.ip.is_ipv4() { 0 } else { 41 };
    let set = |option, value: u32| {
        let value = value.to_ne_bytes();
        // The raw socket remains borrowed for the call and value is a Winsock DWORD.
        let result = unsafe {
            setsockopt(
                socket.as_raw_socket() as usize,
                level,
                option,
                value.as_ptr(),
                4,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }))
        }
    };
    let unicast_index = if address.ip.is_ipv4() {
        address.index.to_be()
    } else {
        address.index
    };
    const UNICAST_IF: i32 = 31;
    const IFLIST: i32 = 28;
    const ADD_IFLIST: i32 = 29;
    set(UNICAST_IF, unicast_index)?;
    if datagram {
        set(IFLIST, 1)?;
        set(ADD_IFLIST, address.index)?;
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", windows, target_os = "linux")))]
fn pin(_socket: &SockRef<'_>, _address: Address, _datagram: bool) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Cable socket binding is unavailable on this OS",
    ))
}

struct Discovery {
    listen: UdpSocket,
    send: UdpSocket,
    group: SocketAddr,
}

impl Discovery {
    fn new(address: Address) -> io::Result<Self> {
        let listen = socket(address, Type::DGRAM, Protocol::UDP)?;
        let send = socket(address, Type::DGRAM, Protocol::UDP)?;
        let group = match address.ip {
            IpAddr::V4(local) => {
                listen
                    .bind(&SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), DISCOVERY_PORT).into())?;
                listen.join_multicast_v4(&DISCOVERY_V4, &local)?;
                send.set_multicast_if_v4(&local)?;
                send.set_multicast_ttl_v4(1)?;
                SocketAddr::new(DISCOVERY_V4.into(), DISCOVERY_PORT)
            }
            IpAddr::V6(_) => {
                listen
                    .bind(&SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), DISCOVERY_PORT).into())?;
                listen.join_multicast_v6(&DISCOVERY_V6, address.index)?;
                send.set_multicast_if_v6(address.index)?;
                send.set_multicast_hops_v6(1)?;
                SocketAddrV6::new(DISCOVERY_V6, DISCOVERY_PORT, 0, address.index).into()
            }
        };
        send.bind(&address.socket(0).into())?;
        listen.set_nonblocking(true)?;
        Ok(Self {
            listen: listen.into(),
            send: send.into(),
            group,
        })
    }

    fn advertise(&self, mode: Mode, id: NodeId) -> io::Result<()> {
        let mut bytes = [0; 24];
        bytes[..8].copy_from_slice(mode.discovery());
        bytes[8..].copy_from_slice(&id);
        self.send.send_to(&bytes, self.group)?;
        Ok(())
    }
}

struct Endpoint {
    address: Address,
    listener: TcpListener,
    discovery: Option<Discovery>,
    next_attempt: Instant,
}

impl Endpoint {
    fn new(address: Address) -> Result<Self> {
        let server = socket(address, Type::STREAM, Protocol::TCP)?;
        #[cfg(unix)]
        server.set_reuse_address(true)?;
        server.bind(&address.socket(SESSION_PORT).into())?;
        server.listen(4)?;
        server.set_nonblocking(true)?;
        Ok(Self {
            address,
            listener: server.into(),
            discovery: None,
            next_attempt: Instant::now(),
        })
    }

    fn accept(&self, mode: Mode, id: NodeId, manual: Option<IpAddr>) -> Result<Option<TcpStream>> {
        let (mut stream, origin) = match self.listener.accept() {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if !self.address.directly_reaches(origin.ip())
            || manual.is_some_and(|peer| peer != origin.ip())
        {
            return Ok(None);
        }
        stream.set_nonblocking(false)?;
        pin_accepted(&stream, self.address)?;
        preface_limits(&stream)?;
        let mut magic = [0; 8];
        if stream.read_exact(&mut magic).is_err() {
            return Ok(None);
        }
        if &magic == PROBE {
            let _ = stream.write_all(ACCEPTED);
            return Ok(None);
        }
        let valid = if &magic == mode.manual() {
            origin.ip() < self.address.ip
        } else if &magic == mode.automatic() && manual.is_none() {
            let mut identities = [0; 32];
            stream.read_exact(&mut identities).is_ok()
                && identities[16..] == id
                && identities[..16] < id[..]
        } else {
            false
        };
        if !valid || stream.write_all(ACCEPTED).is_err() {
            return Ok(None);
        }
        Ok(Some(stream))
    }
}

fn preface_limits(stream: &TcpStream) -> io::Result<()> {
    stream.set_read_timeout(Some(PREFACE_WAIT))?;
    stream.set_write_timeout(Some(PREFACE_WAIT))?;
    stream.set_nodelay(true)
}

fn establish(address: Address, peer: IpAddr, magic: &[u8; 8], data: &[u8]) -> Result<TcpStream> {
    ensure!(
        address.directly_reaches(peer),
        "Peer is not on the selected cable"
    );
    let client = socket(address, Type::STREAM, Protocol::TCP)?;
    client.bind(&address.socket(0).into())?;
    client
        .connect_timeout(
            &address.peer_socket(peer, SESSION_PORT).into(),
            CONNECT_WAIT,
        )
        .context("Could not reach the pairing listener on the selected cable")?;
    let mut stream: TcpStream = client.into();
    preface_limits(&stream)?;
    stream.write_all(magic)?;
    stream.write_all(data)?;
    let mut reply = [0; 8];
    stream.read_exact(&mut reply)?;
    ensure!(
        &reply == ACCEPTED,
        "Peer did not recognize the cable pairing protocol"
    );
    Ok(stream)
}

pub fn connect(
    options: &Options,
    cancel: &AtomicBool,
    status: impl FnMut(String),
) -> Result<(TcpStream, bool)> {
    connect_mode(options, cancel, Mode::Pair, status)
}

fn connect_mode(
    options: &Options,
    cancel: &AtomicBool,
    mode: Mode,
    mut status: impl FnMut(String),
) -> Result<(TcpStream, bool)> {
    let (name, addresses, manual) = select(&scan()?, options)?;
    let mut endpoints = Vec::new();
    for address in addresses {
        let mut endpoint = match Endpoint::new(address) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                status(format!(
                    "Could not open {} on {name}: {error:#}",
                    address.ip
                ));
                continue;
            }
        };
        if manual.is_none() {
            match Discovery::new(address) {
                Ok(discovery) => endpoint.discovery = Some(discovery),
                Err(error) => status(format!(
                    "Discovery unavailable for {}: {error}. Manual pairing is available.",
                    address.ip
                )),
            }
        }
        endpoints.push(endpoint);
    }
    ensure!(
        !endpoints.is_empty(),
        "Could not bind a pairing socket to {name}. Close other copies of the app and check the cable."
    );
    ensure!(
        manual.is_some()
            || endpoints
                .iter()
                .any(|endpoint| endpoint.discovery.is_some()),
        "Discovery could not start on {name}. Try manual pairing on both computers."
    );
    let mut id = [0; 16];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut id)
        .map_err(|_| anyhow::anyhow!("Could not generate a discovery identifier"))?;
    status(format!(
        "{} on {} ({})",
        if manual.is_some() {
            "Waiting for manual peer"
        } else {
            "Discovering peers"
        },
        name,
        endpoints
            .iter()
            .map(|endpoint| endpoint.address.socket(SESSION_PORT).to_string())
            .collect::<Vec<_>>()
            .join(", "),
    ));
    let mut next_advertisement = Instant::now();
    let mut discovery_error_reported = false;
    while !cancel.load(Ordering::Relaxed) {
        let now = Instant::now();
        for endpoint in &mut endpoints {
            if let Some(stream) = endpoint.accept(mode, id, manual)? {
                return Ok((stream, false));
            }
            if let Some(peer) = manual {
                if endpoint.address.ip < peer && now >= endpoint.next_attempt {
                    endpoint.next_attempt = now + Duration::from_millis(600);
                    if let Ok(stream) = establish(endpoint.address, peer, mode.manual(), &[]) {
                        return Ok((stream, true));
                    }
                }
                continue;
            }
            let Some(discovery) = &endpoint.discovery else {
                continue;
            };
            if now >= next_advertisement
                && let Err(error) = discovery.advertise(mode, id)
                && !discovery_error_reported
            {
                status(format!(
                    "Discovery send on {name} failed: {error}. Check the cable or use manual pairing."
                ));
                discovery_error_reported = true;
            }
            for _ in 0..8 {
                let mut packet = [0; 25];
                match discovery.listen.recv_from(&mut packet) {
                    Ok((24, from))
                        if &packet[..8] == mode.discovery()
                            && packet[8..24] > id[..]
                            && endpoint.address.directly_reaches(from.ip())
                            && now >= endpoint.next_attempt =>
                    {
                        endpoint.next_attempt = now + Duration::from_millis(200);
                        let mut handshake = [0; 32];
                        handshake[..16].copy_from_slice(&id);
                        handshake[16..].copy_from_slice(&packet[8..24]);
                        if let Ok(stream) =
                            establish(endpoint.address, from.ip(), mode.automatic(), &handshake)
                        {
                            return Ok((stream, true));
                        }
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => return Err(error).context("Peer discovery receive failed"),
                }
            }
        }
        if now >= next_advertisement {
            next_advertisement = now + Duration::from_millis(625);
        }
        thread::sleep(Duration::from_millis(35));
    }
    bail!("Pairing canceled")
}

pub fn benchmark(
    options: &Options,
    cancel: &AtomicBool,
    bytes: u64,
    mut status: impl FnMut(String),
) -> Result<String> {
    const MIB: u64 = 1024 * 1024;
    ensure!(
        (MIB..=16 * 1024 * MIB).contains(&bytes),
        "Benchmark size must be between 1 MiB and 16 GiB"
    );
    let (mut stream, initiator) = connect_mode(options, cancel, Mode::Benchmark, &mut status)?;
    let _link = watch(&stream)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    stream.write_all(&bytes.to_be_bytes())?;
    let mut peer_bytes = [0; 8];
    stream.read_exact(&mut peer_bytes)?;
    ensure!(
        u64::from_be_bytes(peer_bytes) == bytes,
        "Use the same benchmark size on both computers"
    );
    let mut buffer = vec![0; MIB as usize];
    let mut transfer = |sending| {
        status(
            if sending {
                "Measuring raw TCP upload…"
            } else {
                "Measuring raw TCP download…"
            }
            .into(),
        );
        measure(&mut stream, &mut buffer, bytes, sending, cancel)
    };
    let (sent, received) = if initiator {
        (transfer(true)?, transfer(false)?)
    } else {
        let received = transfer(false)?;
        (transfer(true)?, received)
    };
    let mib = bytes as f64 / MIB as f64;
    Ok(format!(
        "Raw TCP over the cable ({mib:.0} MiB each way): upload {:.2} MiB/s, download {:.2} MiB/s",
        mib / sent.as_secs_f64(),
        mib / received.as_secs_f64()
    ))
}

pub struct LinkGuard {
    _stop: mpsc::Sender<()>,
}

pub fn watch(stream: &TcpStream) -> Result<LinkGuard> {
    let bound = stream.local_addr()?;
    let scoped_index = match bound {
        SocketAddr::V6(ip) => ip.scope_id(),
        SocketAddr::V4(_) => 0,
    };
    let snapshot = scan()?;
    let mut candidates = snapshot
        .cables
        .iter()
        .filter(|cable| cable.up)
        .flat_map(|cable| cable.addresses.iter().map(move |address| (cable, address)))
        .filter(|(_, address)| {
            address.ip == bound.ip() && (scoped_index == 0 || scoped_index == address.index)
        });
    let (cable, &address) = candidates
        .next()
        .context("The connected socket's cable address disappeared")?;
    ensure!(
        candidates.next().is_none(),
        "More than one cable adapter has the connected socket's address"
    );
    let check = link_checker(cable.name.clone(), address)?;
    ensure!(check(), "Cable disconnected before the session started");
    let stream = stream.try_clone()?;
    let (stop, signal) = mpsc::channel();
    thread::Builder::new()
        .name("cable-carrier".into())
        .spawn(move || {
            while let Err(mpsc::RecvTimeoutError::Timeout) =
                signal.recv_timeout(Duration::from_millis(250))
            {
                if !check() {
                    let _ = stream.shutdown(Shutdown::Both);
                    return;
                }
            }
        })?;
    Ok(LinkGuard { _stop: stop })
}

#[cfg(target_os = "macos")]
fn link_checker(name: String, address: Address) -> Result<impl Fn() -> bool + Send + 'static> {
    Ok(move || {
        let mut ifconfig = Command::new("/sbin/ifconfig");
        ifconfig.arg("-v").arg(&name).env("LC_ALL", "C");
        let Ok(output) = command(ifconfig, "ifconfig") else {
            return false;
        };
        let index = output
            .lines()
            .next()
            .and_then(|head| head.rsplit_once(" index "))
            .and_then(|(_, index)| index.parse::<u32>().ok());
        index == Some(address.index)
            && output.lines().any(|line| line.trim() == "status: active")
            && output.lines().any(|line| {
                let mut tokens = line.split_whitespace();
                matches!(tokens.next(), Some("inet" | "inet6"))
                    && tokens
                        .next()
                        .and_then(|ip| ip.split('%').next())
                        .and_then(|ip| ip.parse().ok())
                        == Some(address.ip)
            })
    })
}

#[cfg(windows)]
fn link_checker(_name: String, address: Address) -> Result<impl Fn() -> bool + Send + 'static> {
    use windows_sys::Win32::{
        NetworkManagement::{
            IpHelper::{
                GetIfEntry2, GetUnicastIpAddressEntry, MIB_IF_ROW2, MIB_UNICASTIPADDRESS_ROW,
            },
            Ndis::{IfOperStatusUp, MediaConnectStateDisconnected},
        },
        Networking::WinSock::{
            AF_INET, AF_INET6, IpDadStatePreferred, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
        },
    };

    let interface = move || -> Option<MIB_IF_ROW2> {
        let mut row = MIB_IF_ROW2 {
            InterfaceIndex: address.index,
            ..Default::default()
        };
        // GetIfEntry2 writes the initialized MIB row using its index as the key.
        (unsafe { GetIfEntry2(&mut row) } == 0).then_some(row)
    };
    let first = interface().context("Could not read the Windows cable adapter's carrier state")?;
    // Value is the complete NET_LUID representation, which identifies this adapter.
    let adapter_id = unsafe { first.InterfaceLuid.Value };
    let socket_address = match address.ip {
        IpAddr::V4(ip) => {
            let mut address = SOCKADDR_IN {
                sin_family: AF_INET,
                ..Default::default()
            };
            address.sin_addr.S_un.S_addr = u32::from_ne_bytes(ip.octets());
            SOCKADDR_INET { Ipv4: address }
        }
        IpAddr::V6(ip) => {
            let mut sockaddr = SOCKADDR_IN6 {
                sin6_family: AF_INET6,
                ..Default::default()
            };
            sockaddr.sin6_addr.u.Byte = ip.octets();
            if ip.is_unicast_link_local() {
                sockaddr.Anonymous.sin6_scope_id = address.index;
            }
            SOCKADDR_INET { Ipv6: sockaddr }
        }
    };
    Ok(move || {
        let Some(row) = interface() else { return false };
        if row.OperStatus != IfOperStatusUp
            || row.MediaConnectState == MediaConnectStateDisconnected
            || unsafe { row.InterfaceLuid.Value } != adapter_id
        {
            return false;
        }
        let mut local = MIB_UNICASTIPADDRESS_ROW {
            Address: socket_address,
            InterfaceLuid: row.InterfaceLuid,
            ..Default::default()
        };
        // The union's address and family are initialized above and the LUID selects the adapter.
        (unsafe { GetUnicastIpAddressEntry(&mut local) == 0 })
            && local.DadState == IpDadStatePreferred
            && local.OnLinkPrefixLength == address.prefix
    })
}

#[cfg(target_os = "linux")]
fn link_checker(name: String, address: Address) -> Result<impl Fn() -> bool + Send + 'static> {
    let carrier = std::path::Path::new("/sys/class/net")
        .join(&name)
        .join("carrier");
    Ok(move || {
        std::fs::read_to_string(&carrier).is_ok_and(|value| value.trim() == "1")
            && linux_links(Some(&name)).is_ok_and(|links| {
                links.iter().any(|link| {
                    link.ifname == name
                        && link.up()
                        && link.addresses().any(|entry| entry == address)
                })
            })
    })
}

#[cfg(not(any(target_os = "macos", windows, target_os = "linux")))]
fn link_checker(_name: String, _address: Address) -> Result<impl Fn() -> bool + Send + 'static> {
    Ok(|| false)
}

fn measure(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    bytes: u64,
    sending: bool,
    cancel: &AtomicBool,
) -> Result<Duration> {
    let start = Instant::now();
    let mut remaining = bytes;
    while remaining != 0 {
        ensure!(!cancel.load(Ordering::Relaxed), "Benchmark canceled");
        let count = remaining.min(buffer.len() as u64) as usize;
        if sending {
            stream.write_all(&buffer[..count])?;
        } else {
            stream.read_exact(&mut buffer[..count])?;
        }
        remaining -= count as u64;
    }
    if sending {
        let mut acknowledgment = [0; 8];
        stream.read_exact(&mut acknowledgment)?;
        ensure!(
            &acknowledgment == ACCEPTED,
            "Peer did not acknowledge the benchmark bytes"
        );
    } else {
        stream.write_all(ACCEPTED)?;
    }
    Ok(start.elapsed())
}

fn command(mut command: Command, name: &str) -> Result<String> {
    let output = command
        .output()
        .with_context(|| format!("Could not execute {name}"))?;
    ensure!(
        output.status.success(),
        "{name} failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8(output.stdout).with_context(|| format!("{name} output was not UTF-8"))
}

#[cfg(target_os = "macos")]
fn scan() -> Result<Scan> {
    let mut ifconfig = Command::new("/sbin/ifconfig");
    ifconfig.args(["-a", "-v"]).env("LC_ALL", "C");
    let ifconfig = command(ifconfig, "ifconfig")?;
    let mut ioreg = Command::new("/usr/sbin/ioreg");
    ioreg.arg("-w0");
    let ioreg = command(ioreg, "ioreg")?;
    Ok(parse_mac(&ifconfig, &ioreg))
}

#[cfg(any(target_os = "macos", test))]
fn parse_mac(ifconfig: &str, ioreg: &str) -> Scan {
    use std::collections::HashSet;

    let mut ncm_interfaces = HashSet::new();
    let mut ncm_level = None;
    for line in ioreg.lines() {
        let Some((indent, child)) = line.split_once("+-o ") else {
            continue;
        };
        let level = indent.len();
        if ncm_level.is_some_and(|parent| level <= parent) {
            ncm_level = None;
        }
        if child.contains("<class AppleUSBDeviceNCM")
            && child
                .split_whitespace()
                .next()
                .is_some_and(|name| name.starts_with("AppleUSBDeviceNCM"))
        {
            ncm_level = Some(level);
        } else if ncm_level.is_some()
            && child.contains("EthernetInterface,")
            && let Some(name) = child.split_whitespace().next()
        {
            ncm_interfaces.insert(name);
        }
    }

    let blocks: Vec<_> = ifconfig.lines().filter(|line| !line.is_empty()).fold(
        Vec::<Vec<&str>>::new(),
        |mut groups, line| {
            if !line.starts_with(char::is_whitespace) {
                groups.push(vec![line]);
            } else if let Some(last) = groups.last_mut() {
                last.push(line.trim());
            }
            groups
        },
    );
    let name = |line: &str| line.split(':').next().unwrap_or_default().to_owned();
    let thunderbolt: HashSet<_> = blocks
        .iter()
        .filter(|block| block.contains(&"type: IP over Thunderbolt"))
        .map(|block| name(block[0]))
        .collect();
    let mut scan = Scan::default();
    for block in blocks {
        let head = block[0];
        let name = name(head);
        let members: Vec<_> = block
            .iter()
            .filter_map(|line| line.strip_prefix("member: "))
            .filter_map(|line| line.split_whitespace().next())
            .collect();
        let thunderbolt_bridge =
            !members.is_empty() && members.iter().all(|member| thunderbolt.contains(*member));
        let (kind, driver) = if thunderbolt_bridge || thunderbolt.contains(&name) {
            ("Thunderbolt/USB4", "macOS Thunderbolt IP")
        } else if ncm_interfaces.contains(name.as_str()) {
            ("USB", "macOS Apple USB Device NCM")
        } else {
            continue;
        };
        let Some(index) = head
            .rsplit_once(" index ")
            .and_then(|(_, index)| index.parse::<u32>().ok())
        else {
            continue;
        };
        let mut addresses = Vec::new();
        for line in &block[1..] {
            let mut tokens = line.split_whitespace();
            let Some(family) = tokens.next() else {
                continue;
            };
            if family != "inet" && family != "inet6" {
                continue;
            }
            let Some(address) = tokens.next() else {
                continue;
            };
            let Ok(ip) = address.split('%').next().unwrap_or_default().parse() else {
                continue;
            };
            let prefix = match (family, tokens.next(), tokens.next()) {
                ("inet", Some("netmask"), Some(mask)) => {
                    u32::from_str_radix(mask.trim_start_matches("0x"), 16)
                        .ok()
                        .filter(|mask| mask.count_ones() == mask.leading_ones())
                        .map(|mask| mask.leading_ones() as u8)
                }
                ("inet6", Some("prefixlen"), Some(prefix)) => prefix.parse().ok(),
                _ => None,
            };
            if usable(ip)
                && let Some(prefix) = prefix
            {
                addresses.push(Address { ip, prefix, index });
            }
        }
        let up = block.contains(&"status: active")
            && head
                .split_once('<')
                .and_then(|(_, flags)| flags.split_once('>'))
                .is_some_and(|(flags, _)| flags.split(',').any(|flag| flag == "UP"));
        scan.cables.push(Cable {
            name,
            kind,
            driver: driver.to_owned(),
            up,
            addresses,
        });
    }
    // The public Apple NCM pair is exposed as en*, ahead of the anpi* private pair.
    scan.cables
        .sort_by_key(|cable| cable.kind == "USB" && cable.name.starts_with("anpi"));
    scan
}

#[cfg(windows)]
fn scan() -> Result<Scan> {
    use std::os::windows::process::CommandExt;

    let script = r#"
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$apple = '^USB\\VID_05AC&PID_190[235](?:[&\\]|$)'
$adapters = @(
  foreach ($a in (Get-NetAdapter -IncludeHidden)) {
    $id = [string]$a.PnPDeviceID
    $tb = $id -like 'USB4\USB4NET*' -or $a.InterfaceDescription -match 'USB4.*P2P|Thunderbolt.*Networking'
    if (!$tb -and $id -notmatch $apple) { continue }
    $addresses = @(
      foreach ($entry in (Get-NetIPInterface -InterfaceIndex $a.ifIndex -ErrorAction SilentlyContinue)) {
        foreach ($v in (Get-NetIPAddress -InterfaceIndex $a.ifIndex -AddressFamily $entry.AddressFamily -ErrorAction SilentlyContinue)) {
          if ($v.AddressState -eq 'Preferred') {
            @{ ip = [string]$v.IPAddress; prefix = [int]$v.PrefixLength; weak_host_receive = [string]$entry.WeakHostReceive }
          }
        }
      }
    )
    @{
      name = [string]$a.Name; thunderbolt = [bool]$tb; up = ($a.Status -eq 'Up');
      index = [int]$a.ifIndex; driver = ([string]$a.DriverFileName + ' ' + [string]$a.DriverVersion).Trim();
      addresses = $addresses
    }
  }
)
$devices = @(
  foreach ($d in (Get-PnpDevice -PresentOnly -ErrorAction SilentlyContinue)) {
    if ($d.InstanceId -match $apple) {
      'Apple USB device {0} ({1}), PnP status: {2}' -f $d.InstanceId.Substring(4, 17), $d.FriendlyName, $d.Status
    }
  }
)
@{ adapters = $adapters; devices = $devices } | ConvertTo-Json -Depth 5 -Compress
"#;
    let root = std::env::var_os("SystemRoot").context("Windows SystemRoot is missing")?;
    let mut powershell = Command::new(
        std::path::Path::new(&root).join("System32/WindowsPowerShell/v1.0/powershell.exe"),
    );
    powershell
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            script,
        ])
        .creation_flags(0x08000000);
    parse_windows(&command(powershell, "Windows network diagnostics")?)
}

#[cfg(any(windows, test))]
fn parse_windows(output: &str) -> Result<Scan> {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Snapshot {
        adapters: Vec<Adapter>,
        devices: Vec<String>,
    }

    #[derive(Deserialize)]
    struct Adapter {
        name: String,
        thunderbolt: bool,
        driver: String,
        up: bool,
        index: u32,
        addresses: Vec<WindowsAddress>,
    }

    #[derive(Deserialize)]
    struct WindowsAddress {
        ip: String,
        prefix: u8,
        weak_host_receive: String,
    }

    let snapshot: Snapshot = serde_json::from_str(output)
        .context("Could not decode Windows network adapter information")?;
    let cables = snapshot
        .adapters
        .into_iter()
        .map(|adapter| Cable {
            name: adapter.name,
            kind: if adapter.thunderbolt {
                "Thunderbolt/USB4"
            } else {
                "USB"
            },
            driver: if adapter.driver.is_empty() {
                "Windows network driver".into()
            } else {
                adapter.driver
            },
            up: adapter.up,
            addresses: adapter
                .addresses
                .into_iter()
                .filter_map(|address| {
                    if !matches!(
                        address.weak_host_receive.as_str(),
                        "Disabled" | "0" | "False"
                    ) {
                        return None;
                    }
                    let (literal, scope) = address.ip.split_once('%').unwrap_or((&address.ip, ""));
                    let ip = literal.parse().ok()?;
                    let index = scope.parse().unwrap_or(adapter.index);
                    (index != 0 && usable(ip)).then_some(Address {
                        ip,
                        prefix: address.prefix,
                        index,
                    })
                })
                .collect(),
        })
        .collect();
    let mut devices = snapshot.devices;
    devices.push("Windows cable sockets require strong-host receive on the chosen adapter.".into());
    Ok(Scan { cables, devices })
}

#[cfg(target_os = "linux")]
fn scan() -> Result<Scan> {
    use std::path::Path;

    Ok(scan_linux(
        linux_links(None)?,
        Path::new("/sys/class/net"),
        Path::new("/sys/bus/usb/devices"),
    ))
}

#[cfg(target_os = "linux")]
fn linux_links(name: Option<&str>) -> Result<Vec<LinuxLink>> {
    use std::path::Path;

    let executable = ["/usr/sbin/ip", "/usr/bin/ip", "/sbin/ip", "/bin/ip"]
        .into_iter()
        .find(|path| Path::new(path).is_file())
        .unwrap_or("ip");
    let mut ip = Command::new(executable);
    ip.args(["-j", "address", "show"]);
    if let Some(name) = name {
        ip.args(["dev", name]);
    }
    let output =
        command(ip, "ip address").context("Linux interface discovery requires iproute2")?;
    serde_json::from_str(&output).context("Could not read Linux address information")
}

#[cfg(any(target_os = "linux", all(test, unix)))]
#[derive(serde::Deserialize)]
struct LinuxLink {
    ifname: String,
    ifindex: u32,
    #[serde(default)]
    flags: Vec<String>,
    #[serde(default)]
    operstate: String,
    #[serde(default)]
    addr_info: Vec<LinuxAddress>,
}

#[cfg(any(target_os = "linux", all(test, unix)))]
#[derive(serde::Deserialize)]
struct LinuxAddress {
    #[serde(default)]
    local: String,
    #[serde(default)]
    prefixlen: u8,
    #[serde(default)]
    tentative: bool,
    #[serde(default)]
    dadfailed: bool,
    #[serde(default)]
    deprecated: bool,
}

#[cfg(any(target_os = "linux", all(test, unix)))]
impl LinuxLink {
    fn up(&self) -> bool {
        let has = |flag| self.flags.iter().any(|value| value == flag);
        has("UP") && has("LOWER_UP") && matches!(self.operstate.as_str(), "UP" | "UNKNOWN")
    }

    fn addresses(&self) -> impl Iterator<Item = Address> + '_ {
        self.addr_info.iter().filter_map(|address| {
            if address.tentative || address.dadfailed || address.deprecated {
                return None;
            }
            let ip: IpAddr = address.local.parse().ok()?;
            let prefix = address.prefixlen;
            let direct_prefix = if ip.is_ipv4() { 1..=31 } else { 1..=127 };
            (usable(ip) && direct_prefix.contains(&prefix)).then_some(Address {
                ip,
                prefix,
                index: self.ifindex,
            })
        })
    }
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn scan_linux(
    links: Vec<LinuxLink>,
    net_root: &std::path::Path,
    usb_root: &std::path::Path,
) -> Scan {
    use std::fs;

    let mut ranked = Vec::new();
    for link in links {
        let name = link.ifname.as_str();
        if link.ifindex == 0 || name.contains('/') || matches!(name, "" | "." | "..") {
            continue;
        }
        let device = net_root.join(name).join("device");
        let Ok(usb_driver) = fs::read_link(device.join("driver")) else {
            continue;
        };
        let (kind, driver, private) = match usb_driver.file_name().and_then(|name| name.to_str()) {
            Some("thunderbolt-net") => ("Thunderbolt/USB4", "thunderbolt-net".to_owned(), false),
            Some("cdc_ncm") => {
                let Ok(interface) = fs::canonicalize(&device) else {
                    continue;
                };
                let Some(product) = interface.parent().and_then(apple_ncm_product) else {
                    continue;
                };
                let number =
                    fs::read_to_string(interface.join("bInterfaceNumber")).unwrap_or_default();
                let private = number.trim() != "00";
                ("USB", format!("cdc_ncm 05ac:{product}"), private)
            }
            _ => continue,
        };
        ranked.push((
            private,
            Cable {
                name: name.into(),
                kind,
                driver,
                up: link.up(),
                addresses: link.addresses().collect(),
            },
        ));
    }
    ranked.sort_by_key(|(private, _)| *private);
    let cables: Vec<_> = ranked.into_iter().map(|(_, cable)| cable).collect();

    let mut devices = Vec::new();
    if let Ok(entries) = fs::read_dir(usb_root) {
        for entry in entries.flatten() {
            if let Some(product) = apple_ncm_product(&entry.path()) {
                devices.push(format!("Apple USB device 05ac:{product} detected"));
            }
        }
    }
    devices.sort_unstable();
    devices.dedup();
    if !devices.is_empty() && !cables.iter().any(|cable| cable.kind == "USB") {
        devices.push("The Mac enumerated over USB but no supported cdc_ncm network adapter appeared. Check whether the running Linux kernel supports this device ID.".into());
    }
    Scan { cables, devices }
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn apple_ncm_product(device: &std::path::Path) -> Option<String> {
    let vendor = std::fs::read_to_string(device.join("idVendor")).ok()?;
    let product = std::fs::read_to_string(device.join("idProduct")).ok()?;
    let product = product.trim();
    (vendor.trim().eq_ignore_ascii_case("05ac") && matches!(product, "1902" | "1903" | "1905"))
        .then(|| product.to_owned())
}

#[cfg(not(any(target_os = "macos", windows, target_os = "linux")))]
fn scan() -> Result<Scan> {
    bail!("Direct cable adapter detection is unavailable on this OS")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_ipv6_and_rejects_other_subnets() {
        let address = Address {
            ip: "fe80::1".parse().unwrap(),
            prefix: 64,
            index: 6,
        };
        let peer = parse_peer("fe80::2%en6", "en6", 6).unwrap();
        assert!(address.directly_reaches(peer));
        assert_eq!(
            address.peer_socket(peer, SESSION_PORT).to_string(),
            "[fe80::2%6]:48393"
        );
        assert!(parse_peer("fe80::2%en0", "en6", 6).is_err());
        assert!(!address.directly_reaches("2001:db8::1".parse().unwrap()));
        let address = Address {
            ip: "169.254.17.2".parse().unwrap(),
            prefix: 16,
            index: 6,
        };
        assert!(address.directly_reaches("169.254.8.1".parse().unwrap()));
        assert!(!address.directly_reaches("192.168.0.1".parse().unwrap()));
        assert!(!address.directly_reaches("169.254.255.255".parse().unwrap()));
    }

    #[test]
    fn recognizes_macos_drivers_and_excludes_wifi_and_external_adapters() {
        let ifconfig = "en0: flags=8863<UP,RUNNING> mtu 1500 index 4\n\tstatus: active\n\ttype: Wi-Fi\n\
                        en1: flags=8863<UP,RUNNING> mtu 1500 index 5\n\ttype: IP over Thunderbolt\n\
                        bridge0: flags=8863<UP,RUNNING> mtu 1500 index 6\n\tmember: en1 flags=3\n\tstatus: active\n\tinet6 fe80::1%bridge0 prefixlen 64\n\
                        en3: flags=8863<UP,RUNNING> mtu 1500 index 7\n\ttype: USB Ethernet\n\tstatus: active\n\tinet 169.254.17.2 netmask 0xffff0000\n\
                        en9: flags=8863<UP,RUNNING> mtu 1500 index 8\n\ttype: USB Ethernet\n\tstatus: active\n";
        let ioreg = "  +-o AppleUSBDeviceNCM11Data  <class AppleUSBDeviceNCM11Data, id 0x123>\n    +-o en3  <class IOEthernetInterface, id 0x124>\n  +-o en9  <class IOEthernetInterface, id 0x125>\n";
        let scan = parse_mac(ifconfig, ioreg);
        let names: Vec<_> = scan
            .cables
            .iter()
            .map(|cable| cable.name.as_str())
            .collect();
        assert_eq!(names, ["en1", "bridge0", "en3"]);
        let (chosen, _, _) = select(&scan, &Options::default()).unwrap();
        assert_eq!(chosen, "bridge0");
        let options = Options {
            interface: None,
            peer: Some("169.254.8.1".into()),
        };
        assert_eq!(select(&scan, &options).unwrap().0, "en3");
    }

    #[test]
    fn macos_prefers_the_public_ncm_pair() {
        let ifconfig = "anpi2: flags=1<UP> index 10\n\tstatus: active\n\tinet6 fe80::1%anpi2 prefixlen 64\n\
                        en7: flags=1<UP> index 11\n\tstatus: active\n\tinet6 fe80::2%en7 prefixlen 64\n";
        let ioreg = "  +-o AppleUSBDeviceNCM11Data <class AppleUSBDeviceNCM11Data>\n\
                        \x20   +-o anpi2 <class IOEthernetInterface, id 0x123>\n\
                        \x20   +-o en7 <class IOEthernetInterface, id 0x124>\n";
        let scan = parse_mac(ifconfig, ioreg);
        assert_eq!(select(&scan, &Options::default()).unwrap().0, "en7");
        let private = Options {
            interface: Some("anpi2".into()),
            peer: None,
        };
        assert_eq!(select(&scan, &private).unwrap().0, "anpi2");
    }

    #[cfg(unix)]
    #[test]
    fn linux_filters_adapters_and_uses_the_public_ncm_pair() {
        use std::{fs, os::unix::fs::symlink};

        let fixture = tempfile::tempdir().unwrap();
        let net = fixture.path().join("net");
        let usb = fixture.path().join("usb");
        let apple = usb.join("1-1");
        let generic = usb.join("1-2");
        for (device, vendor, product) in [(&apple, "05ac", "1902"), (&generic, "1234", "1902")] {
            fs::create_dir_all(device).unwrap();
            fs::write(device.join("idVendor"), vendor).unwrap();
            fs::write(device.join("idProduct"), product).unwrap();
        }
        for (name, device, child, number) in [
            ("enx-private", &apple, "1-1:1.2", "02"),
            ("enx-public", &apple, "1-1:1.0", "00"),
            ("enx-external", &generic, "1-2:1.0", "00"),
        ] {
            let interface = device.join(child);
            fs::create_dir(&interface).unwrap();
            fs::write(interface.join("bInterfaceNumber"), number).unwrap();
            symlink("/sys/bus/usb/drivers/cdc_ncm", interface.join("driver")).unwrap();
            fs::create_dir_all(net.join(name)).unwrap();
            symlink(&interface, net.join(name).join("device")).unwrap();
        }

        let links = serde_json::from_str(
            r#"[
          {"ifindex":10,"ifname":"enx-private","operstate":"UP","flags":["UP","LOWER_UP"],
           "addr_info":[{"local":"fe80::4","prefixlen":64}]},
          {"ifindex":11,"ifname":"enx-public","operstate":"UNKNOWN","flags":["UP","LOWER_UP"],
           "addr_info":[{"local":"fe80::5","prefixlen":64,"tentative":true},
                        {"local":"fe80::6","prefixlen":64,"dadfailed":true},
                        {"local":"fe80::7","prefixlen":64,"deprecated":true},
                        {"local":"fe80::8","prefixlen":64}]},
          {"ifindex":12,"ifname":"enx-external","operstate":"UP","flags":["UP","LOWER_UP"],
           "addr_info":[{"local":"fe80::9","prefixlen":64}]}
        ]"#,
        )
        .unwrap();
        let scan = scan_linux(links, &net, &usb);
        assert_eq!(scan.cables.len(), 2);
        assert_eq!(scan.devices, ["Apple USB device 05ac:1902 detected"]);
        let (name, addresses, _) = select(&scan, &Options::default()).unwrap();
        assert_eq!(name, "enx-public");
        assert_eq!(addresses.len(), 1);
        assert_eq!(
            addresses[0].socket(SESSION_PORT).to_string(),
            "[fe80::8%11]:48393"
        );

        let no_adapters = scan_linux(Vec::new(), &net, &usb);
        assert!(
            no_adapters
                .devices
                .iter()
                .any(|entry| entry.contains("no supported cdc_ncm"))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_accept_inherits_the_listener_device_binding() {
        // The loopback interface is always present in the Linux network namespace.
        let index = unsafe { libc::if_nametoindex(c"lo".as_ptr()) };
        assert_ne!(index, 0);
        let address = Address {
            ip: Ipv4Addr::LOCALHOST.into(),
            prefix: 8,
            index,
        };
        let listener = socket(address, Type::STREAM, Protocol::TCP).unwrap();
        listener.bind(&address.socket(0).into()).unwrap();
        listener.listen(1).unwrap();
        let listener = TcpListener::from(listener);
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        assert!(pin_accepted(&accepted, address).is_ok());
        assert!(
            pin_accepted(
                &accepted,
                Address {
                    index: 0,
                    ..address
                }
            )
            .is_err()
        );
    }

    #[test]
    fn windows_uses_only_address_families_with_strong_host_receive() {
        let scan = parse_windows(r#"{
            "devices":["Apple USB device VID_05AC&PID_1902"],
            "adapters":[{"name":"Ethernet 3","thunderbolt":false,"driver":"AppleNcm.sys","up":true,"index":13,
              "addresses":[
                {"ip":"169.254.2.2","prefix":16,"weak_host_receive":"Enabled"},
                {"ip":"192.168.50.3","prefix":24,"weak_host_receive":""},
                {"ip":"fe80::1%14","prefix":64,"weak_host_receive":"Disabled"}
              ]}]
        }"#).unwrap();
        assert_eq!(scan.cables[0].view().address, "fe80::1");
        let (name, addresses, _) = select(&scan, &Options::default()).unwrap();
        assert_eq!(name, "Ethernet 3");
        assert_eq!(
            addresses[0].socket(SESSION_PORT).to_string(),
            "[fe80::1%14]:48393"
        );
        let ipv4 = Options {
            interface: None,
            peer: Some("169.254.2.1".into()),
        };
        assert!(select(&scan, &ipv4).is_err());
    }
}
