use crate::{network, transfer};
use anyhow::{Context, Result, bail, ensure};
use rcgen::generate_simple_self_signed;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{
    ClientConfig, ClientConnection, DigitallySignedStruct, ServerConfig, ServerConnection,
    SignatureScheme, StreamOwned,
};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use transfer::{Manifest, Prepared, Summary};

const PROTOCOL: &[u8] = b"usb-direct-transfer/1";
const MAX_FRAME: usize = 16 * 1024 * 1024;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(180);
const POLL_INTERVAL: Duration = Duration::from_millis(150);

#[derive(Debug)]
pub enum Command {
    Confirm(bool),
    Send(Vec<PathBuf>),
    Approve(Option<PathBuf>),
    Disconnect,
}

#[derive(Debug)]
pub enum Event {
    Status(String),
    PairCode(String),
    Paired,
    Incoming {
        names: Vec<String>,
        bytes: u64,
    },
    IncomingClosed {
        accepted: bool,
    },
    Progress {
        receiving: bool,
        done: u64,
        total: u64,
    },
    Completed {
        receiving: bool,
        summary: Summary,
    },
    Error(String),
    Disconnected,
}

#[derive(Default)]
struct ConnectionControl {
    cancelled: AtomicBool,
    socket: Mutex<Option<TcpStream>>,
}

impl ConnectionControl {
    fn close(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
        if let Some(socket) = self.socket.lock().unwrap().take() {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }

    fn attach(&self, stream: &TcpStream) -> Result<()> {
        let mut socket = self.socket.lock().unwrap();
        ensure!(!self.cancelled.load(Ordering::Relaxed), "Session closed");
        *socket = Some(stream.try_clone()?);
        Ok(())
    }
}

pub struct Session {
    pub commands: mpsc::Sender<Command>,
    pub events: mpsc::Receiver<Event>,
    control: Arc<ConnectionControl>,
}

impl Session {
    pub fn disconnect(&self) {
        self.control.close();
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.disconnect();
    }
}

pub fn start(options: network::Options, destination: PathBuf) -> Session {
    let (commands, requests) = mpsc::channel();
    let (updates, events) = mpsc::channel();
    let control = Arc::new(ConnectionControl::default());
    let worker_control = Arc::clone(&control);
    thread::spawn(move || {
        let result = (|| {
            let (stream, client) =
                network::connect(&options, &worker_control.cancelled, |status| {
                    emit(&updates, Event::Status(status));
                })?;
            worker_control.attach(&stream)?;
            let _link = network::watch(&stream)?;
            run(stream, client, &destination, &requests, &updates)
        })();
        if let Err(error) = result
            && !worker_control.cancelled.load(Ordering::Relaxed)
        {
            emit(&updates, Event::Error(format!("{error:#}")));
        }
        worker_control.close();
        emit(&updates, Event::Disconnected);
    });
    Session {
        commands,
        events,
        control,
    }
}

fn emit(updates: &mpsc::Sender<Event>, event: Event) {
    let _ = updates.send(event);
}

#[derive(Debug)]
struct CompareOnScreen(WebPkiSupportedAlgorithms);

impl ServerCertVerifier for CompareOnScreen {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        // The certificate is session-local. Both users authenticate the TLS channel
        // by comparing its exporter before any manifests or file data are exchanged.
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("TLS 1.3 is required".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, signature, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

trait Io: Read + Write {}
impl<T: Read + Write> Io for T {}

fn secure(mut tcp: TcpStream, client: bool) -> Result<(Box<dyn Io>, String)> {
    tcp.set_read_timeout(Some(RESPONSE_TIMEOUT))?;
    tcp.set_write_timeout(Some(RESPONSE_TIMEOUT))?;
    tcp.set_nodelay(true)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let label = b"EXPORTER-Channel-Binding";
    let (stream, material): (Box<dyn Io>, [u8; 32]) = if client {
        let verifier = CompareOnScreen(provider.signature_verification_algorithms);
        let mut config = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        config.alpn_protocols = vec![PROTOCOL.to_vec()];
        let mut conn = ClientConnection::new(
            Arc::new(config),
            ServerName::try_from("usb-transfer.local")?,
        )?;
        conn.complete_io(&mut tcp).context("TLS handshake failed")?;
        ensure!(
            conn.alpn_protocol() == Some(PROTOCOL),
            "Incompatible transfer protocol"
        );
        let material = conn.export_keying_material([0; 32], label, Some(&[]))?;
        (Box::new(StreamOwned::new(conn, tcp)), material)
    } else {
        let key = generate_simple_self_signed(vec!["usb-transfer.local".into()])?;
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(
                vec![key.cert.der().clone()],
                PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
            )?;
        config.alpn_protocols = vec![PROTOCOL.to_vec()];
        let mut conn = ServerConnection::new(Arc::new(config))?;
        conn.complete_io(&mut tcp).context("TLS handshake failed")?;
        ensure!(
            conn.alpn_protocol() == Some(PROTOCOL),
            "Incompatible transfer protocol"
        );
        let material = conn.export_keying_material([0; 32], label, Some(&[]))?;
        (Box::new(StreamOwned::new(conn, tcp)), material)
    };
    Ok((stream, pairing_code(material[..10].try_into().unwrap())))
}

fn pairing_code(material: [u8; 10]) -> String {
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let bits = u128::from_be_bytes([
        0,
        0,
        0,
        0,
        0,
        0,
        material[0],
        material[1],
        material[2],
        material[3],
        material[4],
        material[5],
        material[6],
        material[7],
        material[8],
        material[9],
    ]);
    let mut code = String::with_capacity(19);
    for digit in (0..16).rev() {
        let value = (bits >> (digit * 5)) & 31;
        code.push(ALPHABET[value as usize] as char);
        if digit > 0 && digit % 4 == 0 {
            code.push('-');
        }
    }
    code
}

fn confirm(
    stream: &mut impl Io,
    requests: &mpsc::Receiver<Command>,
    updates: &mpsc::Sender<Event>,
) -> Result<bool> {
    let deadline = Instant::now() + APPROVAL_TIMEOUT;
    let mut local = 0;
    loop {
        match requests.try_recv() {
            Ok(Command::Confirm(true)) => local = 1,
            Ok(Command::Confirm(false)) => local = 2,
            Ok(Command::Disconnect) | Err(mpsc::TryRecvError::Disconnected) => return Ok(false),
            Ok(_) => emit(
                updates,
                Event::Error("Confirm the pairing code first.".into()),
            ),
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if Instant::now() >= deadline {
            local = 2;
        }
        stream.write_all(&[local])?;
        stream.flush()?;
        let mut peer = [0];
        stream.read_exact(&mut peer)?;
        match (local, peer[0]) {
            (2, _) => return Ok(false),
            (_, 2) => bail!("The other computer declined or timed out while pairing."),
            (1, 1) => return Ok(true),
            (_, 0 | 1) => thread::sleep(POLL_INTERVAL),
            _ => bail!("Invalid pairing response"),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Frame {
    Idle,
    Offer(Manifest),
    Waiting,
    Accept,
    Decline,
    Receipt(Summary),
    ReceiptConfirmed,
}

fn write_frame(stream: &mut impl Write, frame: Frame) -> Result<()> {
    let encoded = serde_json::to_vec(&frame)?;
    ensure!(
        encoded.len() <= MAX_FRAME,
        "Transfer has too many entries for one batch"
    );
    stream.write_all(&(encoded.len() as u32).to_be_bytes())?;
    stream.write_all(&encoded)?;
    stream.flush()?;
    Ok(())
}

fn read_frame(stream: &mut impl Read) -> Result<Frame> {
    let mut prefix = [0; 4];
    stream
        .read_exact(&mut prefix)
        .context("Cable session ended or the peer stopped responding")?;
    let size = u32::from_be_bytes(prefix) as usize;
    ensure!(
        (1..=MAX_FRAME).contains(&size),
        "Invalid transfer frame size"
    );
    let mut payload = vec![0; size];
    stream.read_exact(&mut payload)?;
    Ok(serde_json::from_slice(&payload).context("Invalid transfer frame")?)
}

fn progress(updates: &mpsc::Sender<Event>, receiving: bool) -> impl FnMut(u64, u64) + '_ {
    let mut last = None;
    move |done, total| {
        let now = Instant::now();
        if done == total || last.is_none_or(|at| now - at >= POLL_INTERVAL) {
            emit(
                updates,
                Event::Progress {
                    receiving,
                    done,
                    total,
                },
            );
            last = Some(now);
        }
    }
}

fn outgoing(stream: &mut impl Io, batch: Prepared, updates: &mpsc::Sender<Event>) -> Result<()> {
    write_frame(stream, Frame::Offer(batch.manifest.clone()))?;
    loop {
        match read_frame(stream)? {
            Frame::Accept => break,
            Frame::Waiting => {}
            Frame::Decline => {
                emit(updates, Event::Error("The peer declined the batch.".into()));
                return Ok(());
            }
            _ => bail!("Unexpected response to the transfer offer"),
        }
    }
    transfer::send(stream, &batch, progress(updates, false)).context("Sending files failed")?;
    stream.flush()?;
    let Frame::Receipt(mut summary) = read_frame(stream)? else {
        bail!("Missing transfer receipt");
    };
    for line in summary.renamed.iter_mut().chain(&mut summary.skipped) {
        *line = transfer::display_name(line);
    }
    emit(
        updates,
        Event::Completed {
            receiving: false,
            summary,
        },
    );
    write_frame(stream, Frame::ReceiptConfirmed)?;
    Ok(())
}

fn incoming(
    stream: &mut impl Io,
    manifest: Manifest,
    destination: &Path,
    requests: &mpsc::Receiver<Command>,
    updates: &mpsc::Sender<Event>,
    preparation: &mut Preparation,
) -> Result<bool> {
    let (names, bytes) = transfer::describe(&manifest)?;
    emit(updates, Event::Incoming { names, bytes });
    let deadline = Instant::now() + APPROVAL_TIMEOUT;
    let to = loop {
        match requests.recv_timeout(Duration::from_secs(1)) {
            Ok(Command::Approve(Some(to))) => {
                let to = if to.as_os_str().is_empty() {
                    destination
                } else {
                    &to
                };
                if to.is_dir() {
                    break to.to_path_buf();
                }
                emit(
                    updates,
                    Event::Error("Choose an existing destination folder.".into()),
                );
            }
            Ok(Command::Approve(None)) => {
                write_frame(stream, Frame::Decline)?;
                emit(updates, Event::IncomingClosed { accepted: false });
                return Ok(true);
            }
            Ok(Command::Disconnect) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Ok(false);
            }
            Ok(Command::Send(paths)) => preparation.queue(paths, updates),
            Ok(_) => emit(
                updates,
                Event::Status("Respond to the incoming batch first.".into()),
            ),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if Instant::now() >= deadline {
            write_frame(stream, Frame::Decline)?;
            emit(updates, Event::IncomingClosed { accepted: false });
            emit(updates, Event::Error("The incoming offer expired.".into()));
            return Ok(true);
        }
        write_frame(stream, Frame::Waiting)?;
    };
    write_frame(stream, Frame::Accept)?;
    emit(updates, Event::IncomingClosed { accepted: true });
    let summary = transfer::receive(stream, &manifest, &to, progress(updates, true))
        .context("Receiving files failed")?;
    let receipt = (|| {
        write_frame(stream, Frame::Receipt(summary.clone()))?;
        let Frame::ReceiptConfirmed = read_frame(stream)? else {
            bail!("Missing transfer receipt confirmation");
        };
        transfer::confirm_receipt(&to, &manifest)
    })();
    emit(
        updates,
        Event::Completed {
            receiving: true,
            summary,
        },
    );
    receipt?;
    Ok(true)
}

#[derive(Default)]
struct Preparation {
    pending: Option<mpsc::Receiver<Result<Prepared>>>,
    cancelled: Arc<AtomicBool>,
}

impl Preparation {
    fn queue(&mut self, paths: Vec<PathBuf>, updates: &mpsc::Sender<Event>) {
        if self.pending.is_some() {
            emit(
                updates,
                Event::Status("A batch is already being prepared.".into()),
            );
            return;
        }
        let (ready, prepared) = mpsc::channel();
        let cancelled = Arc::clone(&self.cancelled);
        thread::spawn(move || {
            let _ = ready.send(transfer::prepare(&paths, &cancelled));
        });
        self.pending = Some(prepared);
        emit(updates, Event::Status("Preparing files…".into()));
    }

    fn take_ready(&mut self) -> Option<Result<Prepared>> {
        let result = match self.pending.as_ref()?.try_recv() {
            Ok(result) => result,
            Err(mpsc::TryRecvError::Disconnected) => {
                Err(anyhow::anyhow!("Source preparation ended unexpectedly"))
            }
            Err(mpsc::TryRecvError::Empty) => return None,
        };
        self.pending = None;
        Some(result)
    }
}

impl Drop for Preparation {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

fn run(
    tcp: TcpStream,
    client: bool,
    destination: &Path,
    requests: &mpsc::Receiver<Command>,
    updates: &mpsc::Sender<Event>,
) -> Result<()> {
    let (mut stream, code) = secure(tcp, client)?;
    emit(updates, Event::PairCode(code));
    if !confirm(&mut stream, requests, updates)? {
        return Ok(());
    }
    emit(updates, Event::Paired);

    let mut offer_turn = client;
    let mut preparation = Preparation::default();
    loop {
        match requests.try_recv() {
            Ok(Command::Send(paths)) => preparation.queue(paths, updates),
            Ok(Command::Disconnect) | Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
            Ok(_) | Err(mpsc::TryRecvError::Empty) => {}
        }
        if offer_turn {
            if let Some(result) = preparation.take_ready() {
                match result {
                    Ok(batch) => outgoing(&mut stream, batch, updates)?,
                    Err(error) => {
                        emit(updates, Event::Error(format!("{error:#}")));
                        write_frame(&mut stream, Frame::Idle)?;
                    }
                }
            } else {
                write_frame(&mut stream, Frame::Idle)?;
            }
        } else {
            match read_frame(&mut stream)? {
                Frame::Offer(manifest) => {
                    if !incoming(
                        &mut stream,
                        manifest,
                        destination,
                        requests,
                        updates,
                        &mut preparation,
                    )? {
                        return Ok(());
                    }
                }
                Frame::Idle => thread::sleep(POLL_INTERVAL),
                _ => bail!("Unexpected frame outside a transfer"),
            }
        }
        offer_turn = !offer_turn;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::net::TcpListener;
    use std::sync::mpsc::{Receiver, Sender};

    struct Endpoint {
        commands: Sender<Command>,
        events: Receiver<Event>,
        socket: TcpStream,
    }

    impl Endpoint {
        fn next<T>(&self, select: impl Fn(Event) -> Option<T>) -> T {
            loop {
                let event = self.events.recv_timeout(Duration::from_secs(30)).unwrap();
                match event {
                    Event::Error(error) => panic!("{error:#}"),
                    Event::Disconnected => panic!("Unexpected disconnect"),
                    _ => {
                        if let Some(value) = select(event) {
                            return value;
                        }
                    }
                }
            }
        }

        fn approve_batch(&self) {
            self.next(|event| matches!(event, Event::Incoming { .. }).then_some(()));
            self.commands
                .send(Command::Approve(Some(PathBuf::new())))
                .unwrap();
        }
    }

    impl Drop for Endpoint {
        fn drop(&mut self) {
            let _ = self.socket.shutdown(Shutdown::Both);
        }
    }

    fn endpoint(socket: TcpStream, client: bool, to: PathBuf) -> Endpoint {
        let (commands, requests) = mpsc::channel();
        let (updates, events) = mpsc::channel();
        let worker_socket = socket.try_clone().unwrap();
        thread::spawn(move || {
            if let Err(error) = run(worker_socket, client, &to, &requests, &updates) {
                emit(&updates, Event::Error(format!("{error:#}")));
            }
            emit(&updates, Event::Disconnected);
        });
        Endpoint {
            commands,
            events,
            socket,
        }
    }

    fn connected(to: &Path) -> (Endpoint, Endpoint) {
        let left_to = to.join("left-output");
        let right_to = to.join("right-output");
        fs::create_dir(&left_to).unwrap();
        fs::create_dir(&right_to).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        (
            endpoint(client, true, left_to),
            endpoint(server, false, right_to),
        )
    }

    fn pair(left: &Endpoint, right: &Endpoint) {
        fn take_code(event: Event) -> Option<String> {
            match event {
                Event::PairCode(code) => Some(code),
                _ => None,
            }
        }
        assert_eq!(left.next(take_code), right.next(take_code));
        left.commands.send(Command::Confirm(true)).unwrap();
        assert!(
            left.events
                .recv_timeout(Duration::from_millis(300))
                .is_err()
        );
        right.commands.send(Command::Confirm(true)).unwrap();
        for peer in [left, right] {
            peer.next(|event| matches!(event, Event::Paired).then_some(()));
        }
    }

    #[test]
    fn encrypted_loopback_transfers_files_and_a_folder_both_ways() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let folder = root.join("Travel");
        fs::create_dir_all(folder.join("empty")).unwrap();
        fs::write(folder.join("Résumé.txt"), b"left-hand data").unwrap();
        let right_file = root.join("right.txt");
        fs::write(&right_file, b"right-hand data").unwrap();
        let (left, right) = connected(root);
        fs::create_dir(root.join("right-output/Travel")).unwrap();
        fs::write(root.join("right-output/Travel/keep.txt"), b"original").unwrap();
        pair(&left, &right);

        left.commands.send(Command::Send(vec![folder])).unwrap();
        right.approve_batch();
        left.next(|event| {
            matches!(
                event,
                Event::Completed {
                    receiving: false,
                    ..
                }
            )
            .then_some(())
        });
        right
            .commands
            .send(Command::Send(vec![right_file]))
            .unwrap();
        left.approve_batch();
        right.next(|event| {
            matches!(
                event,
                Event::Completed {
                    receiving: false,
                    ..
                }
            )
            .then_some(())
        });

        assert_eq!(
            fs::read(root.join("right-output/Travel (2)/Résumé.txt")).unwrap(),
            b"left-hand data"
        );
        assert!(root.join("right-output/Travel (2)/empty").is_dir());
        assert_eq!(
            fs::read(root.join("right-output/Travel/keep.txt")).unwrap(),
            b"original"
        );
        assert_eq!(
            fs::read(root.join("left-output/right.txt")).unwrap(),
            b"right-hand data"
        );
    }

    #[test]
    #[ignore = "copies a 4 GiB sparse fixture; run explicitly in release mode"]
    fn encrypted_loopback_transfers_a_file_larger_than_four_gib() {
        use std::io::{Seek, SeekFrom};
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let file_path = root.join("large.bin");
        let size = (4u64 << 30) + 7;
        let mut source = fs::File::create(&file_path).unwrap();
        source.set_len(size).unwrap();
        source.seek(SeekFrom::End(-7)).unwrap();
        source.write_all(b"the end").unwrap();
        source.sync_all().unwrap();
        let (left, right) = connected(root);
        pair(&left, &right);
        left.commands.send(Command::Send(vec![file_path])).unwrap();
        right.approve_batch();
        left.next(|event| {
            matches!(
                event,
                Event::Completed {
                    receiving: false,
                    ..
                }
            )
            .then_some(())
        });
        let mut output = fs::File::open(root.join("right-output/large.bin")).unwrap();
        assert_eq!(output.metadata().unwrap().len(), size);
        output.seek(SeekFrom::End(-7)).unwrap();
        let mut tail = [0; 7];
        output.read_exact(&mut tail).unwrap();
        assert_eq!(&tail, b"the end");
    }
}
