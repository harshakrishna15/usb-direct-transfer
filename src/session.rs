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
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use transfer::{Destination, Manifest, OfferPreview, Prepared, ResumeVerification, Summary};

const PROTOCOL: &[u8] = b"usb-direct-transfer/1";
const PREVIEW_PROTOCOL: &[u8] = b"usb-direct-transfer/2";
// Peers on version 3 can still pair through the prehashed version 2 protocol.
const STREAM_PROTOCOL: &[u8] = b"usb-direct-transfer/4";
const PARALLEL_RESUME_PROTOCOL: &[u8] = b"usb-direct-transfer/5";
const MAX_FRAME: usize = 16 * 1024 * 1024;
const MIN_PREVIEW_BYTES: u64 = 32 * 1024 * 1024;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(180);
const POLL_INTERVAL: Duration = Duration::from_millis(150);
const OFFER_HEARTBEAT: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub enum Command {
    Confirm(bool),
    Send(Vec<PathBuf>),
    Approve(Option<PathBuf>),
    Disconnect,
}

#[derive(Clone, Copy, Debug)]
pub enum PreparationProgress {
    Scanning { files: usize, bytes: u64 },
    Checking { done: u64, total: u64 },
    CheckingSaved { done: u64, total: u64 },
}

#[derive(Debug)]
pub enum Event {
    Status(String),
    PairingPortBusy(String),
    PairCode(String),
    Paired,
    AwaitingApproval(bool),
    Incoming {
        names: Vec<String>,
        bytes: u64,
    },
    IncomingClosed {
        accepted: bool,
    },
    Preparing {
        receiving: bool,
        progress: PreparationProgress,
    },
    Progress {
        receiving: bool,
        done: u64,
        total: u64,
        sampled_at: Instant,
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
    fn close(&self, abort_transfer: bool) {
        self.cancelled.store(true, Ordering::Relaxed);
        if let Some(socket) = self.socket.lock().unwrap().take() {
            if abort_transfer {
                // Reset the connection on close so a peer blocked in a write
                // can stop even when its TCP send buffer is full.
                let _ = socket2::SockRef::from(&socket).set_linger(Some(Duration::ZERO));
            }
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
        self.control.close(false);
    }

    pub fn cancel_transfer(&self) {
        self.control.close(true);
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.disconnect();
    }
}

pub fn start(options: network::Options, destination: PathBuf) -> Session {
    start_with_notify(options, destination, || {})
}

/// Calls `notify` from a worker thread after each event is queued.
pub fn start_with_notify(
    options: network::Options,
    destination: PathBuf,
    notify: impl Fn() + Send + 'static,
) -> Session {
    let (commands, requests) = mpsc::channel();
    let (send, events) = mpsc::channel();
    let updates = Updates {
        send,
        notify: Arc::new(Mutex::new(notify)),
    };
    let control = Arc::new(ConnectionControl::default());
    let worker_control = Arc::clone(&control);
    thread::spawn(move || {
        let result = (|| {
            let (stream, client) =
                network::connect(&options, &worker_control.cancelled, |status| {
                    let event = match status {
                        network::ConnectStatus::Detail(message) => Event::Status(message),
                        network::ConnectStatus::PortBusy(message) => {
                            Event::PairingPortBusy(message)
                        }
                    };
                    emit(&updates, event);
                })?;
            worker_control.attach(&stream)?;
            let _link = network::watch(&stream)?;
            run(
                stream,
                client,
                &destination,
                &requests,
                &updates,
                &worker_control.cancelled,
            )
        })();
        if let Err(error) = result
            && !worker_control.cancelled.load(Ordering::Relaxed)
        {
            emit(&updates, Event::Error(format!("{error:#}")));
        }
        worker_control.close(false);
        emit(&updates, Event::Disconnected);
    });
    Session {
        commands,
        events,
        control,
    }
}

#[derive(Clone)]
struct Updates {
    send: mpsc::Sender<Event>,
    notify: Arc<Mutex<dyn Fn() + Send>>,
}

fn emit(updates: &Updates, event: Event) {
    if updates.send.send(event).is_ok() {
        (updates.notify.lock().unwrap())();
    }
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

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Protocol {
    #[default]
    Legacy,
    Preview,
    Streaming,
    ParallelResume,
}

impl Protocol {
    fn streaming(self) -> bool {
        matches!(self, Self::Streaming | Self::ParallelResume)
    }
}

fn secure(mut tcp: TcpStream, client: bool) -> Result<(Box<dyn Io>, String, Protocol)> {
    tcp.set_read_timeout(Some(RESPONSE_TIMEOUT))?;
    tcp.set_write_timeout(Some(RESPONSE_TIMEOUT))?;
    tcp.set_nodelay(true)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let label = b"EXPORTER-Channel-Binding";
    let versions = vec![
        PARALLEL_RESUME_PROTOCOL.to_vec(),
        STREAM_PROTOCOL.to_vec(),
        PREVIEW_PROTOCOL.to_vec(),
        PROTOCOL.to_vec(),
    ];
    let (stream, material, protocol): (Box<dyn Io>, [u8; 32], Protocol) = if client {
        let verifier = CompareOnScreen(provider.signature_verification_algorithms);
        let mut config = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        config.alpn_protocols = versions;
        let mut conn = ClientConnection::new(
            Arc::new(config),
            ServerName::try_from("usb-transfer.local")?,
        )?;
        conn.complete_io(&mut tcp).context("TLS handshake failed")?;
        let protocol = negotiated_protocol(conn.alpn_protocol())?;
        let material = conn.export_keying_material([0; 32], label, Some(&[]))?;
        (Box::new(StreamOwned::new(conn, tcp)), material, protocol)
    } else {
        let key = generate_simple_self_signed(vec!["usb-transfer.local".into()])?;
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(
                vec![key.cert.der().clone()],
                PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
            )?;
        config.alpn_protocols = versions;
        let mut conn = ServerConnection::new(Arc::new(config))?;
        conn.complete_io(&mut tcp).context("TLS handshake failed")?;
        let protocol = negotiated_protocol(conn.alpn_protocol())?;
        let material = conn.export_keying_material([0; 32], label, Some(&[]))?;
        (Box::new(StreamOwned::new(conn, tcp)), material, protocol)
    };
    Ok((
        stream,
        pairing_code(material[..10].try_into().unwrap()),
        protocol,
    ))
}

fn negotiated_protocol(protocol: Option<&[u8]>) -> Result<Protocol> {
    match protocol {
        Some(PARALLEL_RESUME_PROTOCOL) => Ok(Protocol::ParallelResume),
        Some(STREAM_PROTOCOL) => Ok(Protocol::Streaming),
        Some(PREVIEW_PROTOCOL) => Ok(Protocol::Preview),
        Some(PROTOCOL) => Ok(Protocol::Legacy),
        _ => bail!("Incompatible transfer protocol"),
    }
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
    updates: &Updates,
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
enum Frame<Batch = Manifest, Report = Summary> {
    Idle,
    Preview(OfferPreview),
    Offer(Batch),
    Waiting,
    Accept,
    AcceptPrehashed,
    Decline,
    Receipt(Report),
    ReceiptConfirmed,
}

struct FrameBuffer(Vec<u8>);

impl Write for FrameBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_FRAME + 4 - self.0.len() {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "Transfer metadata exceeds the 16 MiB batch limit. Try a smaller batch.",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_frame(frame: impl Serialize) -> Result<Vec<u8>> {
    let mut encoded = FrameBuffer(vec![0; 4]);
    serde_json::to_writer(&mut encoded, &frame)?;
    let size = (encoded.0.len() - 4) as u32;
    encoded.0[..4].copy_from_slice(&size.to_be_bytes());
    Ok(encoded.0)
}

fn frame_too_large(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<serde_json::Error>()
        .and_then(serde_json::Error::io_error_kind)
        == Some(io::ErrorKind::FileTooLarge)
}

fn write_frame(stream: &mut impl Write, frame: Frame) -> Result<()> {
    stream.write_all(&encode_frame(frame)?)?;
    stream.flush()?;
    Ok(())
}

fn write_receipt(stream: &mut impl Write, summary: &Summary) -> Result<()> {
    let encoded = match encode_frame(Frame::<Manifest, _>::Receipt(summary)) {
        Err(error) if frame_too_large(&error) => {
            let renamed = receipt_excerpt(&summary.renamed);
            let skipped = receipt_excerpt(&summary.skipped);
            encode_frame(Frame::<Manifest>::Receipt(Summary {
                additional_renamed: summary.renamed.len() - renamed.len(),
                additional_skipped: summary.skipped.len() - skipped.len(),
                renamed,
                skipped,
            }))
        }
        result => result,
    }?;
    stream.write_all(&encoded)?;
    stream.flush()?;
    Ok(())
}

fn receipt_excerpt(details: &[String]) -> Vec<String> {
    // Split the frame budget between the two lists. Allow up to six bytes
    // of JSON escaping per byte of text, plus quotes and a comma per item.
    let mut remaining = (MAX_FRAME - 1024) / 2;
    details
        .iter()
        .filter(|line| {
            let size = line.len().saturating_mul(6).saturating_add(3);
            if let Some(left) = remaining.checked_sub(size) {
                remaining = left;
                true
            } else {
                false
            }
        })
        .cloned()
        .collect()
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

fn progress(updates: &Updates, receiving: bool) -> impl FnMut(u64, u64) + '_ {
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
                    sampled_at: now,
                },
            );
            last = Some(now);
        }
    }
}

fn preparation_progress(
    updates: &Updates,
    receiving: bool,
) -> impl FnMut(PreparationProgress) + Send + '_ {
    let mut last = None;
    move |progress| {
        use std::mem::discriminant;

        let now = Instant::now();
        let milestone = matches!(progress, PreparationProgress::Scanning { files: 1, .. })
            || matches!(
                progress,
                PreparationProgress::Checking { done, total }
                | PreparationProgress::CheckingSaved { done, total } if done == total
            );
        if milestone
            || last.is_none_or(|(at, previous)| {
                discriminant(&previous) != discriminant(&progress) || now - at >= POLL_INTERVAL
            })
        {
            emit(
                updates,
                Event::Preparing {
                    receiving,
                    progress,
                },
            );
            last = Some((now, progress));
        }
    }
}

fn resume_verification(
    updates: &Updates,
    protocol: Protocol,
    receiving: bool,
) -> ResumeVerification<impl FnMut(u64, u64) + Send + '_> {
    let mut report = preparation_progress(updates, receiving);
    ResumeVerification {
        parallel: protocol == Protocol::ParallelResume,
        checked: move |done, total| report(PreparationProgress::CheckingSaved { done, total }),
    }
}

fn outgoing(
    stream: &mut impl Io,
    offer: OutgoingOffer,
    updates: &Updates,
    protocol: Protocol,
) -> Result<()> {
    let (frame, mut ready, preparation) = match offer {
        OutgoingOffer::Ready(mut offer) => (std::mem::take(&mut offer.frame), Some(offer), None),
        OutgoingOffer::Preview { frame, preparation } => (frame, None, Some(preparation)),
    };
    stream.write_all(&frame)?;
    stream.flush()?;
    drop(frame);
    emit(updates, Event::AwaitingApproval(true));
    let prehashed = loop {
        match read_frame(stream)? {
            Frame::Accept => break false,
            Frame::AcceptPrehashed
                if ready
                    .as_ref()
                    .is_some_and(|offer| offer.batch.manifest.is_streaming()) =>
            {
                break true;
            }
            Frame::Waiting => {
                if let Some(preparation) = &preparation {
                    if ready.is_none()
                        && let Some(result) =
                            receive_preparation(&preparation.ready, Duration::ZERO)
                    {
                        match result {
                            Ok(offer) => ready = Some(offer),
                            Err(error) => {
                                emit(updates, Event::AwaitingApproval(false));
                                return withdraw_offer(stream, updates, error);
                            }
                        }
                    }
                    write_frame(stream, Frame::Waiting)?;
                }
            }
            Frame::Decline => {
                emit(updates, Event::AwaitingApproval(false));
                emit(updates, Event::Error("The peer declined the batch.".into()));
                return Ok(());
            }
            _ => bail!("Unexpected response to the transfer offer"),
        }
    };
    emit(updates, Event::AwaitingApproval(false));
    let PreparedOffer { batch, frame } = match ready {
        Some(offer) => offer,
        None => {
            let preparation = preparation
                .as_ref()
                .context("Missing transfer preparation")?;
            let Some(offer) = wait_for_preparation(stream, updates, preparation)? else {
                return Ok(());
            };
            offer
        }
    };
    if preparation.is_some() {
        stream.write_all(&frame)?;
        stream.flush()?;
    }
    drop(frame);
    let batch = if prehashed {
        let preparation = prepare_legacy_retry(batch, updates);
        let Some(PreparedOffer { batch, frame }) =
            wait_for_preparation(stream, updates, &preparation)?
        else {
            return Ok(());
        };
        stream.write_all(&frame)?;
        stream.flush()?;
        batch
    } else {
        batch
    };
    transfer::send(
        stream,
        &batch,
        resume_verification(updates, protocol, false),
        progress(updates, false),
    )
    .context("Sending files failed")?;
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

fn wait_for_preparation(
    stream: &mut impl Write,
    updates: &Updates,
    preparation: &PendingOffer,
) -> Result<Option<PreparedOffer>> {
    loop {
        match receive_preparation(&preparation.ready, OFFER_HEARTBEAT) {
            Some(Ok(offer)) => return Ok(Some(offer)),
            Some(Err(error)) => {
                withdraw_offer(stream, updates, error)?;
                return Ok(None);
            }
            None => write_frame(stream, Frame::Waiting)?,
        }
    }
}

fn prepare_legacy_retry(mut batch: Prepared, updates: &Updates) -> PendingOffer {
    let (complete, ready) = mpsc::channel();
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = Arc::clone(&cancelled);
    let updates = updates.clone();
    thread::spawn(move || {
        let mut report = preparation_progress(&updates, false);
        let result = batch
            .finish_hashes(&worker_cancelled, |done, total| {
                report(PreparationProgress::Checking { done, total });
            })
            .and_then(|()| PreparedOffer::encode(batch));
        let _ = complete.send(result);
    });
    PendingOffer { ready, cancelled }
}

fn withdraw_offer(stream: &mut impl Write, updates: &Updates, error: anyhow::Error) -> Result<()> {
    emit(updates, Event::Error(format!("{error:#}")));
    write_frame(stream, Frame::Decline)
}

enum IncomingOffer {
    Ready(Manifest),
    Preview(OfferPreview),
}

fn incoming(
    stream: &mut impl Io,
    offer: IncomingOffer,
    destination: &Path,
    requests: &mpsc::Receiver<Command>,
    updates: &Updates,
    preparation: &mut Preparation,
    cancelled: &AtomicBool,
) -> Result<bool> {
    let (names, bytes) = match &offer {
        IncomingOffer::Ready(manifest) => transfer::describe(manifest)?,
        IncomingOffer::Preview(preview) => (preview.display_names()?, preview.bytes),
    };
    emit(updates, Event::Incoming { names, bytes });
    let deadline = Instant::now() + APPROVAL_TIMEOUT;
    let to = loop {
        match requests.recv_timeout(OFFER_HEARTBEAT) {
            Ok(Command::Approve(Some(to))) => {
                let to = if to.as_os_str().is_empty() {
                    destination
                } else {
                    &to
                };
                match Destination::open(to) {
                    Ok(to) => break to,
                    Err(error) => emit(updates, Event::Error(format!("{error:#}"))),
                }
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
        if matches!(&offer, IncomingOffer::Preview(_)) {
            match read_frame(stream)? {
                Frame::Waiting => {}
                Frame::Decline => {
                    emit(updates, Event::IncomingClosed { accepted: false });
                    emit(
                        updates,
                        Event::Error("The sender could not prepare the batch.".into()),
                    );
                    return Ok(true);
                }
                _ => bail!("Unexpected response while reviewing the offer"),
            }
        }
    };
    let prehashed = match &offer {
        IncomingOffer::Ready(manifest) => to.has_prehashed_progress(manifest)?,
        IncomingOffer::Preview(_) => false,
    };
    write_frame(
        stream,
        if prehashed {
            Frame::AcceptPrehashed
        } else {
            Frame::Accept
        },
    )?;
    emit(updates, Event::IncomingClosed { accepted: true });
    let manifest = match offer {
        IncomingOffer::Ready(initial) if prehashed => {
            let Some(manifest) = read_prehashed_offer(stream, updates)? else {
                return Ok(true);
            };
            initial.verify_prehashed_offer(&manifest)?;
            manifest
        }
        IncomingOffer::Ready(manifest) => manifest,
        IncomingOffer::Preview(preview) => {
            let Some(manifest) = read_prehashed_offer(stream, updates)? else {
                return Ok(true);
            };
            preview.verify(&manifest)?;
            manifest
        }
    };
    let summary = transfer::receive(
        stream,
        &manifest,
        &to,
        cancelled,
        resume_verification(updates, preparation.protocol, true),
        progress(updates, true),
    )
    .context("Receiving files failed")?;
    let receipt = (|| {
        write_receipt(stream, &summary)?;
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

fn read_prehashed_offer(stream: &mut impl Read, updates: &Updates) -> Result<Option<Manifest>> {
    loop {
        match read_frame(stream)? {
            Frame::Waiting => {}
            Frame::Offer(manifest) if !manifest.is_streaming() => return Ok(Some(manifest)),
            Frame::Decline => {
                emit(
                    updates,
                    Event::Error("The sender could not prepare the batch.".into()),
                );
                return Ok(None);
            }
            _ => bail!("Unexpected response while preparing the transfer"),
        }
    }
}

struct PreparedOffer {
    batch: Prepared,
    frame: Vec<u8>,
}

impl PreparedOffer {
    fn encode(mut batch: Prepared) -> Result<Self> {
        let frame = match encode_frame(Frame::<&Manifest>::Offer(&batch.manifest)) {
            Err(error) if frame_too_large(&error) && batch.omit_checkpoints() => {
                encode_frame(Frame::<&Manifest>::Offer(&batch.manifest))
            }
            result => result,
        }?;
        Ok(Self { batch, frame })
    }
}

enum OutgoingOffer {
    Ready(PreparedOffer),
    Preview {
        frame: Vec<u8>,
        preparation: PendingOffer,
    },
}

struct PendingOffer {
    ready: mpsc::Receiver<Result<PreparedOffer>>,
    cancelled: Arc<AtomicBool>,
}

impl Drop for PendingOffer {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

fn receive_preparation<T>(ready: &mpsc::Receiver<Result<T>>, wait: Duration) -> Option<Result<T>> {
    match ready.recv_timeout(wait) {
        Ok(result) => Some(result),
        Err(mpsc::RecvTimeoutError::Disconnected) => Some(Err(anyhow::anyhow!(
            "Source preparation ended unexpectedly"
        ))),
        Err(mpsc::RecvTimeoutError::Timeout) => None,
    }
}

#[derive(Default)]
struct Preparation {
    pending: Option<mpsc::Receiver<Result<OutgoingOffer>>>,
    cancelled: Arc<AtomicBool>,
    protocol: Protocol,
}

impl Preparation {
    fn queue(&mut self, paths: Vec<PathBuf>, updates: &Updates) {
        if self.pending.is_some() {
            emit(
                updates,
                Event::Status("A batch is already being prepared.".into()),
            );
            return;
        }
        let (ready, prepared) = mpsc::channel();
        self.cancelled = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::clone(&self.cancelled);
        let protocol = self.protocol;
        self.pending = Some(prepared);
        emit(updates, Event::Status("Preparing files…".into()));
        let updates = updates.clone();
        thread::spawn(move || {
            let mut report = preparation_progress(&updates, false);
            let mut deferred = None;
            let offer = (|| {
                let mut batch = transfer::scan_batch(&paths, &cancelled, |files, bytes| {
                    report(PreparationProgress::Scanning { files, bytes });
                })?;
                if protocol.streaming() && batch.bytes >= MIN_PREVIEW_BYTES {
                    batch.enable_streaming(&paths)?;
                } else {
                    if protocol == Protocol::Preview && batch.bytes >= MIN_PREVIEW_BYTES {
                        let preview = batch.preview();
                        let frame = encode_frame(Frame::<Manifest>::Preview(preview))?;
                        let (complete, result) = mpsc::channel();
                        let preparation = PendingOffer {
                            ready: result,
                            cancelled: Arc::clone(&cancelled),
                        };
                        ready
                            .send(Ok(OutgoingOffer::Preview { frame, preparation }))
                            .map_err(|_| anyhow::anyhow!("Transfer preparation canceled"))?;
                        deferred = Some(complete);
                    }
                    batch.finish_hashes(&cancelled, |done, total| {
                        report(PreparationProgress::Checking { done, total });
                    })?;
                }
                PreparedOffer::encode(batch)
            })();
            if let Some(complete) = deferred {
                let _ = complete.send(offer);
            } else {
                let _ = ready.send(offer.map(OutgoingOffer::Ready));
            }
        });
    }

    fn take_ready(&mut self, deadline: Instant) -> Option<Result<OutgoingOffer>> {
        let wait = deadline.saturating_duration_since(Instant::now());
        let result = receive_preparation(self.pending.as_ref()?, wait)?;
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
    updates: &Updates,
    cancelled: &AtomicBool,
) -> Result<()> {
    let (mut stream, code, protocol) = secure(tcp, client)?;
    emit(updates, Event::PairCode(code));
    if !confirm(&mut stream, requests, updates)? {
        return Ok(());
    }
    emit(updates, Event::Paired);

    let mut offer_turn = client;
    let mut preparation = Preparation {
        pending: None,
        cancelled: Arc::new(AtomicBool::new(false)),
        protocol,
    };
    while !cancelled.load(Ordering::Relaxed) {
        // Keep the idle-turn limit while waking promptly for a local
        // command or a prepared batch.
        let deadline = Instant::now() + POLL_INTERVAL;
        let wait = if offer_turn && preparation.pending.is_none() {
            POLL_INTERVAL
        } else {
            Duration::ZERO
        };
        match requests.recv_timeout(wait) {
            Ok(Command::Send(paths)) => preparation.queue(paths, updates),
            Ok(Command::Disconnect) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
            Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if offer_turn {
            if let Some(result) = preparation.take_ready(deadline) {
                match result {
                    Ok(batch) => outgoing(&mut stream, batch, updates, protocol)?,
                    Err(error) => {
                        emit(updates, Event::Error(format!("{error:#}")));
                        write_frame(&mut stream, Frame::Idle)?;
                    }
                }
            } else {
                write_frame(&mut stream, Frame::Idle)?;
            }
        } else {
            let offer = match read_frame(&mut stream)? {
                Frame::Offer(manifest) if !manifest.is_streaming() || protocol.streaming() => {
                    Some(IncomingOffer::Ready(manifest))
                }
                Frame::Preview(preview) if protocol != Protocol::Legacy => {
                    Some(IncomingOffer::Preview(preview))
                }
                Frame::Idle => None,
                _ => bail!("Unexpected frame outside a transfer"),
            };
            if let Some(offer) = offer
                && !incoming(
                    &mut stream,
                    offer,
                    destination,
                    requests,
                    updates,
                    &mut preparation,
                    cancelled,
                )?
            {
                return Ok(());
            }
        }
        offer_turn = !offer_turn;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::net::TcpListener;
    use std::sync::mpsc::{Receiver, Sender};

    #[test]
    fn queues_session_events_before_waking_the_ui() {
        let (send, events) = mpsc::channel();
        let (notified, observed) = mpsc::channel();
        let updates = Updates {
            send,
            notify: Arc::new(Mutex::new(move || {
                notified.send(events.try_recv().unwrap()).unwrap();
            })),
        };
        emit(&updates, Event::Paired);
        assert!(matches!(observed.try_recv(), Ok(Event::Paired)));
    }

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
        let (send, events) = mpsc::channel();
        let updates = Updates {
            send,
            notify: Arc::new(Mutex::new(|| {})),
        };
        let worker_socket = socket.try_clone().unwrap();
        thread::spawn(move || {
            if let Err(error) = run(
                worker_socket,
                client,
                &to,
                &requests,
                &updates,
                &AtomicBool::new(false),
            ) {
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

    fn loopback() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }

    fn connected(to: &Path) -> (Endpoint, Endpoint) {
        let left_to = to.join("left-output");
        let right_to = to.join("right-output");
        fs::create_dir_all(&left_to).unwrap();
        fs::create_dir_all(&right_to).unwrap();
        let (client, server) = loopback();
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
    fn reports_files_found_and_hash_progress_before_copying() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source = temp.path().join("example.bin");
        let bytes = 2 * 1024 * 1024;
        fs::File::create(&source)?.set_len(bytes)?;
        let (left, right) = connected(temp.path());
        pair(&left, &right);

        left.commands.send(Command::Send(vec![source]))?;
        let found = left.next(|event| match event {
            Event::Preparing {
                receiving: false,
                progress: PreparationProgress::Scanning { files: 1, bytes },
            } => Some(bytes),
            _ => None,
        });
        assert_eq!(found, bytes);
        for expected in [0, bytes] {
            let sample = left.next(|event| match event {
                Event::Preparing {
                    receiving: false,
                    progress: PreparationProgress::Checking { done, total },
                } if done == expected => Some(total),
                Event::AwaitingApproval(true) => panic!("Missing preparation progress"),
                _ => None,
            });
            assert_eq!(sample, bytes);
        }
        left.next(|event| matches!(event, Event::AwaitingApproval(true)).then_some(()));
        right.approve_batch();
        right.next(|event| {
            matches!(
                event,
                Event::Completed {
                    receiving: true,
                    ..
                }
            )
            .then_some(())
        });
        Ok(())
    }

    #[test]
    fn declining_an_early_offer_cancels_pending_preparation() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source = temp.path().join("archive.bin");
        fs::write(&source, b"payload")?;
        let mut preview = None;
        transfer::prepare_with_preview(&[source], &AtomicBool::new(false), |offer| {
            preview = Some(offer);
            Ok(())
        })?;
        let frame = encode_frame(Frame::<Manifest>::Preview(preview.unwrap()))?;
        let (mut sender, mut receiver) = loopback();
        sender.set_read_timeout(Some(Duration::from_secs(5)))?;
        receiver.set_read_timeout(Some(Duration::from_secs(5)))?;
        let (_complete, ready) = mpsc::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let preparation = PendingOffer {
            ready,
            cancelled: Arc::clone(&cancelled),
        };
        let (send, _events) = mpsc::channel();
        let updates = Updates {
            send,
            notify: Arc::new(Mutex::new(|| {})),
        };
        thread::scope(|scope| -> Result<()> {
            let outgoing = scope.spawn(move || {
                outgoing(
                    &mut sender,
                    OutgoingOffer::Preview { frame, preparation },
                    &updates,
                    Protocol::Preview,
                )
            });
            assert!(matches!(read_frame(&mut receiver)?, Frame::Preview(_)));
            write_frame(&mut receiver, Frame::Waiting)?;
            assert!(matches!(read_frame(&mut receiver)?, Frame::Waiting));
            write_frame(&mut receiver, Frame::Decline)?;
            outgoing.join().unwrap()
        })?;
        assert!(cancelled.load(Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn unavailable_save_folder_keeps_the_offer_open() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source = temp.path().join("photo.jpg");
        fs::write(&source, b"picture")?;
        let unavailable = temp.path().join("unavailable");
        fs::create_dir_all(unavailable.join(".usb-direct-transfer"))?;

        let (left, right) = connected(temp.path());
        pair(&left, &right);
        left.commands.send(Command::Send(vec![source]))?;
        right.next(|event| matches!(event, Event::Incoming { .. }).then_some(()));
        right.commands.send(Command::Approve(Some(unavailable)))?;
        let error = loop {
            match right.events.recv_timeout(Duration::from_secs(30))? {
                Event::Error(error) => break error,
                Event::IncomingClosed { .. } | Event::Disconnected => {
                    bail!("An unavailable destination closed the offer")
                }
                _ => {}
            }
        };
        assert!(error.contains("Unrecognized staging folder"), "{error}");

        right
            .commands
            .send(Command::Approve(Some(PathBuf::new())))?;
        for (peer, receiving_side) in [(&right, true), (&left, false)] {
            peer.next(|event| {
                matches!(event, Event::Completed { receiving, .. } if receiving == receiving_side)
                    .then_some(())
            });
        }
        assert_eq!(
            fs::read(temp.path().join("right-output/photo.jpg"))?,
            b"picture"
        );
        Ok(())
    }

    #[test]
    fn large_receipt_preserves_counts_and_can_be_read_by_older_peers() -> Result<()> {
        #[derive(Deserialize)]
        struct OldSummary {
            renamed: Vec<String>,
            skipped: Vec<String>,
        }

        #[derive(Deserialize)]
        enum OldFrame {
            Receipt(OldSummary),
        }

        let original_name = "Catalog export ".repeat(15) + "daily records and photos";
        let summary = Summary {
            renamed: vec![format!("{original_name} -> {original_name} (2)"); 34_000],
            skipped: vec![format!("{original_name} (symbolic link)"); 8_000],
            ..Summary::default()
        };
        assert!(frame_too_large(
            &encode_frame(Frame::<Manifest, _>::Receipt(&summary)).unwrap_err()
        ));

        let mut wire = Vec::new();
        write_receipt(&mut wire, &summary)?;
        let Frame::Receipt(report) = read_frame(&mut wire.as_slice())? else {
            bail!("Missing receipt")
        };
        assert!(report.additional_renamed > 0);
        assert!(report.additional_skipped > 0);
        assert_eq!(
            report.renamed.len() + report.additional_renamed,
            summary.renamed.len()
        );
        assert_eq!(
            report.skipped.len() + report.additional_skipped,
            summary.skipped.len()
        );
        assert_eq!(report.renamed.first(), summary.renamed.first());
        assert_eq!(report.skipped.first(), summary.skipped.first());
        let OldFrame::Receipt(old_report) = serde_json::from_slice(&wire[4..])?;
        assert_eq!(old_report.renamed, report.renamed);
        assert_eq!(old_report.skipped, report.skipped);
        Ok(())
    }

    #[test]
    fn oversized_offer_does_not_end_the_session() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let source = temp.path().join("photos");
        let mut folder = source.clone();
        for _ in 0..6 {
            folder.push("a".repeat(120));
        }
        fs::create_dir_all(&folder)?;
        for index in 0..20_000 {
            fs::File::create(folder.join(format!("photo-{index:05}.jpg")))?;
        }
        let small = temp.path().join("small.txt");
        fs::write(&small, b"ready")?;
        let (left, right) = connected(temp.path());
        pair(&left, &right);

        left.commands.send(Command::Send(vec![source]))?;
        let error = loop {
            match left.events.recv_timeout(Duration::from_secs(30))? {
                Event::Error(error) => break error,
                Event::Disconnected => bail!("The session ended before the batch was rejected"),
                _ => {}
            }
        };
        assert!(error.contains("batch"), "{error}");

        left.commands.send(Command::Send(vec![small]))?;
        right.approve_batch();
        right.next(|event| {
            matches!(
                event,
                Event::Completed {
                    receiving: true,
                    ..
                }
            )
            .then_some(())
        });
        assert_eq!(
            fs::read(temp.path().join("right-output/small.txt"))?,
            b"ready"
        );
        Ok(())
    }

    fn receive_until_canceled(
        batch: &Prepared,
        output: &Path,
        cancel_at: impl Fn(u64, u64) -> bool,
    ) -> Result<()> {
        let (sender, receiver) = loopback();
        let sender_socket = sender.try_clone()?;
        let (finished, outcome) = mpsc::channel();
        let control = ConnectionControl::default();
        control.attach(&receiver)?;
        thread::scope(|scope| -> Result<()> {
            scope.spawn(|| {
                let result = secure(sender, true).and_then(|(mut stream, _, protocol)| {
                    transfer::send(
                        &mut stream,
                        batch,
                        ResumeVerification::silent(protocol == Protocol::ParallelResume),
                        |_, _| {},
                    )
                });
                let _ = finished.send(result);
            });
            let (mut stream, _, protocol) = secure(receiver, false)?;
            let destination = Destination::open(output)?;
            let received = transfer::receive(
                &mut stream,
                &batch.manifest,
                &destination,
                &control.cancelled,
                ResumeVerification::silent(protocol == Protocol::ParallelResume),
                |done, total| {
                    if cancel_at(done, total) {
                        control.close(true);
                    }
                },
            );
            drop(stream);
            let sent = outcome.recv_timeout(Duration::from_secs(5));
            if sent.is_err() {
                let _ = sender_socket.shutdown(Shutdown::Both);
            }
            assert!(control.cancelled.load(Ordering::Relaxed));
            assert!(received.is_err());
            assert!(
                sent.context("Sender kept writing after the download was canceled")?
                    .is_err()
            );
            Ok(())
        })
    }

    #[test]
    fn canceling_a_download_preserves_verified_files_and_checkpoints_for_retry() -> Result<()> {
        retry_after_cancel(false, false)
    }

    #[test]
    fn streaming_retry_reuses_the_saved_prefix() -> Result<()> {
        retry_after_cancel(true, false)
    }

    #[test]
    fn streaming_retry_keeps_checkpoints_before_damaged_data() -> Result<()> {
        retry_after_cancel(true, true)
    }

    fn retry_after_cancel(streaming: bool, damaged_prefix: bool) -> Result<()> {
        use std::io::{Seek, SeekFrom};

        let temp = tempfile::tempdir()?;
        let root = temp.path();
        let folder = root.join("batch");
        let output = root.join("right-output");
        fs::create_dir(&folder)?;
        fs::create_dir(&output)?;
        let first = b"already verified";
        fs::write(folder.join("a.txt"), first)?;
        let large_path = folder.join("b.bin");
        let large_size = 64 * 1024 * 1024;
        let checkpoint = 16 * 1024 * 1024;
        let mut large = fs::File::create(&large_path)?;
        large.set_len(large_size)?;
        large.write_all(b"first block")?;
        large.seek(SeekFrom::Start(checkpoint))?;
        large.write_all(b"second block")?;
        large.seek(SeekFrom::End(-4))?;
        large.write_all(b"tail")?;
        drop(large);
        let paths = std::slice::from_ref(&folder);
        let cancelled = AtomicBool::new(false);
        let batch = if streaming {
            let mut batch = transfer::scan_batch(paths, &cancelled, |_, _| {})?;
            batch.enable_streaming(paths)?;
            batch
        } else {
            transfer::prepare(paths, &cancelled)?
        };

        let stop = if damaged_prefix { 52 } else { 20 } * 1024 * 1024;
        receive_until_canceled(&batch, &output, |done, total| {
            done >= first.len() as u64 + stop && done < total
        })?;
        assert!(!output.join("batch").exists());
        let pending = transfer::pending(&output)?;
        assert_eq!(pending[0].bytes, first.len() as u64);
        assert!(pending[0].partial_bytes >= stop);
        assert!(pending[0].partial_bytes < large_size);
        if damaged_prefix {
            let mut partial = fs::OpenOptions::new().write(true).open(
                output
                    .join(".usb-direct-transfer")
                    .join(&pending[0].id)
                    .join(".resume-2"),
            )?;
            partial.seek(SeekFrom::Start(2 * checkpoint))?;
            partial.write_all(b"damaged block")?;
        }

        let (left, right) = connected(root);
        pair(&left, &right);
        left.commands.send(Command::Send(vec![folder]))?;
        right.approve_batch();
        let checked = if streaming {
            right.next(|event| match event {
                Event::Preparing {
                    receiving: true,
                    progress: PreparationProgress::CheckingSaved { done, total },
                } if done == total => Some(done),
                Event::Progress {
                    receiving: true, ..
                } => {
                    panic!("Missing saved-data verification progress");
                }
                _ => None,
            })
        } else {
            0
        };
        let reused = right.next(|event| match event {
            Event::Progress {
                receiving: true,
                done,
                ..
            } => Some(done),
            _ => None,
        });
        assert_eq!(
            reused,
            first.len() as u64
                + if damaged_prefix {
                    2 * checkpoint
                } else {
                    checkpoint
                }
        );
        if streaming {
            assert_eq!(
                checked,
                first.len() as u64
                    + if damaged_prefix {
                        3 * checkpoint
                    } else {
                        checkpoint
                    },
            );
        }
        right.next(|event| {
            matches!(
                event,
                Event::Completed {
                    receiving: true,
                    ..
                }
            )
            .then_some(())
        });
        assert_eq!(fs::read(output.join("batch/a.txt"))?, first);
        assert_eq!(fs::read(output.join("batch/b.bin"))?, fs::read(large_path)?);
        assert!(transfer::pending(&output)?.is_empty());
        Ok(())
    }

    #[test]
    fn canceling_after_the_last_byte_leaves_files_ready_to_retry() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path();
        let output = root.join("right-output");
        fs::create_dir(&output)?;
        let source = root.join("notes.txt");
        let data = b"received and verified";
        fs::write(&source, data)?;
        let batch = transfer::prepare(std::slice::from_ref(&source), &AtomicBool::new(false))?;

        receive_until_canceled(&batch, &output, |done, total| done == total)?;
        assert!(
            !output.join("notes.txt").exists(),
            "Canceled file was published"
        );
        let pending = transfer::pending(&output)?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].bytes, data.len() as u64);

        let (left, right) = connected(root);
        pair(&left, &right);
        left.commands.send(Command::Send(vec![source]))?;
        right.approve_batch();
        let reused = right.next(|event| match event {
            Event::Progress {
                receiving: true,
                done,
                ..
            } => Some(done),
            _ => None,
        });
        assert_eq!(reused, data.len() as u64);
        right.next(|event| {
            matches!(
                event,
                Event::Completed {
                    receiving: true,
                    ..
                }
            )
            .then_some(())
        });
        assert_eq!(fs::read(output.join("notes.txt"))?, data);
        assert!(transfer::pending(&output)?.is_empty());
        Ok(())
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
