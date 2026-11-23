#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use eframe::egui;
use indexmap::IndexSet;
use usb_direct_transfer::{
    Command, ConnectionMonitor, ConnectionReport, Event, Options, Pending, Session, Summary,
    discard, pending, probe_peer, start_with_notify,
};

#[path = "usb-transfer-gui/picker.rs"]
mod picker;
#[path = "usb-transfer-gui/view.rs"]
mod view;

use picker::FilePicker;

const TITLE: &str = "USB Direct Transfer";
const DESTINATION_KEY: &str = "destination";
const RATE_STALE_AFTER: Duration = Duration::from_secs(3);

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_app_id("usb-direct-transfer")
            .with_decorations(true)
            .with_inner_size([720.0, 580.0])
            .with_min_inner_size([440.0, 480.0])
            .with_drag_and_drop(true),
        #[cfg(target_os = "linux")]
        event_loop_builder: gnome_window_manager(),
        ..Default::default()
    };
    eframe::run_native(
        TITLE,
        options,
        Box::new(|context| Ok(Box::new(Desktop::new(context)))),
    )
}

#[cfg(target_os = "linux")]
fn gnome_window_manager() -> Option<eframe::EventLoopBuilderHook> {
    use winit::platform::x11::EventLoopBuilderExtX11;

    let desktop = std::env::var("XDG_CURRENT_DESKTOP").ok()?;
    if std::env::var("XDG_SESSION_TYPE").as_deref() != Ok("wayland")
        || !desktop
            .split(':')
            .any(|part| part.eq_ignore_ascii_case("GNOME"))
    {
        return None;
    }

    // GNOME Wayland uses client-drawn decorations for this window. With
    // XWayland, the window manager draws the desktop's configured title bar.
    let display = std::env::var("DISPLAY").ok()?;
    if !display.starts_with(':') {
        return None;
    }
    x11rb::connect(Some(&display)).ok()?;
    Some(Box::new(|builder| {
        builder.with_x11();
    }))
}

type SaveDialog = Pin<Box<dyn Future<Output = Option<PathBuf>>>>;

struct Repaint(egui::Context);

impl Wake for Repaint {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.request_repaint();
    }
}

enum Background {
    PeerCheck(Result<(), String>),
    Pending(PathBuf, Result<Vec<Pending>, String>),
    Discarded(PathBuf, Result<(), String>),
}

struct LiveConnection {
    updates: Receiver<Arc<ConnectionReport>>,
    stop: Arc<AtomicBool>,
}

impl LiveConnection {
    fn new(ctx: &egui::Context) -> Self {
        let (tx, updates) = mpsc::sync_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let cancelled = stop.clone();
        let ctx = ctx.clone();
        thread::spawn(move || {
            let mut monitor = ConnectionMonitor::default();
            let mut last_report = None;
            while !cancelled.load(Ordering::Relaxed) {
                let report = monitor.poll();
                if last_report.as_deref() != Some(&report) {
                    let report = Arc::new(report);
                    match tx.try_send(Arc::clone(&report)) {
                        Ok(()) => {
                            last_report = Some(report);
                            ctx.request_repaint();
                        }
                        Err(mpsc::TrySendError::Full(_)) => {}
                        Err(mpsc::TrySendError::Disconnected(_)) => break,
                    }
                }
                for _ in 0..10 {
                    if cancelled.load(Ordering::Relaxed) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }
        });
        Self { updates, stop }
    }
}

impl Drop for LiveConnection {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

struct Incoming {
    names: Vec<String>,
    bytes: u64,
}

struct Progress {
    receiving: bool,
    started: Instant,
    sampled_at: Instant,
    initial: u64,
    done: u64,
    total: u64,
}

impl Progress {
    fn rate(&self, now: Instant) -> Option<f64> {
        let elapsed = self.sampled_at.duration_since(self.started).as_secs_f64();
        let transferred = self.done.saturating_sub(self.initial);
        (elapsed >= 0.3
            && transferred > 0
            && now.duration_since(self.sampled_at) < RATE_STALE_AFTER)
            .then(|| transferred as f64 / elapsed)
    }
}

enum TransferResult {
    Completed { receiving: bool, summary: Summary },
    Canceled,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Page {
    Receive,
    Send,
    Settings,
}

enum Pairing {
    Idle,
    Searching(Instant),
    Compare(String),
    WaitingForConfirmation,
    Paired,
    Disconnecting,
}

struct Desktop {
    session: Option<Session>,
    pairing: Pairing,
    page: Page,
    interface_text: String,
    peer_text: String,
    session_detail: Option<String>,
    error: Option<String>,
    destination: Option<PathBuf>,
    queued: IndexSet<PathBuf>,
    last_sent: Vec<PathBuf>,
    incoming: Option<Incoming>,
    sending: bool,
    receiving: bool,
    progress: Option<Progress>,
    result: Option<TransferResult>,
    saved: Vec<Pending>,
    confirm_discard: Option<String>,
    loading_pending: bool,
    discarding: bool,
    peer_check: Option<Result<(), String>>,
    checking_peer: bool,
    connection: Option<Arc<ConnectionReport>>,
    connection_watch: LiveConnection,
    detail: Option<view::Detail>,
    diagnostics_tab: view::DiagnosticsTab,
    save_dialog: Option<SaveDialog>,
    picker: Option<FilePicker>,
    picker_location: Option<PathBuf>,
    background_tx: Sender<Background>,
    background_rx: Receiver<Background>,
    repaint: Waker,
}

impl Desktop {
    fn new(context: &eframe::CreationContext<'_>) -> Self {
        view::configure_style(&context.egui_ctx);
        let (background_tx, background_rx) = mpsc::channel();
        let mut app = Self {
            session: None,
            pairing: Pairing::Idle,
            page: Page::Receive,
            interface_text: String::new(),
            peer_text: String::new(),
            session_detail: None,
            error: None,
            destination: context
                .storage
                .and_then(|storage| storage.get_string(DESTINATION_KEY))
                .filter(|path| !path.is_empty())
                .map(PathBuf::from),
            queued: IndexSet::new(),
            last_sent: Vec::new(),
            incoming: None,
            sending: false,
            receiving: false,
            progress: None,
            result: None,
            saved: Vec::new(),
            confirm_discard: None,
            loading_pending: false,
            discarding: false,
            peer_check: None,
            checking_peer: false,
            connection: None,
            connection_watch: LiveConnection::new(&context.egui_ctx),
            detail: None,
            diagnostics_tab: view::DiagnosticsTab::Overview,
            save_dialog: None,
            picker: None,
            picker_location: None,
            background_tx,
            background_rx,
            repaint: Waker::from(Arc::new(Repaint(context.egui_ctx.clone()))),
        };
        app.refresh_pending(&context.egui_ctx);
        app
    }

    fn options(&self) -> Options {
        Options {
            interface: nonempty(&self.interface_text),
            peer: nonempty(&self.peer_text),
        }
    }

    fn background(&self, ctx: &egui::Context, job: impl FnOnce() -> Background + Send + 'static) {
        let tx = self.background_tx.clone();
        let ctx = ctx.clone();
        thread::spawn(move || {
            let _ = tx.send(job());
            ctx.request_repaint();
        });
    }

    fn check_peer(&mut self, ctx: &egui::Context) {
        let options = self.options();
        self.checking_peer = true;
        self.peer_check = None;
        self.background(ctx, move || {
            Background::PeerCheck(probe_peer(&options).map_err(|error| format!("{error:#}")))
        });
    }

    fn refresh_pending(&mut self, ctx: &egui::Context) {
        let Some(destination) = self.destination.clone() else {
            return;
        };
        self.confirm_discard = None;
        self.loading_pending = true;
        self.background(ctx, move || {
            let result = pending(&destination).map_err(|error| format!("{error:#}"));
            Background::Pending(destination, result)
        });
    }

    fn discard(&mut self, ctx: &egui::Context, id: String) {
        let Some(destination) = self.destination.clone() else {
            return;
        };
        self.discarding = true;
        self.background(ctx, move || {
            let result = discard(&destination, &id).map_err(|error| format!("{error:#}"));
            Background::Discarded(destination, result)
        });
    }

    fn update_background(&mut self, ctx: &egui::Context) {
        while let Ok(report) = self.connection_watch.updates.try_recv() {
            self.connection = Some(report);
        }
        while let Ok(message) = self.background_rx.try_recv() {
            match message {
                Background::PeerCheck(result) => {
                    self.checking_peer = false;
                    self.peer_check = Some(result);
                }
                Background::Pending(path, result) if Some(&path) == self.destination.as_ref() => {
                    self.loading_pending = false;
                    match result {
                        Ok(items) => self.saved = items,
                        Err(error) => self.error = Some(error),
                    }
                }
                Background::Discarded(path, result) => {
                    self.discarding = false;
                    if let Err(error) = result {
                        self.error = Some(error);
                    }
                    if Some(&path) == self.destination.as_ref() {
                        self.refresh_pending(ctx);
                    }
                }
                Background::Pending(_, _) => {}
            }
        }
    }

    fn choose_save_folder(&mut self, frame: &eframe::Frame) {
        let mut dialog = rfd::AsyncFileDialog::new()
            .set_parent(frame)
            .set_title("Choose a receive folder");
        if let Some(path) = &self.destination {
            dialog = dialog.set_directory(path);
        }
        self.save_dialog = Some(Box::pin(async move {
            dialog
                .pick_folder()
                .await
                .map(|handle| handle.path().to_path_buf())
        }));
    }

    fn update_save_dialog(&mut self, ctx: &egui::Context) {
        let Some(dialog) = &mut self.save_dialog else {
            return;
        };
        let mut context = Context::from_waker(&self.repaint);
        if let Poll::Ready(path) = dialog.as_mut().poll(&mut context) {
            if let Some(path) = path {
                self.destination = Some(path);
                self.saved.clear();
                self.confirm_discard = None;
                self.refresh_pending(ctx);
            }
            self.save_dialog = None;
        }
    }

    fn open_picker(&mut self, ctx: &egui::Context) {
        self.picker = Some(FilePicker::new(ctx, self.picker_location.clone()));
    }

    fn add_paths(&mut self, paths: Vec<PathBuf>) {
        self.queued.extend(paths);
    }

    fn send_command(&mut self, command: Command) -> bool {
        let sent = self
            .session
            .as_ref()
            .is_some_and(|session| session.commands.send(command).is_ok());
        if !sent {
            self.error = Some("The session ended. Pair again to continue.".into());
        }
        sent
    }

    fn begin(&mut self) {
        let repaint = self.repaint.clone();
        self.session = Some(start_with_notify(
            self.options(),
            self.destination.clone().unwrap_or_default(),
            move || repaint.wake_by_ref(),
        ));
        self.error = None;
        self.session_detail = None;
        self.pairing = Pairing::Searching(Instant::now());
    }

    fn disconnect(&mut self, ctx: &egui::Context) {
        self.session.take();
        self.pairing = Pairing::Idle;
        self.incoming = None;
        self.sending = false;
        self.receiving = false;
        self.progress = None;
        self.refresh_pending(ctx);
    }

    fn request_disconnect(&mut self) {
        if let Some(session) = &self.session {
            session.disconnect();
            self.pairing = Pairing::Disconnecting;
        }
    }

    fn cancel_transfer(&mut self) {
        if let Some(session) = &self.session {
            session.cancel_transfer();
            self.result = Some(TransferResult::Canceled);
            self.pairing = Pairing::Disconnecting;
        }
    }

    fn confirm_pairing(&mut self, matching: bool) {
        if self.send_command(Command::Confirm(matching)) {
            self.pairing = if matching {
                Pairing::WaitingForConfirmation
            } else {
                Pairing::Disconnecting
            };
        }
    }

    fn send(&mut self, paths: Vec<PathBuf>) -> bool {
        if self.send_command(Command::Send(paths.clone())) {
            self.last_sent = paths;
            self.error = None;
            self.result = None;
            self.sending = true;
            true
        } else {
            false
        }
    }

    fn respond_to_transfer(&mut self, accept: bool) {
        let destination = if accept {
            let Some(folder) = self.destination.clone() else {
                return;
            };
            Some(folder)
        } else {
            None
        };
        if self.send_command(Command::Approve(destination)) {
            self.error = None;
            self.result = None;
            self.receiving = true;
        }
    }

    fn update_session(&mut self, ctx: &egui::Context) {
        for _ in 0..512 {
            let Some(session) = self.session.as_ref() else {
                return;
            };
            let event = match session.events.try_recv() {
                Ok(event) => event,
                Err(mpsc::TryRecvError::Empty) => return,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.disconnect(ctx);
                    return;
                }
            };
            match event {
                Event::Status(status) => self.session_detail = Some(status),
                Event::PairCode(code) if !matches!(self.pairing, Pairing::Disconnecting) => {
                    self.pairing = Pairing::Compare(code);
                    self.detail = None;
                    if self.page == Page::Settings {
                        self.page = Page::Receive;
                    }
                }
                Event::PairCode(_) => {}
                Event::Paired => {
                    if !matches!(self.pairing, Pairing::Disconnecting) {
                        self.pairing = Pairing::Paired;
                    }
                }
                Event::Incoming { names, bytes } => {
                    self.incoming = Some(Incoming { names, bytes });
                    self.detail = None;
                    self.page = Page::Receive;
                }
                Event::IncomingClosed { accepted } => {
                    self.incoming = None;
                    self.receiving = accepted;
                }
                Event::Progress {
                    receiving,
                    done,
                    total,
                    sampled_at,
                } => match &mut self.progress {
                    Some(progress) if progress.receiving == receiving => {
                        progress.sampled_at = sampled_at;
                        progress.done = done;
                        progress.total = total;
                    }
                    _ => {
                        self.progress = Some(Progress {
                            receiving,
                            started: sampled_at,
                            sampled_at,
                            initial: done,
                            done,
                            total,
                        });
                    }
                },
                Event::Completed { receiving, summary } => {
                    self.result = Some(TransferResult::Completed { receiving, summary });
                    self.progress = None;
                    if receiving {
                        self.receiving = false;
                    } else {
                        self.sending = false;
                        self.last_sent.clear();
                    }
                    if receiving {
                        self.refresh_pending(ctx);
                    }
                }
                Event::Error(error) => {
                    self.error = Some(error);
                    if self.incoming.is_none() {
                        self.sending = false;
                    }
                    self.receiving = false;
                    self.progress = None;
                }
                Event::Disconnected => self.disconnect(ctx),
            }
        }
        ctx.request_repaint();
    }
}

impl eframe::App for Desktop {
    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {
        self.update_background(ctx);
        self.update_session(ctx);
        self.update_save_dialog(ctx);
        let dropped: Vec<_> = ctx.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .filter_map(|file| file.path.clone())
                .collect()
        });
        if !dropped.is_empty() {
            self.add_paths(dropped);
            if self.incoming.is_none() {
                self.page = Page::Send;
            }
        }
        self.draw(ctx, frame);
        if let Some(picker) = self.picker.as_mut()
            && let Some(paths) = picker.show(ctx)
        {
            if !paths.is_empty() {
                self.picker_location = Some(picker.directory().to_path_buf());
                self.add_paths(paths);
            }
            self.picker = None;
        }
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        let destination = self
            .destination
            .as_ref()
            .and_then(|path| path.to_str())
            .unwrap_or_default();
        storage.set_string(DESTINATION_KEY, destination.to_owned());
    }
}

fn display_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or(path.as_os_str())
        .to_string_lossy()
        .into_owned()
}

fn nonempty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn size(bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    const KIB: u64 = 1024;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_rate_excludes_resumed_bytes_and_waits_for_a_sample() {
        let started = Instant::now();
        let mut progress = Progress {
            receiving: true,
            started,
            sampled_at: started,
            initial: 800,
            done: 800,
            total: 1000,
        };
        assert_eq!(progress.rate(started), None);
        progress.sampled_at += Duration::from_secs(2);
        progress.done += 100;
        assert_eq!(progress.rate(progress.sampled_at), Some(50.0));
        assert_eq!(
            progress.rate(progress.sampled_at + Duration::from_secs(4)),
            None
        );
    }
}
