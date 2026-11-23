#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::mpsc::{self, Receiver, Sender};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use eframe::egui;
use usb_direct_transfer::{
    Command, Event, Interface, Options, Pending, Session, Summary, discard, doctor, interfaces,
    pending, start,
};

const TITLE: &str = "USB Direct Transfer";
const DESTINATION_KEY: &str = "destination";

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_app_id("usb-direct-transfer")
            .with_inner_size([720.0, 640.0])
            .with_min_inner_size([460.0, 440.0])
            .with_drag_and_drop(true),
        ..Default::default()
    };
    eframe::run_native(
        TITLE,
        options,
        Box::new(|context| Ok(Box::new(Desktop::new(context)))),
    )
}

#[derive(Clone, Copy)]
enum Pick {
    Files,
    Folder,
    Destination,
}

type Dialog = Pin<Box<dyn Future<Output = Vec<PathBuf>>>>;

enum Background {
    Diagnostics(Vec<Interface>, String),
    Pending(PathBuf, Result<Vec<Pending>, String>),
    Discarded(PathBuf, Result<(), String>),
}

struct Incoming {
    names: Vec<String>,
    bytes: u64,
}

struct Progress {
    receiving: bool,
    started: Instant,
    initial: u64,
    done: u64,
    total: u64,
}

struct Desktop {
    session: Option<Session>,
    interface_text: String,
    peer_text: String,
    pairing_code: Option<String>,
    paired: bool,
    status: String,
    error: Option<String>,
    destination: Option<PathBuf>,
    queued: Vec<PathBuf>,
    last_sent: Vec<PathBuf>,
    incoming: Option<Incoming>,
    sending: bool,
    receiving: bool,
    progress: Option<Progress>,
    result: Option<(bool, Summary)>,
    saved: Vec<Pending>,
    confirm_discard: Option<String>,
    loading_pending: bool,
    discarding: bool,
    diagnostics: Option<String>,
    available: Vec<Interface>,
    diagnosing: bool,
    dialog: Option<(Pick, Dialog)>,
    background_tx: Sender<Background>,
    background_rx: Receiver<Background>,
}

impl Desktop {
    fn new(context: &eframe::CreationContext<'_>) -> Self {
        let (background_tx, background_rx) = mpsc::channel();
        let mut app = Self {
            session: None,
            interface_text: String::new(),
            peer_text: String::new(),
            pairing_code: None,
            paired: false,
            status: "Connect the USB-C cable to both computers.".into(),
            error: None,
            destination: context
                .storage
                .and_then(|storage| storage.get_string(DESTINATION_KEY))
                .filter(|path| !path.is_empty())
                .map(PathBuf::from),
            queued: Vec::new(),
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
            diagnostics: None,
            available: Vec::new(),
            diagnosing: false,
            dialog: None,
            background_tx,
            background_rx,
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

    fn diagnose(&mut self, ctx: &egui::Context) {
        let options = self.options();
        self.diagnosing = true;
        self.background(ctx, move || {
            let available = interfaces().unwrap_or_default();
            let details = doctor(&options).unwrap_or_else(|error| format!("{error:#}"));
            Background::Diagnostics(available, details)
        });
    }

    fn refresh_pending(&mut self, ctx: &egui::Context) {
        let Some(destination) = self.destination.clone() else {
            return;
        };
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
        while let Ok(message) = self.background_rx.try_recv() {
            match message {
                Background::Diagnostics(interfaces, details) => {
                    self.diagnosing = false;
                    self.available = interfaces;
                    self.diagnostics = Some(details);
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

    fn open_dialog(&mut self, pick: Pick, frame: &eframe::Frame) {
        let mut dialog = rfd::AsyncFileDialog::new()
            .set_parent(frame)
            .set_title(match pick {
                Pick::Files => "Select files to send",
                Pick::Folder => "Select a folder to send",
                Pick::Destination => "Choose a receive folder",
            });
        if let (Pick::Destination, Some(path)) = (pick, &self.destination) {
            dialog = dialog.set_directory(path);
        }
        self.dialog = Some((
            pick,
            Box::pin(async move {
                let selection = match pick {
                    Pick::Files => dialog.pick_files().await.unwrap_or_default(),
                    Pick::Folder | Pick::Destination => {
                        dialog.pick_folder().await.into_iter().collect()
                    }
                };
                selection
                    .into_iter()
                    .map(|handle| handle.path().to_path_buf())
                    .collect()
            }),
        ));
    }

    fn update_dialog(&mut self, ctx: &egui::Context) {
        let Some((pick, dialog)) = &mut self.dialog else {
            return;
        };
        let mut context = Context::from_waker(Waker::noop());
        if let Poll::Ready(mut paths) = dialog.as_mut().poll(&mut context) {
            if matches!(pick, Pick::Destination) {
                if let Some(path) = paths.pop() {
                    self.destination = Some(path);
                    self.saved.clear();
                    self.confirm_discard = None;
                    self.refresh_pending(ctx);
                }
            } else {
                self.add_paths(paths);
            }
            self.dialog = None;
        }
    }

    fn add_paths(&mut self, paths: Vec<PathBuf>) {
        for path in paths {
            if !self.queued.contains(&path) {
                self.queued.push(path);
            }
        }
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
        self.session = Some(start(
            self.options(),
            self.destination.clone().unwrap_or_default(),
        ));
        self.error = None;
        self.status = "Looking for the peer over the cable. Open Pair on both computers.".into();
    }

    fn disconnect(&mut self, ctx: &egui::Context) {
        self.session.take();
        self.pairing_code = None;
        self.paired = false;
        self.incoming = None;
        self.sending = false;
        self.receiving = false;
        self.progress = None;
        self.status = "Disconnected. Pair again to transfer files.".into();
        self.refresh_pending(ctx);
    }

    fn send(&mut self, paths: Vec<PathBuf>) -> bool {
        if self.send_command(Command::Send(paths.clone())) {
            self.last_sent = paths;
            self.status = "Preparing files for the peer to approve.".into();
            self.error = None;
            self.result = None;
            self.sending = true;
            true
        } else {
            false
        }
    }

    fn update_session(&mut self, ctx: &egui::Context) {
        for _ in 0..512 {
            let Some(session) = self.session.as_ref() else {
                return;
            };
            let event = match session.events.try_recv() {
                Ok(event) => event,
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.disconnect(ctx);
                    break;
                }
            };
            match event {
                Event::Status(status) => self.status = status,
                Event::PairCode(code) => self.pairing_code = Some(code),
                Event::Paired => {
                    self.paired = true;
                    self.pairing_code = None;
                    self.status = "Paired over the cable.".into();
                }
                Event::Incoming { names, bytes } => {
                    self.incoming = Some(Incoming { names, bytes });
                    self.status = "Review the incoming transfer.".into();
                }
                Event::IncomingClosed { accepted } => {
                    self.incoming = None;
                    self.receiving = accepted;
                    self.status = if accepted {
                        "Receiving approved files."
                    } else {
                        "Paired over the cable."
                    }
                    .into();
                }
                Event::Progress {
                    receiving,
                    done,
                    total,
                } => {
                    let progress = self.progress.get_or_insert(Progress {
                        receiving,
                        started: Instant::now(),
                        initial: done,
                        done,
                        total,
                    });
                    progress.done = done;
                    progress.total = total;
                }
                Event::Completed { receiving, summary } => {
                    self.result = Some((receiving, summary));
                    self.progress = None;
                    if receiving {
                        self.receiving = false;
                    } else {
                        self.sending = false;
                        self.last_sent.clear();
                    }
                    self.status = "Paired over the cable.".into();
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
    }

    fn connection_ui(&mut self, ui: &mut egui::Ui) {
        ui.horizontal_wrapped(|ui| {
            if self.session.is_none() {
                if ui.button("Pair").clicked() {
                    self.begin();
                }
            } else if ui.button("Disconnect").clicked() {
                if let Some(session) = &self.session {
                    session.disconnect();
                }
                self.status = "Disconnecting…".into();
            }
            ui.label(&self.status);
        });

        if let Some(code) = self.pairing_code.clone() {
            ui.group(|ui| {
                ui.label("Compare this code on both computers.");
                ui.label(egui::RichText::new(code).monospace().heading());
                ui.horizontal(|ui| {
                    if ui.button("Codes match").clicked()
                        && self.send_command(Command::Confirm(true))
                    {
                        self.pairing_code = None;
                        self.status = "Waiting for the peer to confirm the code.".into();
                    }
                    if ui.button("Codes differ").clicked() {
                        let _ = self.send_command(Command::Confirm(false));
                        self.pairing_code = None;
                        self.status = "Ending pairing.".into();
                    }
                });
            });
        }

        egui::CollapsingHeader::new("Connection diagnostics").show(ui, |ui| {
            let disconnected = self.session.is_none();
            ui.add_enabled_ui(disconnected, |ui| {
                ui.horizontal_wrapped(|ui| {
                    let label = ui.label("Cable interface");
                    ui.text_edit_singleline(&mut self.interface_text)
                        .labelled_by(label.id)
                        .on_hover_text("Leave blank for automatic interface selection.");
                });
                ui.horizontal_wrapped(|ui| {
                    let label = ui.label("Peer address");
                    ui.text_edit_singleline(&mut self.peer_text)
                        .labelled_by(label.id)
                        .on_hover_text("Leave blank for cable discovery.");
                });
            });
            if ui
                .add_enabled(!self.diagnosing, egui::Button::new("Run diagnostics"))
                .clicked()
            {
                self.diagnose(ui.ctx());
            }
            if self.diagnosing {
                ui.spinner();
            }
            for interface in &self.available {
                ui.horizontal_wrapped(|ui| {
                    if ui
                        .add_enabled(disconnected, egui::Button::new(&interface.name))
                        .clicked()
                    {
                        self.interface_text.clone_from(&interface.name);
                    }
                    ui.label(format!("{}  {}", interface.address, interface.status));
                });
            }
            if let Some(details) = &self.diagnostics {
                ui.add(egui::Label::new(details).selectable(true));
            }
        });
    }

    fn send_ui(&mut self, ui: &mut egui::Ui, frame: &eframe::Frame) {
        ui.heading("Send");
        let can_choose = self.dialog.is_none();
        ui.horizontal_wrapped(|ui| {
            if ui
                .add_enabled(can_choose, egui::Button::new("Add files…"))
                .clicked()
            {
                self.open_dialog(Pick::Files, frame);
            }
            if ui
                .add_enabled(can_choose, egui::Button::new("Add folder…"))
                .clicked()
            {
                self.open_dialog(Pick::Folder, frame);
            }
            ui.label("Drop files or folders here");
        });

        let mut remove = None;
        for (index, path) in self.queued.iter().enumerate() {
            ui.horizontal_wrapped(|ui| {
                if ui
                    .button("Remove")
                    .on_hover_text(path.display().to_string())
                    .clicked()
                {
                    remove = Some(index);
                }
                ui.label(display_name(path))
                    .on_hover_text(path.display().to_string());
            });
        }
        if let Some(index) = remove {
            self.queued.remove(index);
        }

        let ready = self.paired && !self.sending && !self.receiving && self.incoming.is_none();
        ui.horizontal(|ui| {
            if ui
                .add_enabled(
                    ready && !self.queued.is_empty(),
                    egui::Button::new("Send selection"),
                )
                .clicked()
                && self.send(self.queued.clone())
            {
                self.queued.clear();
            }
            if ui
                .add_enabled(
                    ready && !self.last_sent.is_empty(),
                    egui::Button::new("Retry last send"),
                )
                .on_hover_text(
                    "The receiver can reuse verified files by approving the same destination.",
                )
                .clicked()
            {
                self.send(self.last_sent.clone());
            }
        });
    }

    fn receive_ui(&mut self, ui: &mut egui::Ui, frame: &eframe::Frame) {
        ui.heading("Receive");
        ui.horizontal_wrapped(|ui| {
            let label = self
                .destination
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "No receive folder selected".into());
            if ui
                .add_enabled(
                    !self.receiving && !self.discarding && self.dialog.is_none(),
                    egui::Button::new("Choose folder…"),
                )
                .clicked()
            {
                self.open_dialog(Pick::Destination, frame);
            }
            ui.add(egui::Label::new(label).selectable(true));
        });

        let Some(incoming) = &self.incoming else {
            return;
        };
        let (accept, decline) = ui
            .group(|ui| {
                ui.strong(format!("Incoming transfer · {}", size(incoming.bytes)));
                let row_height = ui.text_style_height(&egui::TextStyle::Body);
                egui::ScrollArea::vertical()
                    .id_salt("incoming-files")
                    .max_height(120.0)
                    .show_rows(ui, row_height, incoming.names.len(), |ui, range| {
                        for name in &incoming.names[range] {
                            ui.add(egui::Label::new(name).truncate())
                                .on_hover_text(name);
                        }
                    });
                ui.horizontal(|ui| {
                    let accept = ui
                        .add_enabled(
                            self.destination.is_some()
                                && self.dialog.is_none()
                                && !self.discarding
                                && !self.receiving,
                            egui::Button::new("Accept transfer"),
                        )
                        .clicked();
                    (
                        accept,
                        ui.add_enabled(!self.receiving, egui::Button::new("Decline"))
                            .clicked(),
                    )
                })
                .inner
            })
            .inner;
        if accept && self.send_command(Command::Approve(self.destination.clone())) {
            self.error = None;
            self.result = None;
            self.receiving = true;
            self.status = "Accepting transfer…".into();
        }
        if decline && self.send_command(Command::Approve(None)) {
            self.receiving = true;
            self.status = "Declining transfer…".into();
        }
    }

    fn pending_ui(&mut self, ui: &mut egui::Ui) {
        egui::CollapsingHeader::new("Pending transfers").show(ui, |ui| {
            if self.destination.is_none() {
                ui.label("Choose a receive folder to see its saved transfers.");
                return;
            }
            if ui
                .add_enabled(!self.loading_pending && !self.discarding, egui::Button::new("Refresh"))
                .clicked()
            {
                self.refresh_pending(ui.ctx());
            }
            if self.loading_pending || self.discarding {
                ui.spinner();
            } else if self.saved.is_empty() {
                ui.label("No saved transfers in this folder.");
            } else {
                ui.label("Ask the peer to resend the same selection and approve this folder to reuse verified files.");
            }

            let mut discard_id = None;
            let can_discard = !self.receiving && !self.discarding && self.incoming.is_none();
            for item in &self.saved {
                ui.push_id(&item.id, |ui| {
                    ui.horizontal_wrapped(|ui| {
                        ui.monospace(&item.id).on_hover_text("Saved transfer ID");
                        ui.label(size(item.bytes));
                        if self.confirm_discard.as_ref() == Some(&item.id) {
                            if ui.add_enabled(can_discard, egui::Button::new("Discard saved files"))
                                .clicked()
                            {
                                discard_id = Some(item.id.clone());
                            }
                            if ui.button("Keep files").clicked() {
                                self.confirm_discard = None;
                            }
                        } else if ui
                            .add_enabled(can_discard, egui::Button::new("Discard…"))
                            .clicked()
                        {
                            self.confirm_discard = Some(item.id.clone());
                        }
                    });
                });
            }
            if let Some(id) = discard_id {
                self.confirm_discard = None;
                self.discard(ui.ctx(), id);
            }
        });
    }

    fn transfer_ui(&self, ui: &mut egui::Ui) {
        if let Some(progress) = &self.progress {
            let direction = if progress.receiving {
                "Receiving"
            } else {
                "Sending"
            };
            let fraction = if progress.total == 0 {
                1.0
            } else {
                (progress.done as f64 / progress.total as f64).min(1.0) as f32
            };
            let elapsed = progress.started.elapsed().as_secs_f64();
            let transferred = progress.done.saturating_sub(progress.initial);
            let speed = if elapsed > 0.05 && transferred > 0 {
                format!(" · {}/s", size((transferred as f64 / elapsed) as u64))
            } else {
                String::new()
            };
            ui.add(egui::ProgressBar::new(fraction).text(format!(
                "{direction} · {} of {}{speed}",
                size(progress.done),
                size(progress.total)
            )));
        }
        if let Some((receiving, summary)) = &self.result {
            ui.strong(if *receiving {
                "Files verified and saved"
            } else {
                "Peer verified and saved files"
            });
            for renamed in &summary.renamed {
                ui.label(format!("Renamed · {renamed}"));
            }
            for skipped in &summary.skipped {
                ui.label(format!("Skipped · {skipped}"));
            }
        }
    }
}

impl eframe::App for Desktop {
    fn update(&mut self, ctx: &egui::Context, frame: &mut eframe::Frame) {
        self.update_background(ctx);
        self.update_session(ctx);
        self.update_dialog(ctx);
        if self.session.is_some() || self.dialog.is_some() {
            ctx.request_repaint_after(Duration::from_millis(60));
        }
        self.add_paths(ctx.input(|input| {
            input
                .raw
                .dropped_files
                .iter()
                .filter_map(|file| file.path.clone())
                .collect()
        }));
        egui::TopBottomPanel::top("title").show(ctx, |ui| {
            ui.add_space(8.0);
            ui.heading(TITLE);
            ui.add_space(8.0);
        });
        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.spacing_mut().item_spacing.y = 8.0;
                    self.connection_ui(ui);
                    if let Some(error) = &self.error {
                        ui.colored_label(ui.visuals().error_fg_color, error);
                    }
                    self.transfer_ui(ui);
                    ui.separator();
                    if self.incoming.is_some() {
                        self.receive_ui(ui, frame);
                        ui.separator();
                        self.send_ui(ui, frame);
                    } else {
                        self.send_ui(ui, frame);
                        ui.separator();
                        self.receive_ui(ui, frame);
                    }
                    self.pending_ui(ui);
                });
        });
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
