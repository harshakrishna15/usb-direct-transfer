use super::view::{heading, item_count, primary_button, quiet_button};
use eframe::egui::{self, Align, Layout, Modal, Ui, vec2};
use indexmap::IndexSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;

struct Entry {
    name: String,
    path: PathBuf,
    folder: bool,
}

struct Directory {
    path: PathBuf,
    entries: Vec<Entry>,
}

impl Directory {
    fn read(path: &Path) -> io::Result<Self> {
        let path = path.canonicalize()?;
        let mut entries = Vec::new();
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if !kind.is_file() && !kind.is_dir() {
                continue;
            }
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            entries.push(Entry {
                name,
                path: entry.path(),
                folder: kind.is_dir(),
            });
        }
        entries.sort_by_cached_key(|entry| {
            (
                entry.name.starts_with('.'),
                !entry.folder,
                entry.name.to_lowercase(),
            )
        });
        Ok(Self { path, entries })
    }
}

pub(super) struct FilePicker {
    home: PathBuf,
    directory: PathBuf,
    location: String,
    entries: Vec<Entry>,
    selected: IndexSet<PathBuf>,
    reading: Option<Receiver<io::Result<Directory>>>,
    error: Option<String>,
}

impl FilePicker {
    pub(super) fn new(ctx: &egui::Context, previous: Option<PathBuf>) -> Self {
        let home = std::env::home_dir()
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."));
        let directory = previous.unwrap_or_else(|| home.clone());
        let mut picker = Self {
            home,
            directory: PathBuf::new(),
            location: String::new(),
            entries: Vec::new(),
            selected: IndexSet::new(),
            reading: None,
            error: None,
        };
        picker.open(ctx, directory);
        picker
    }

    pub(super) fn directory(&self) -> &Path {
        &self.directory
    }

    fn open(&mut self, ctx: &egui::Context, path: PathBuf) {
        self.location = path.to_string_lossy().into_owned();
        self.directory = path.clone();
        let previous_entries = std::mem::take(&mut self.entries);
        self.error = None;
        let (tx, rx) = mpsc::channel();
        self.reading = Some(rx);
        let ctx = ctx.clone();
        thread::spawn(move || {
            drop(previous_entries);
            if tx.send(Directory::read(&path)).is_ok() {
                ctx.request_repaint();
            }
        });
    }

    fn update(&mut self) {
        let Some(reading) = &self.reading else {
            return;
        };
        match reading.try_recv() {
            Ok(Ok(directory)) => {
                self.location = directory.path.to_string_lossy().into_owned();
                self.directory = directory.path;
                self.entries = directory.entries;
            }
            Ok(Err(error)) => self.error = Some(format!("Could not open this folder. {error}")),
            Err(TryRecvError::Disconnected) => {
                self.error = Some("Could not finish opening this folder.".into());
            }
            Err(TryRecvError::Empty) => return,
        }
        self.reading = None;
    }

    pub(super) fn show(&mut self, ctx: &egui::Context) -> Option<Vec<PathBuf>> {
        self.update();
        let bounds = ctx.content_rect();
        let width = (bounds.width() - 60.0).min(540.0);
        let list_height = (bounds.height() - 235.0).clamp(150.0, 340.0);
        let modal = Modal::new(egui::Id::new("send-picker"))
            .frame(egui::Frame::popup(&ctx.style()).inner_margin(16.0))
            .show(ctx, |ui| {
                ui.set_width(width);
                heading(ui, "Add files and folders");
                self.navigation_ui(ui);
                ui.separator();
                ui.allocate_ui_with_layout(
                    vec2(width, list_height),
                    Layout::top_down(Align::Min),
                    |ui| {
                        ui.set_min_height(list_height);
                        self.entries_ui(ui, list_height);
                    },
                );
                ui.separator();
                ui.horizontal(|ui| {
                    ui.weak(format!("{} selected", item_count(self.selected.len())));
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        let add =
                            primary_button(ui, "Add items", !self.selected.is_empty()).clicked();
                        if quiet_button(ui, "Cancel").clicked() {
                            ui.close();
                        }
                        add
                    })
                    .inner
                })
                .inner
            });
        if modal.inner {
            Some(std::mem::take(&mut self.selected).into_iter().collect())
        } else if modal.should_close() {
            Some(Vec::new())
        } else {
            None
        }
    }

    fn navigation_ui(&mut self, ui: &mut Ui) {
        let parent = self
            .directory
            .parent()
            .filter(|path| !path.as_os_str().is_empty());
        let next = ui
            .horizontal(|ui| {
                let up = ui
                    .add_enabled(parent.is_some(), egui::Button::new("Up"))
                    .clicked();
                let home = ui.button("Home").clicked();
                let field = ui.add_sized(
                    [ui.available_width() - 52.0, ui.spacing().interact_size.y],
                    egui::TextEdit::singleline(&mut self.location).hint_text("Folder path"),
                );
                let go = ui.button("Go").clicked()
                    || (field.lost_focus()
                        && ui.input(|input| input.key_pressed(egui::Key::Enter)));
                if up {
                    parent.map(Path::to_path_buf)
                } else if home {
                    Some(self.home.clone())
                } else if go && !self.location.trim().is_empty() {
                    Some(self.directory.join(&self.location))
                } else {
                    None
                }
            })
            .inner;
        if let Some(path) = next {
            self.open(ui.ctx(), path);
        }
    }

    fn entries_ui(&mut self, ui: &mut Ui, height: f32) {
        if let Some(error) = &self.error {
            ui.colored_label(ui.visuals().error_fg_color, error);
            return;
        }
        if self.reading.is_some() {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.weak("Opening folder…");
            });
            return;
        }
        if self.entries.is_empty() {
            ui.weak("This folder is empty.");
            return;
        }

        let mut next = None;
        let row_height = ui.spacing().interact_size.y;
        egui::ScrollArea::vertical()
            .id_salt(&self.directory)
            .max_height(height)
            .show_rows(ui, row_height, self.entries.len(), |ui, rows| {
                for entry in &self.entries[rows] {
                    ui.push_id(&entry.path, |ui| {
                        ui.allocate_ui_with_layout(
                            vec2(ui.available_width(), row_height),
                            Layout::right_to_left(Align::Center),
                            |ui| {
                                if entry.folder && quiet_button(ui, "Open").clicked() {
                                    next = Some(entry.path.clone());
                                }
                                ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
                                    ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Truncate);
                                    let mut selected = self.selected.contains(&entry.path);
                                    if ui
                                        .checkbox(&mut selected, &entry.name)
                                        .on_hover_ui(|ui| {
                                            ui.label(entry.path.display().to_string());
                                        })
                                        .changed()
                                    {
                                        if selected {
                                            self.selected.insert(entry.path.clone());
                                        } else {
                                            self.selected.shift_remove(&entry.path);
                                        }
                                    }
                                });
                            },
                        );
                    });
                }
            });
        if let Some(path) = next {
            self.open(ui.ctx(), path);
        }
    }
}
