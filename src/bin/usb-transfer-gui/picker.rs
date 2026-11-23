use super::view::{chevron, dialog_backdrop, heading, item_count, primary_button, quiet_button};
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
    editing_location: bool,
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
            editing_location: false,
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
        self.editing_location = false;
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
                if !self.editing_location {
                    self.location = directory.path.to_string_lossy().into_owned();
                }
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
            .backdrop_color(dialog_backdrop(ctx))
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
                    if self.selected.is_empty() {
                        ui.weak("Check items to add");
                    } else {
                        ui.weak(format!("{} selected", item_count(self.selected.len())));
                    }
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
        let location_id = ui.make_persistent_id("folder-location");
        if ui.input_mut(|input| input.consume_key(egui::Modifiers::COMMAND, egui::Key::L)) {
            self.editing_location = true;
            ui.memory_mut(|memory| memory.request_focus(location_id));
        }
        if self.editing_location
            && ui.input_mut(|input| input.consume_key(egui::Modifiers::NONE, egui::Key::Escape))
        {
            self.editing_location = false;
            self.location = self.directory.to_string_lossy().into_owned();
        }
        let parent = self
            .directory
            .parent()
            .filter(|path| !path.as_os_str().is_empty());
        let next = ui
            .horizontal(|ui| {
                let up = ui
                    .add_enabled(
                        parent.is_some(),
                        egui::Button::new("Up").frame_when_inactive(false),
                    )
                    .clicked();
                let home = quiet_button(ui, "Home").clicked();
                let height = ui.spacing().interact_size.y;
                let go = if self.editing_location {
                    let field = ui.add_sized(
                        [ui.available_width() - 52.0, height],
                        egui::TextEdit::singleline(&mut self.location)
                            .id(location_id)
                            .hint_text("Folder path"),
                    );
                    ui.button("Go").clicked()
                        || (field.lost_focus()
                            && ui.input(|input| input.key_pressed(egui::Key::Enter)))
                } else {
                    let name = self
                        .directory
                        .file_name()
                        .unwrap_or(self.directory.as_os_str())
                        .to_string_lossy();
                    let location = egui::Button::new(egui::RichText::new(name.as_ref()).strong())
                        .right_text(egui::RichText::new("Path…").weak())
                        .truncate()
                        .min_size(vec2(ui.available_width(), height));
                    if ui
                        .add(location)
                        .on_hover_ui(|ui| {
                            ui.label(self.directory.display().to_string());
                            ui.weak("Edit the folder path");
                        })
                        .clicked()
                    {
                        self.editing_location = true;
                        ui.memory_mut(|memory| memory.request_focus(location_id));
                    }
                    false
                };
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
                            vec2(ui.available_width() - 10.0, row_height),
                            Layout::left_to_right(Align::Center),
                            |ui| {
                                let was_selected = self.selected.contains(&entry.path);
                                let mut selected = was_selected;
                                let checkbox = ui.checkbox(&mut selected, "");
                                let arrow = ui.id().with("browse-folder");
                                let name = egui::Button::new(&entry.name)
                                    .frame_when_inactive(false)
                                    .truncate()
                                    .right_text(egui::Atom::custom(arrow, vec2(14.0, 14.0)))
                                    .min_size(vec2(ui.available_width(), row_height))
                                    .atom_ui(ui);
                                checkbox.labelled_by(name.response.id);
                                if entry.folder
                                    && let Some(rect) = name.rect(arrow)
                                {
                                    chevron(ui, rect, ui.visuals().weak_text_color());
                                }
                                if name
                                    .response
                                    .on_hover_ui(|ui| {
                                        ui.label(entry.path.display().to_string());
                                    })
                                    .clicked()
                                {
                                    if entry.folder {
                                        next = Some(entry.path.clone());
                                    } else {
                                        selected = !selected;
                                    }
                                }
                                if selected != was_selected {
                                    if selected {
                                        self.selected.insert(entry.path.clone());
                                    } else {
                                        self.selected.shift_remove(&entry.path);
                                    }
                                }
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
