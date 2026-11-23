use super::{
    Desktop, Page, Pairing, Progress, RATE_STALE_AFTER, TransferResult, display_name, nonempty,
    size,
};
use eframe::egui::{
    self, Align, Color32, FontFamily, FontId, Frame, Layout, RichText, Stroke, TextStyle, Ui, vec2,
};
use std::time::{Duration, Instant};
use usb_direct_transfer::{CheckState, ConnectionReport};

const BLUE: Color32 = Color32::from_rgb(39, 85, 207);
const CONTENT_WIDTH: f32 = 640.0;
const PAIRING_HINT_DELAY: Duration = Duration::from_secs(8);

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Detail {
    SavedTransfers,
    Connection,
    ManualPairing,
    TransferResult,
}

impl Detail {
    fn title(self) -> &'static str {
        match self {
            Self::SavedTransfers => "Saved transfers",
            Self::Connection => "Connection diagnostics",
            Self::ManualPairing => "Manual pairing",
            Self::TransferResult => "Renamed and skipped items",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum DiagnosticsTab {
    Overview,
    Activity,
    Hardware,
}

impl Page {
    fn label(self) -> &'static str {
        match self {
            Self::Receive => "Receive",
            Self::Send => "Send",
            Self::Settings => "Settings",
        }
    }
}

pub(super) fn configure_style(ctx: &egui::Context) {
    for theme in [egui::Theme::Light, egui::Theme::Dark] {
        ctx.style_mut_of(theme, |style| {
            let (canvas, surface, inset, border, text, muted, accent) = match theme {
                egui::Theme::Light => (
                    rgb(248, 250, 252),
                    Color32::WHITE,
                    rgb(230, 238, 253),
                    rgb(216, 224, 235),
                    rgb(23, 33, 50),
                    rgb(78, 93, 115),
                    BLUE,
                ),
                egui::Theme::Dark => (
                    rgb(18, 23, 32),
                    rgb(24, 31, 42),
                    rgb(33, 47, 70),
                    rgb(49, 61, 81),
                    rgb(237, 242, 250),
                    rgb(166, 181, 202),
                    rgb(151, 180, 249),
                ),
            };
            for (kind, points) in [
                (TextStyle::Heading, 22.0),
                (TextStyle::Body, 15.0),
                (TextStyle::Button, 14.0),
                (TextStyle::Small, 13.0),
            ] {
                style
                    .text_styles
                    .insert(kind, FontId::new(points, FontFamily::Proportional));
            }
            style.spacing.item_spacing = vec2(8.0, 8.0);
            style.spacing.interact_size.y = 32.0;
            style.spacing.button_padding = vec2(12.0, 6.0);
            style.spacing.scroll = egui::style::ScrollStyle::floating();

            let visuals = &mut style.visuals;
            visuals.panel_fill = canvas;
            visuals.window_fill = surface;
            visuals.extreme_bg_color = canvas;
            visuals.faint_bg_color = inset;
            visuals.weak_text_color = Some(muted);
            visuals.hyperlink_color = accent;
            visuals.menu_corner_radius = 12.into();
            visuals.selection.bg_fill = BLUE;
            visuals.selection.stroke = Stroke::new(1.0_f32, Color32::WHITE);
            let widgets = &mut visuals.widgets;
            widgets.noninteractive.fg_stroke.color = text;
            widgets.noninteractive.bg_stroke.color = border;
            widgets.inactive.fg_stroke.color = text;
            widgets.inactive.bg_stroke = Stroke::new(1.0_f32, border);
            widgets.inactive.bg_fill = surface;
            widgets.inactive.weak_bg_fill = surface;
            widgets.hovered.fg_stroke.color = text;
            widgets.hovered.weak_bg_fill = inset;
            widgets.hovered.bg_stroke = Stroke::new(1.0_f32, accent);
            widgets.active.fg_stroke.color = text;
            widgets.active.weak_bg_fill = inset;
            widgets.active.bg_stroke = Stroke::new(1.0_f32, accent);
            for widget in [
                &mut widgets.inactive,
                &mut widgets.hovered,
                &mut widgets.active,
            ] {
                widget.corner_radius = 8.into();
                widget.expansion = 0.0;
            }
        });
    }
}

impl Desktop {
    pub(super) fn draw(&mut self, ctx: &egui::Context, frame: &eframe::Frame) {
        let margin = if ctx.content_rect().width() < 580.0 {
            20
        } else {
            30
        };
        let nav_frame = Frame::new()
            .fill(ctx.style().visuals.window_fill)
            .inner_margin(egui::Margin::symmetric(margin, 8));
        egui::TopBottomPanel::top("navigation")
            .frame(nav_frame)
            .show(ctx, |ui| {
                column(ui, |ui| {
                    ui.horizontal(|ui| {
                        self.nav_button(ui, Page::Send);
                        self.nav_button(ui, Page::Receive);
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            self.nav_button(ui, Page::Settings);
                        });
                    });
                });
            });

        let comparing = matches!(self.pairing, Pairing::Compare(_));
        let cable_ready = self
            .connection
            .as_ref()
            .is_some_and(|report| report.interfaces.iter().any(|interface| interface.ready));
        if !comparing {
            egui::TopBottomPanel::top("connection")
                .show_separator_line(false)
                .frame(
                    Frame::new()
                        .fill(ctx.style().visuals.panel_fill)
                        .inner_margin(egui::Margin::symmetric(margin, 8)),
                )
                .show(ctx, |ui| {
                    column(ui, |ui| self.connection_ui(ui, cable_ready))
                });
        }
        let panel =
            Frame::central_panel(&ctx.style()).inner_margin(egui::Margin::symmetric(margin, 12));
        egui::CentralPanel::default().frame(panel).show(ctx, |ui| {
            let body_height = ui.available_height();
            let idle_height = if self.error.is_none()
                && self.result.is_none()
                && !self.sending
                && !self.receiving
                && self.last_sent.is_empty()
            {
                body_height
            } else {
                0.0
            };
            egui::ScrollArea::vertical()
                .id_salt((self.page, comparing, self.incoming.is_some()))
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    column(ui, |ui| {
                        if comparing {
                            self.pairing_code_ui(ui, body_height);
                            return;
                        }
                        self.error_ui(ui);
                        if self.incoming.is_none() || self.page != Page::Receive || self.sending {
                            self.transfer_ui(ui);
                        }
                        match self.page {
                            Page::Receive if self.incoming.is_some() => self.incoming_ui(ui, frame),
                            Page::Receive => self.receive_ui(ui, frame, cable_ready, idle_height),
                            Page::Send => self.send_ui(ui, idle_height),
                            Page::Settings => self.settings_ui(ui, frame),
                        }
                    })
                });
        });
        self.detail_ui(ctx);
    }

    fn nav_button(&mut self, ui: &mut Ui, page: Page) {
        let label = if page == Page::Receive && self.incoming.is_some() {
            "Receive (1)"
        } else {
            page.label()
        };
        let selected = page == self.page;
        let response = ui
            .scope(|ui| {
                let color = ui.visuals().hyperlink_color;
                let selected_fill = ui.visuals().faint_bg_color;
                ui.visuals_mut().selection.bg_fill = selected_fill;
                ui.visuals_mut().selection.stroke.color = color;
                let focus = ui.visuals().widgets.hovered.bg_stroke;
                ui.visuals_mut().widgets.inactive.bg_stroke = Stroke::NONE;
                let icon = ui.id().with(page);
                let atoms = (egui::Atom::custom(icon, vec2(18.0, 18.0)), label);
                let enabled = !matches!(self.pairing, Pairing::Compare(_));
                let button = ui
                    .add_enabled_ui(enabled, |ui| {
                        egui::Button::selectable(selected, atoms)
                            .min_size(vec2(106.0, 38.0))
                            .atom_ui(ui)
                    })
                    .inner;
                if let Some(rect) = button.rect(icon) {
                    let icon_color = if selected {
                        color
                    } else {
                        ui.visuals().text_color()
                    };
                    draw_page_icon(ui, page, rect, icon_color);
                }
                if button.response.has_focus() {
                    ui.painter().rect_stroke(
                        button.response.rect,
                        8,
                        focus,
                        egui::StrokeKind::Inside,
                    );
                }
                button.response
            })
            .inner;
        if response.clicked() && self.page != page {
            self.page = page;
            self.confirm_discard = None;
        }
    }

    fn pairing_code_ui(&mut self, ui: &mut Ui, height: f32) {
        let Pairing::Compare(code) = &self.pairing else {
            return;
        };
        let (matched, differs) = ui
            .vertical_centered(|ui| {
                let caption = centered_caption(
                    ui,
                    "Confirm on both computers if the codes are identical.",
                    ui.visuals().text_color(),
                );
                ui.add_space(centered_padding(
                    height,
                    154.0 + caption.size().y - ui.text_style_height(&TextStyle::Body),
                ));
                heading(ui, "Compare the pairing code");
                ui.label(caption);
                ui.add_space(12.0);
                ui.add(
                    egui::Label::new(RichText::new(code).monospace().size(24.0)).selectable(true),
                );
                ui.add_space(12.0);
                let button_width = 122.0;
                let row_width = 2.0 * button_width + ui.spacing().item_spacing.x;
                ui.allocate_ui_with_layout(
                    vec2(row_width, 36.0),
                    Layout::left_to_right(Align::Center),
                    |ui| {
                        (
                            primary_button_with_width(ui, "Codes match", true, button_width)
                                .clicked(),
                            ui.add(
                                egui::Button::new("Codes differ")
                                    .min_size(vec2(button_width, 36.0)),
                            )
                            .clicked(),
                        )
                    },
                )
                .inner
            })
            .inner;
        if matched || differs {
            self.confirm_pairing(matched);
        }
    }

    fn connection_ui(&mut self, ui: &mut Ui, cable_ready: bool) {
        if let Pairing::Searching(started) = self.pairing
            && let Some(delay) = PAIRING_HINT_DELAY.checked_sub(started.elapsed())
        {
            ui.ctx().request_repaint_after(delay);
        }
        let manual = !self.peer_text.trim().is_empty() || !self.interface_text.trim().is_empty();
        let (title, action) = match self.pairing {
            Pairing::Idle if manual => ("Manual pairing", Some("Pair")),
            Pairing::Idle if cable_ready => ("Cable connected", Some("Pair")),
            Pairing::Idle if self.connection.is_none() => ("Checking the cable…", Some("Pair")),
            Pairing::Idle => ("Connect a data cable", Some("Pair")),
            Pairing::PortBusy => ("Pairing port in use", Some("Cancel")),
            Pairing::Searching(started) if started.elapsed() >= PAIRING_HINT_DELAY => {
                ("Still waiting for the other computer", Some("Cancel"))
            }
            Pairing::Searching(_) => ("Looking for the other computer…", Some("Cancel")),
            Pairing::WaitingForConfirmation => ("Confirm on the other computer", Some("Cancel")),
            Pairing::Paired if self.sending || self.receiving => ("Paired over the cable", None),
            Pairing::Paired => ("Paired over the cable", Some("Disconnect")),
            Pairing::Disconnecting => ("Disconnecting…", None),
            Pairing::Compare(_) => return,
        };
        let idle = matches!(self.pairing, Pairing::Idle);
        let clicked = ui
            .allocate_ui_with_layout(
                vec2(ui.available_width(), 40.0),
                Layout::right_to_left(Align::Center),
                |ui| {
                    let clicked = match action {
                        Some(label) if idle => primary_button(ui, label, true).clicked(),
                        Some(label) => quiet_button(ui, label).clicked(),
                        None => false,
                    };
                    ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
                        let (rect, _) =
                            ui.allocate_exact_size(vec2(8.0, 8.0), egui::Sense::hover());
                        let color = match self.pairing {
                            Pairing::Paired => success_color(ui),
                            _ if cable_ready => ui.visuals().hyperlink_color,
                            _ => ui.visuals().weak_text_color(),
                        };
                        ui.painter().circle_filled(rect.center(), 3.5, color);
                        ui.add(egui::Label::new(title).truncate());
                    });
                    clicked
                },
            )
            .inner;
        if clicked {
            if idle {
                self.begin();
            } else {
                self.request_disconnect();
            }
        }
    }

    fn send_ui(&mut self, ui: &mut Ui, idle_height: f32) {
        let paired = matches!(self.pairing, Pairing::Paired);
        let ready = paired && !self.sending && !self.receiving && self.incoming.is_none();
        let can_choose = self.save_dialog.is_none() && self.picker.is_none();
        if self.queued.is_empty() {
            let dragging = ui.input(|input| !input.raw.hovered_files.is_empty());
            let highlight = if dragging {
                ui.visuals().faint_bg_color
            } else {
                Color32::TRANSPARENT
            };
            Frame::new()
                .fill(highlight)
                .corner_radius(12)
                .show(ui, |ui| {
                    ui.set_min_height(idle_height);
                    ui.vertical_centered(|ui| {
                        ui.add_space(centered_padding(idle_height, 152.0));
                        transfer_icon(ui, Page::Send);
                        ui.add_space(8.0);
                        heading(
                            ui,
                            if dragging {
                                "Drop to add items"
                            } else {
                                "Send files and folders"
                            },
                        );
                        ui.weak("Drop items here, or choose them below.");
                        ui.add_space(8.0);
                        if primary_button(ui, "Add items…", can_choose).clicked() {
                            self.open_picker(ui.ctx());
                        }
                        ui.add_space(24.0);
                    });
                });
        } else {
            ui.horizontal(|ui| {
                heading(ui, &format!("{} selected", item_count(self.queued.len())));
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if ui
                        .add_enabled(
                            can_choose,
                            egui::Button::new("Add items…").frame_when_inactive(false),
                        )
                        .clicked()
                    {
                        self.open_picker(ui.ctx());
                    }
                });
            });
            let mut remove = None;
            let row_height = ui.spacing().interact_size.y;
            let list_height = 4.0 * row_height + 3.0 * ui.spacing().item_spacing.y;
            egui::ScrollArea::vertical()
                .id_salt("files-to-send")
                .max_height(list_height)
                .min_scrolled_height(list_height)
                .show_rows(ui, row_height, self.queued.len(), |ui, rows| {
                    for index in rows {
                        let path = &self.queued[index];
                        ui.push_id(index, |ui| {
                            ui.horizontal(|ui| {
                                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                    if quiet_button(ui, "Remove")
                                        .on_hover_text(format!("Remove {}", display_name(path)))
                                        .clicked()
                                    {
                                        remove = Some(index);
                                    }
                                    ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
                                        ui.add(egui::Label::new(display_name(path)).truncate())
                                            .on_hover_text(path.display().to_string());
                                    });
                                });
                            });
                        });
                    }
                });
            if let Some(index) = remove {
                self.queued.shift_remove_index(index);
            }
            let count = self.queued.len();
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let send_label = format!("Send {}", item_count(count));
                if primary_button(ui, &send_label, ready && count > 0 && can_choose).clicked()
                    && self.send(self.queued.iter().cloned().collect())
                {
                    self.queued.clear();
                }
                ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
                    if self.incoming.is_some() {
                        ui.weak("Review the incoming transfer in Receive.");
                    } else if !paired && count > 0 {
                        ui.weak("Choose Pair on both computers to send.");
                    }
                });
            });
        }
        if !self.last_sent.is_empty() && !self.sending && !self.receiving {
            ui.horizontal_wrapped(|ui| {
                if ui
                    .add_enabled(
                        ready && can_choose,
                        egui::Button::new("Retry last transfer"),
                    )
                    .on_hover_text(
                        "Use the same save folder on the other computer to reuse saved progress.",
                    )
                    .clicked()
                {
                    self.send(self.last_sent.clone());
                }
                if !paired {
                    ui.weak("Pair again to retry.");
                }
            });
        }
    }

    fn receive_ui(
        &mut self,
        ui: &mut Ui,
        frame: &eframe::Frame,
        cable_ready: bool,
        idle_height: f32,
    ) {
        if self.receiving {
            return;
        }
        let (title, detail) = match self.pairing {
            Pairing::Paired => (
                "Ready to receive",
                "Send files from the other computer. They’ll appear here for your approval.",
            ),
            Pairing::PortBusy => (
                "Waiting to pair",
                "Pairing will continue automatically when the port is free.",
            ),
            Pairing::Searching(started) if started.elapsed() >= PAIRING_HINT_DELAY => (
                "Pairing the computers",
                "If both computers are waiting, check the firewall’s rules for this cable.",
            ),
            Pairing::Searching(_) => (
                "Pairing the computers",
                "Choose Pair on the other computer too.",
            ),
            Pairing::WaitingForConfirmation => (
                "Pairing the computers",
                "Confirm the matching code on the other computer.",
            ),
            _ if cable_ready => ("Pair to receive", "Choose Pair on both computers."),
            _ => (
                "Pair to receive",
                "Connect a data cable, then choose Pair on both computers.",
            ),
        };
        let message_height = (idle_height - 48.0).max(0.0);
        ui.vertical_centered(|ui| {
            let caption = centered_caption(ui, detail, ui.visuals().weak_text_color());
            ui.set_min_height(message_height);
            ui.add_space(centered_padding(
                message_height,
                100.0 + caption.size().y - ui.text_style_height(&TextStyle::Body),
            ));
            transfer_icon(ui, Page::Receive);
            ui.add_space(7.0);
            heading(ui, title);
            ui.label(caption);
            ui.add_space(24.0);
        });
        self.save_folder_ui(ui, frame);
    }

    fn save_folder_ui(&mut self, ui: &mut Ui, frame: &eframe::Frame) {
        let can_choose = self.save_dialog.is_none() && !self.receiving && !self.discarding;
        let name = self.destination.as_deref().map(display_name);
        if action_row(
            ui,
            "Save folder",
            Some(name.as_deref().unwrap_or("Choose folder")),
            can_choose,
        )
        .on_hover_ui(|ui| {
            if let Some(folder) = &self.destination {
                ui.label(folder.display().to_string());
            } else {
                ui.label("Choose where incoming files are saved.");
            }
        })
        .clicked()
        {
            self.choose_save_folder(frame);
        }
    }

    fn incoming_ui(&mut self, ui: &mut Ui, frame: &eframe::Frame) {
        let Some(incoming) = &self.incoming else {
            return;
        };
        ui.add_space(8.0);
        heading(ui, "Incoming transfer");
        ui.weak(format!(
            "{} · {}",
            item_count(incoming.names.len()),
            size(incoming.bytes)
        ));
        let row_height = ui.text_style_height(&TextStyle::Body);
        egui::ScrollArea::vertical()
            .id_salt("incoming-files")
            .max_height(140.0)
            .min_scrolled_height(140.0)
            .show_rows(ui, row_height, incoming.names.len(), |ui, range| {
                for name in &incoming.names[range] {
                    ui.add(egui::Label::new(name).truncate())
                        .on_hover_text(name);
                }
            });
        ui.add_space(8.0);
        ui.separator();
        let has_folder = self.destination.is_some();
        if has_folder {
            self.save_folder_ui(ui, frame);
        }
        let can_respond = self.save_dialog.is_none()
            && !self.receiving
            && !self.discarding
            && matches!(self.pairing, Pairing::Paired);
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            let label = if has_folder {
                "Accept transfer"
            } else {
                "Choose save folder…"
            };
            if primary_button(ui, label, can_respond).clicked() {
                if has_folder {
                    self.respond_to_transfer(true);
                } else {
                    self.choose_save_folder(frame);
                }
            }
            if ui
                .add_enabled(
                    can_respond,
                    egui::Button::new("Decline").frame_when_inactive(false),
                )
                .clicked()
            {
                self.respond_to_transfer(false);
            }
            if self.receiving {
                ui.spinner();
            }
        });
    }

    fn transfer_ui(&mut self, ui: &mut Ui) {
        if self.sending || self.receiving {
            self.active_transfer_ui(ui);
            return;
        }
        let (title, summary) = match &self.result {
            None => return,
            Some(TransferResult::Canceled) => ("Transfer canceled", None),
            Some(TransferResult::Completed { receiving, summary }) => (
                if *receiving {
                    "Transfer received"
                } else {
                    "Transfer sent"
                },
                Some(summary),
            ),
        };
        let dismiss = ui
            .scope(|ui| {
                let dismiss = ui
                    .horizontal(|ui| {
                        let color = if summary.is_some() {
                            success_color(ui)
                        } else {
                            ui.visuals().text_color()
                        };
                        ui.colored_label(color, RichText::new(title).strong());
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            quiet_button(ui, "Dismiss").clicked()
                        })
                        .inner
                    })
                    .inner;
                if let Some(summary) = summary {
                    if (!summary.renamed.is_empty() || !summary.skipped.is_empty())
                        && quiet_button(ui, "Renamed and skipped items…").clicked()
                    {
                        self.detail = Some(Detail::TransferResult);
                    }
                } else if !matches!(self.pairing, Pairing::Paired) {
                    ui.weak("Pair again to retry. The receiver can reuse verified files.");
                }
                dismiss
            })
            .inner;
        if dismiss {
            self.result = None;
        }
    }

    fn active_transfer_ui(&mut self, ui: &mut Ui) {
        let stopping = matches!(self.pairing, Pairing::Disconnecting);
        let title = if stopping {
            "Stopping transfer…"
        } else {
            match &self.progress {
                Some(progress) if progress.receiving => "Receiving files",
                Some(_) => "Sending files",
                None if self.receiving => "Preparing download…",
                None => "Preparing transfer…",
            }
        };
        let cancel = ui
            .horizontal(|ui| {
                if stopping || self.progress.is_none() {
                    ui.spinner();
                }
                heading(ui, title);
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    ui.add_enabled(
                        matches!(self.pairing, Pairing::Paired),
                        egui::Button::new("Cancel transfer"),
                    )
                    .on_hover_text("Stops the current transfer and ends pairing.")
                    .clicked()
                })
                .inner
            })
            .inner;
        if cancel {
            self.cancel_transfer();
        }
        if !stopping && let Some(progress) = &self.progress {
            transfer_progress(ui, progress);
        }
        ui.add_space(12.0);
    }

    fn error_ui(&mut self, ui: &mut Ui) {
        let Some(error) = &self.error else {
            return;
        };
        let dismiss = ui
            .scope(|ui| {
                let dismiss = ui
                    .horizontal(|ui| {
                        ui.colored_label(
                            ui.visuals().error_fg_color,
                            RichText::new("Needs attention").strong(),
                        );
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            quiet_button(ui, "Dismiss").clicked()
                        })
                        .inner
                    })
                    .inner;
                ui.label(
                    if error.starts_with("No ready direct USB or Thunderbolt interface") {
                        "Reconnect the cable, then check Connection diagnostics in Settings."
                    } else {
                        error
                    },
                );
                dismiss
            })
            .inner;
        if dismiss {
            self.error = None;
        }
    }

    fn settings_ui(&mut self, ui: &mut Ui, frame: &eframe::Frame) {
        heading(ui, "Receiving");
        self.save_folder_ui(ui, frame);
        let saved =
            (!self.saved.is_empty() && !self.loading_pending).then(|| self.saved.len().to_string());
        if action_row(ui, "Saved transfers", saved.as_deref(), true).clicked() {
            self.detail = Some(Detail::SavedTransfers);
        }
        ui.add_space(20.0);
        heading(ui, "Connection");
        if action_row(ui, "Connection diagnostics", None, true).clicked() {
            self.detail = Some(Detail::Connection);
        }
        let pairing = if self.interface_text.trim().is_empty() && self.peer_text.trim().is_empty() {
            "Automatic"
        } else {
            "Custom"
        };
        if action_row(ui, "Manual pairing", Some(pairing), true).clicked() {
            self.detail = Some(Detail::ManualPairing);
        }
    }

    fn detail_ui(&mut self, ctx: &egui::Context) {
        let Some(detail) = self.detail else {
            return;
        };
        let bounds = ctx.content_rect();
        let width = (bounds.width() - 60.0).min(640.0);
        let height = (bounds.height() - 144.0).clamp(180.0, 450.0);
        let modal = egui::Modal::new(egui::Id::new(("detail", detail)))
            .backdrop_color(dialog_backdrop(ctx))
            .frame(Frame::popup(&ctx.style()).inner_margin(16.0))
            .show(ctx, |ui| {
                ui.set_width(width);
                ui.horizontal(|ui| {
                    heading(ui, detail.title());
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if quiet_button(ui, "Done").clicked() {
                            ui.close();
                        }
                    });
                });
                ui.separator();
                ui.allocate_ui_with_layout(
                    vec2(width, height),
                    Layout::top_down(Align::Min),
                    |ui| {
                        ui.set_min_height(height);
                        match detail {
                            Detail::Connection => self.connection_help_ui(ui, height),
                            Detail::ManualPairing => {
                                detail_scroll("manual", height)
                                    .show(ui, |ui| self.manual_pairing_ui(ui));
                            }
                            Detail::SavedTransfers => self.saved_transfers_ui(ui, height),
                            Detail::TransferResult => self.transfer_result_ui(ui, height),
                        }
                    },
                );
            });
        if modal.should_close() {
            self.detail = None;
            self.confirm_discard = None;
        }
    }

    fn transfer_result_ui(&self, ui: &mut Ui, height: f32) {
        if let Some(TransferResult::Completed { summary, .. }) = &self.result {
            let renamed = summary.renamed.len();
            let row_height = ui.text_style_height(&TextStyle::Body);
            detail_scroll("renamed-and-skipped", height).show_rows(
                ui,
                row_height,
                renamed + summary.skipped.len(),
                |ui, rows| {
                    for index in rows {
                        let line = if index < renamed {
                            format!("Renamed · {}", summary.renamed[index])
                        } else {
                            format!("Skipped · {}", summary.skipped[index - renamed])
                        };
                        detail_line(ui, &line);
                    }
                },
            );
        }
    }

    fn connection_help_ui(&mut self, ui: &mut Ui, height: f32) {
        let Some(report) = &self.connection else {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label("Checking the cable…");
            });
            return;
        };
        ui.horizontal(|ui| {
            ui.selectable_value(
                &mut self.diagnostics_tab,
                DiagnosticsTab::Overview,
                "Overview",
            );
            ui.selectable_value(
                &mut self.diagnostics_tab,
                DiagnosticsTab::Activity,
                "Activity",
            );
            ui.selectable_value(
                &mut self.diagnostics_tab,
                DiagnosticsTab::Hardware,
                "Hardware & kernel",
            );
        });
        let list_height =
            height - 2.0 * (ui.spacing().interact_size.y + ui.spacing().item_spacing.y);
        let scroll = detail_scroll(self.diagnostics_tab, list_height);
        match self.diagnostics_tab {
            DiagnosticsTab::Overview => {
                scroll.show(ui, |ui| self.connection_overview_ui(ui, report));
            }
            DiagnosticsTab::Activity => {
                diagnostic_lines(ui, scroll.stick_to_bottom(true), &report.activity);
            }
            DiagnosticsTab::Hardware => diagnostic_lines(ui, scroll, &report.details),
        }
        self.copy_report_ui(ui, report);
    }

    fn connection_overview_ui(&self, ui: &mut Ui, report: &ConnectionReport) {
        ui.strong(&report.summary);
        for check in &report.checks {
            let color = match check.state {
                CheckState::Ready => success_color(ui),
                CheckState::Blocked => ui.visuals().error_fg_color,
                CheckState::Waiting => ui.visuals().warn_fg_color,
                CheckState::Unknown => ui.visuals().weak_text_color(),
            };
            ui.horizontal_wrapped(|ui| {
                ui.colored_label(color, check.state.label());
                ui.label(RichText::new(check.name).strong());
                ui.label(&check.detail);
            });
        }
        if report.interfaces.iter().all(|interface| !interface.ready) {
            for step in &report.next_steps {
                ui.weak(format!("• {step}"));
            }
        }
        if let Some(detail) = &self.session_detail {
            ui.separator();
            ui.strong("Last pairing message");
            ui.add(egui::Label::new(detail).selectable(true));
        }
    }

    fn copy_report_ui(&self, ui: &mut Ui, report: &ConnectionReport) {
        let copy_id = ui.id().with("copied-report");
        let now = ui.input(|input| input.time);
        let copied = ui
            .data(|data| data.get_temp::<f64>(copy_id))
            .is_some_and(|time| now - time < 2.0);
        ui.horizontal_wrapped(|ui| {
            if ui.button("Copy report").clicked() {
                let mut text = report.text();
                if let Some(detail) = &self.session_detail {
                    text.push_str(&format!("\n\nLast pairing message\n{detail}"));
                }
                ui.ctx().copy_text(text);
                ui.data_mut(|data| data.insert_temp(copy_id, now));
                ui.ctx().request_repaint();
                ui.ctx()
                    .request_repaint_after(std::time::Duration::from_secs(2));
            }
            if copied {
                ui.colored_label(success_color(ui), "Report copied");
            }
        });
    }

    fn manual_pairing_ui(&mut self, ui: &mut Ui) {
        ui.weak("Enter the other computer’s cable IP address on both computers if automatic pairing fails.");
        if self.session.is_some() {
            ui.weak("Disconnect to change these settings.");
        }
        ui.add_enabled_ui(self.session.is_none() && !self.checking_peer, |ui| {
            let label = ui.label("Cable interface");
            if ui
                .add(egui::TextEdit::singleline(&mut self.interface_text).hint_text("Automatic"))
                .labelled_by(label.id)
                .changed()
            {
                self.peer_check = None;
            }
            if let Some(report) = &self.connection {
                for interface in &report.interfaces {
                    ui.horizontal_wrapped(|ui| {
                        if ui.button(&interface.name).clicked() {
                            self.interface_text.clone_from(&interface.name);
                            self.peer_check = None;
                        }
                        ui.weak(if interface.address.is_empty() {
                            &interface.status
                        } else {
                            &interface.address
                        });
                    });
                }
            }
            let label = ui.label("Other computer’s cable IP address");
            if ui
                .add(
                    egui::TextEdit::singleline(&mut self.peer_text)
                        .hint_text("Automatic discovery"),
                )
                .labelled_by(label.id)
                .changed()
            {
                self.peer_check = None;
            }
            ui.horizontal_wrapped(|ui| {
                if ui
                    .add_enabled(
                        nonempty(&self.peer_text).is_some(),
                        egui::Button::new("Test connection"),
                    )
                    .on_hover_text("Choose Pair on the other computer before testing.")
                    .clicked()
                {
                    self.check_peer(ui.ctx());
                }
                if (!self.peer_text.is_empty() || !self.interface_text.is_empty())
                    && quiet_button(ui, "Reset to automatic").clicked()
                {
                    self.peer_text.clear();
                    self.interface_text.clear();
                    self.peer_check = None;
                }
            });
        });
        if self.checking_peer {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label("Testing the cable connection…");
            });
        } else if let Some(result) = &self.peer_check {
            match result {
                Ok(()) => {
                    ui.colored_label(
                        success_color(ui),
                        "The other computer is reachable over the cable.",
                    );
                }
                Err(error) => {
                    ui.colored_label(
                        ui.visuals().error_fg_color,
                        "Couldn’t reach the other computer.",
                    );
                    ui.label("Choose Pair on the other computer and check its cable address.");
                    ui.weak(error);
                }
            }
        }
    }

    fn saved_transfers_ui(&mut self, ui: &mut Ui, height: f32) {
        if self.destination.is_none() {
            ui.weak("Choose the folder where an interrupted transfer was being saved.");
            return;
        }
        if self.loading_pending || self.discarding {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label("Checking saved transfers…");
            });
        } else if self.saved.is_empty() {
            ui.weak("This folder has no interrupted transfers.");
        } else {
            ui.weak("To resume, have the other computer resend the same files and accept them into this folder.");
        }
        let can_discard = self.incoming.is_none()
            && !self.receiving
            && !self.discarding
            && !self.loading_pending
            && self.save_dialog.is_none();
        let row_height = ui.spacing().interact_size.y;
        let list_height =
            (ui.available_height() - row_height - ui.spacing().item_spacing.y).clamp(0.0, height);
        detail_scroll("saved-batches", list_height).show_rows(
            ui,
            row_height,
            self.saved.len(),
            |ui, rows| {
                for index in rows {
                    let item = &self.saved[index];
                    ui.push_id(&item.id, |ui| {
                        ui.allocate_ui_with_layout(
                            vec2(ui.available_width(), row_height),
                            Layout::right_to_left(Align::Center),
                            |ui| {
                                ui.add_space(12.0);
                                if ui
                                    .add_enabled(can_discard, egui::Button::new("Discard…"))
                                    .clicked()
                                {
                                    self.confirm_discard = Some(item.id.clone());
                                }
                                ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
                                    let short_id = item.id.get(..8).unwrap_or(&item.id);
                                    ui.strong(format!("Transfer {short_id}"))
                                        .on_hover_text(&item.id);
                                    ui.weak(format!("{} saved", size(item.bytes)));
                                });
                            },
                        );
                    });
                }
            },
        );
        let mut discard = None;
        ui.horizontal_wrapped(|ui| {
            if let Some(id) = &self.confirm_discard {
                let short_id = id.get(..8).unwrap_or(id);
                ui.label(format!("Discard {short_id}?"));
                let keep = ui.button("Keep").clicked();
                if ui
                    .add_enabled(can_discard, egui::Button::new("Discard progress"))
                    .clicked()
                {
                    discard = Some(id.clone());
                }
                if keep {
                    self.confirm_discard = None;
                }
            } else if ui
                .add_enabled(
                    !self.loading_pending && !self.discarding,
                    egui::Button::new("Refresh"),
                )
                .clicked()
            {
                self.refresh_pending(ui.ctx());
            }
        });
        if let Some(id) = discard {
            self.confirm_discard = None;
            self.discard(ui.ctx(), id);
        }
    }
}

fn detail_scroll(id: impl std::hash::Hash, height: f32) -> egui::ScrollArea {
    egui::ScrollArea::vertical()
        .id_salt(id)
        .max_height(height)
        .auto_shrink([false, false])
}

fn diagnostic_lines(ui: &mut Ui, scroll: egui::ScrollArea, lines: &[String]) {
    let row_height = ui.text_style_height(&TextStyle::Body);
    scroll.show_rows(ui, row_height, lines.len(), |ui, rows| {
        for line in &lines[rows] {
            detail_line(ui, line);
        }
    });
}

fn detail_line(ui: &mut Ui, line: &str) {
    ui.add(egui::Label::new(line).truncate().selectable(true))
        .on_hover_text(line);
}

fn rgb(r: u8, g: u8, b: u8) -> Color32 {
    Color32::from_rgb(r, g, b)
}

fn success_color(ui: &Ui) -> Color32 {
    if ui.visuals().dark_mode {
        rgb(113, 223, 176)
    } else {
        rgb(20, 112, 73)
    }
}

fn column(ui: &mut Ui, body: impl FnOnce(&mut Ui)) {
    let width = ui.available_width().min(CONTENT_WIDTH);
    ui.with_layout(Layout::top_down(Align::Center), |ui| {
        ui.allocate_ui_with_layout(vec2(width, 0.0), Layout::top_down(Align::Min), |ui| {
            ui.set_width(width);
            body(ui);
        });
    });
}

pub(super) fn heading(ui: &mut Ui, title: &str) {
    ui.heading(RichText::new(title).strong());
}

pub(super) fn primary_button(ui: &mut Ui, label: &str, enabled: bool) -> egui::Response {
    primary_button_with_width(ui, label, enabled, 80.0)
}

fn primary_button_with_width(
    ui: &mut Ui,
    label: &str,
    enabled: bool,
    width: f32,
) -> egui::Response {
    ui.scope(|ui| {
        if enabled {
            let focus = ui.visuals().hyperlink_color;
            let widgets = &mut ui.visuals_mut().widgets;
            for (visual, fill, border) in [
                (&mut widgets.inactive, BLUE, BLUE),
                (&mut widgets.hovered, rgb(34, 74, 184), focus),
                (&mut widgets.active, rgb(30, 63, 156), focus),
            ] {
                visual.weak_bg_fill = fill;
                visual.bg_stroke = Stroke::new(1.0_f32, border);
                visual.fg_stroke.color = Color32::WHITE;
            }
        }
        ui.with_layout(ui.layout().with_main_align(Align::Center), |ui| {
            ui.add_enabled(
                enabled,
                egui::Button::new(RichText::new(label).strong()).min_size(vec2(width, 36.0)),
            )
        })
        .inner
    })
    .inner
}

fn centered_padding(available: f32, content: f32) -> f32 {
    ((available - content) * 0.5).max(16.0)
}

fn centered_caption(ui: &Ui, text: &str, color: Color32) -> std::sync::Arc<egui::Galley> {
    let mut job = egui::text::LayoutJob::simple(
        text.to_owned(),
        TextStyle::Body.resolve(ui.style()),
        color,
        ui.available_width(),
    );
    job.halign = Align::Center;
    ui.fonts_mut(|fonts| fonts.layout_job(job))
}

pub(super) fn quiet_button(ui: &mut Ui, label: &str) -> egui::Response {
    ui.add(egui::Button::new(label).frame_when_inactive(false))
}

fn action_row(ui: &mut Ui, label: &str, value: Option<&str>, enabled: bool) -> egui::Response {
    let arrow = ui.id().with(label);
    let icon = egui::Atom::custom(arrow, vec2(14.0, 14.0));
    let row = if let Some(value) = value {
        egui::Button::new((label, egui::Atom::grow(), RichText::new(value).weak(), icon))
    } else {
        egui::Button::new(label).right_text(icon)
    };
    let button = ui
        .add_enabled_ui(enabled, |ui| {
            row.frame_when_inactive(false)
                .truncate()
                .min_size(vec2(ui.available_width(), 38.0))
                .atom_ui(ui)
        })
        .inner;
    if let Some(rect) = button.rect(arrow) {
        chevron(ui, rect, ui.visuals().weak_text_color());
    }
    button.response
}

pub(super) fn dialog_backdrop(ctx: &egui::Context) -> Color32 {
    Color32::from_black_alpha(if ctx.style().visuals.dark_mode {
        128
    } else {
        55
    })
}

pub(super) fn chevron(ui: &Ui, rect: egui::Rect, color: Color32) {
    let center = rect.center();
    ui.painter().add(egui::Shape::line(
        vec![
            center + vec2(-2.0, -4.0),
            center + vec2(2.0, 0.0),
            center + vec2(-2.0, 4.0),
        ],
        Stroke::new(1.5_f32, color),
    ));
}

fn transfer_icon(ui: &mut Ui, page: Page) {
    let (rect, _) = ui.allocate_exact_size(vec2(34.0, 34.0), egui::Sense::hover());
    draw_page_icon(ui, page, rect, ui.visuals().hyperlink_color);
}

fn draw_page_icon(ui: &Ui, page: Page, rect: egui::Rect, color: Color32) {
    let center = rect.center();
    let scale = rect.width() / 24.0;
    let stroke = Stroke::new((scale * 1.8).max(1.4), color);
    let line = |points: &[(f32, f32)]| {
        ui.painter().add(egui::Shape::line(
            points
                .iter()
                .map(|&(x, y)| center + scale * vec2(x, y))
                .collect(),
            stroke,
        ));
    };
    if page == Page::Settings {
        line(&[(-9.0, -4.0), (9.0, -4.0)]);
        line(&[(-9.0, 4.0), (9.0, 4.0)]);
        line(&[(-3.0, -7.0), (-3.0, -1.0)]);
        line(&[(4.0, 1.0), (4.0, 7.0)]);
    } else {
        line(&[(-9.0, 3.0), (-9.0, 9.0), (9.0, 9.0), (9.0, 3.0)]);
        line(&[(0.0, -8.0), (0.0, 4.0)]);
        if page == Page::Receive {
            line(&[(-4.0, 0.0), (0.0, 4.0), (4.0, 0.0)]);
        } else {
            line(&[(-4.0, -4.0), (0.0, -8.0), (4.0, -4.0)]);
        }
    }
}

pub(super) fn item_count(count: usize) -> String {
    if count == 1 {
        "1 item".into()
    } else {
        format!("{count} items")
    }
}

fn transfer_progress(ui: &mut Ui, progress: &Progress) {
    let remaining = progress.total.saturating_sub(progress.done);
    let fraction = if remaining == 0 {
        1.0
    } else {
        (progress.done as f64 / progress.total as f64) as f32
    };
    let percent = if remaining == 0 {
        100
    } else {
        u128::from(progress.done) * 100 / u128::from(progress.total)
    };
    let now = Instant::now();
    let rate = progress.rate(now);
    if rate.is_some() {
        ui.ctx().request_repaint_after(
            RATE_STALE_AFTER.saturating_sub(now.duration_since(progress.sampled_at)),
        );
    }
    let time = if remaining == 0 {
        "Finishing transfer…".into()
    } else if let Some(rate) = rate {
        let seconds = (remaining as f64 / rate).ceil() as u64;
        format!("About {} left", duration_label(seconds))
    } else {
        "Estimating time remaining…".into()
    };
    ui.strong(format!("{percent}% · {time}"));
    let painted_fraction =
        ui.ctx()
            .animate_value_with_time(ui.id().with(progress.started), fraction, 0.12);
    ui.scope(|ui| {
        ui.visuals_mut().extreme_bg_color = ui.visuals().faint_bg_color;
        ui.add(
            egui::ProgressBar::new(painted_fraction)
                .desired_height(12.0)
                .fill(BLUE),
        );
    });
    let mut bytes = format!("{} of {}", size(progress.done), size(progress.total));
    if let Some(rate) = rate {
        bytes.push_str(&format!(" · {}/s", size(rate as u64)));
    }
    ui.weak(bytes);
}

fn duration_label(seconds: u64) -> String {
    if seconds >= 3600 {
        format!("{}h {}m", seconds / 3600, seconds / 60 % 60)
    } else if seconds >= 60 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}
