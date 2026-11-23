use super::{Desktop, Page, Pairing, Pick, display_name, nonempty, size};
use eframe::egui::{
    self, Align, Color32, FontFamily, FontId, Frame, Layout, RichText, Stroke, TextStyle, Ui, vec2,
};
use std::time::Duration;
use usb_direct_transfer::CheckState;

const BLUE: Color32 = Color32::from_rgb(39, 85, 207);
const CONTENT_WIDTH: f32 = 640.0;
const PAIRING_HINT_DELAY: Duration = Duration::from_secs(8);

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
                    rgb(250, 251, 253),
                    rgb(236, 240, 248),
                    rgb(224, 235, 255),
                    rgb(209, 217, 232),
                    rgb(24, 36, 59),
                    rgb(78, 94, 120),
                    BLUE,
                ),
                egui::Theme::Dark => (
                    rgb(22, 27, 36),
                    rgb(30, 37, 49),
                    rgb(41, 55, 80),
                    rgb(59, 73, 100),
                    rgb(239, 243, 252),
                    rgb(167, 181, 205),
                    rgb(153, 182, 255),
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
            style.spacing.scroll = egui::style::ScrollStyle::solid();

            let visuals = &mut style.visuals;
            visuals.panel_fill = canvas;
            visuals.window_fill = surface;
            visuals.extreme_bg_color = canvas;
            visuals.faint_bg_color = inset;
            visuals.weak_text_color = Some(muted);
            visuals.hyperlink_color = accent;
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
        let narrow = ctx.content_rect().width() < 580.0;
        let nav_frame = Frame::new()
            .fill(ctx.style().visuals.window_fill)
            .inner_margin(egui::Margin::symmetric(14, if narrow { 8 } else { 18 }));
        if narrow {
            egui::TopBottomPanel::bottom("navigation-tabs")
                .frame(nav_frame)
                .show(ctx, |ui| {
                    ui.columns_const(|[receive, send, settings]| {
                        self.nav_button(receive, Page::Receive, true);
                        self.nav_button(send, Page::Send, true);
                        self.nav_button(settings, Page::Settings, true);
                    });
                });
        } else {
            egui::SidePanel::left("navigation-rail")
                .exact_width(166.0)
                .resizable(false)
                .frame(nav_frame)
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        cable_icon(ui);
                        ui.label(RichText::new("USB Direct\nTransfer").size(17.0).strong());
                    });
                    ui.add_space(20.0);
                    for page in [Page::Receive, Page::Send, Page::Settings] {
                        self.nav_button(ui, page, false);
                    }
                });
        }

        let margin = if narrow { 20 } else { 30 };
        let panel =
            Frame::central_panel(&ctx.style()).inner_margin(egui::Margin::symmetric(margin, 24));
        egui::CentralPanel::default().frame(panel).show(ctx, |ui| {
            let comparing = matches!(self.pairing, Pairing::Compare(_));
            let cable_ready = self
                .connection
                .as_ref()
                .is_some_and(|report| report.interfaces.iter().any(|interface| interface.ready));
            egui::ScrollArea::vertical()
                .id_salt((self.page, comparing, self.incoming.is_some()))
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    column(ui, |ui| {
                        if comparing {
                            self.pairing_code_ui(ui);
                            return;
                        }
                        ui.heading(RichText::new(self.page.label()).size(30.0).strong());
                        self.connection_ui(ui, cable_ready);
                        ui.separator();
                        self.error_ui(ui);
                        if self.incoming.is_none() || self.page != Page::Receive {
                            self.transfer_ui(ui);
                        }
                        match self.page {
                            Page::Receive if self.incoming.is_some() => self.incoming_ui(ui, frame),
                            Page::Receive => self.receive_ui(ui, frame, cable_ready),
                            Page::Send => self.send_ui(ui, frame),
                            Page::Settings => self.settings_ui(ui, frame),
                        }
                    })
                });
        });
    }

    fn nav_button(&mut self, ui: &mut Ui, page: Page, centered: bool) {
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
                let layout = Layout::top_down(if centered { Align::Center } else { Align::Min });
                ui.with_layout(layout, |ui| {
                    let icon = ui.id().with(page);
                    let atoms = (egui::Atom::custom(icon, vec2(18.0, 18.0)), label);
                    let enabled = !matches!(self.pairing, Pairing::Compare(_));
                    let width = ui.available_width();
                    let button = ui
                        .add_enabled_ui(enabled, |ui| {
                            egui::Button::selectable(selected, atoms)
                                .min_size(vec2(width, 40.0))
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
                .inner
            })
            .inner;
        if response.clicked() && self.page != page {
            self.page = page;
            self.confirm_discard = None;
        }
    }

    fn pairing_code_ui(&mut self, ui: &mut Ui) {
        let Pairing::Compare(code) = &self.pairing else {
            return;
        };
        let (matched, differs) = ui
            .vertical_centered(|ui| {
                ui.add_space(30.0);
                heading(ui, "Compare the pairing code");
                ui.label("Confirm on both computers if the codes are identical.");
                ui.add_space(12.0);
                ui.add(
                    egui::Label::new(RichText::new(code).monospace().size(24.0)).selectable(true),
                );
                ui.add_space(12.0);
                ui.columns_const(|[confirm, decline]| {
                    (
                        confirm
                            .with_layout(Layout::right_to_left(Align::Center), |ui| {
                                primary_button(ui, "Codes match", true).clicked()
                            })
                            .inner,
                        decline
                            .with_layout(Layout::left_to_right(Align::Center), |ui| {
                                ui.add(egui::Button::new("Codes differ").min_size(vec2(0.0, 36.0)))
                                    .clicked()
                            })
                            .inner,
                    )
                })
            })
            .inner;
        if matched || differs {
            self.confirm_pairing(matched);
        }
    }

    fn connection_ui(&mut self, ui: &mut Ui, cable_ready: bool) {
        let manual = !self.peer_text.trim().is_empty() || !self.interface_text.trim().is_empty();
        let (title, action) = match self.pairing {
            Pairing::Idle if manual => ("Manual pairing", Some("Pair")),
            Pairing::Idle if cable_ready => ("Cable connected", Some("Pair")),
            Pairing::Idle if self.connection.is_none() => ("Checking the cable…", Some("Pair")),
            Pairing::Idle => ("Connect a data cable", Some("Pair")),
            Pairing::Searching(started) if started.elapsed() >= PAIRING_HINT_DELAY => {
                ("Still waiting for the other computer", Some("Cancel"))
            }
            Pairing::Searching(_) => ("Looking for the other computer…", Some("Cancel")),
            Pairing::WaitingForConfirmation => ("Confirm on the other computer", Some("Cancel")),
            Pairing::Paired => ("Paired over the cable", Some("Disconnect")),
            Pairing::Disconnecting => ("Disconnecting…", None),
            Pairing::Compare(_) => return,
        };
        let idle = matches!(self.pairing, Pairing::Idle);
        let clicked = ui
            .horizontal(|ui| {
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
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
                        ui.label(title);
                    });
                    clicked
                })
                .inner
            })
            .inner;
        if clicked {
            if idle {
                self.begin();
            } else {
                self.request_disconnect();
            }
        }
    }

    fn send_ui(&mut self, ui: &mut Ui, frame: &eframe::Frame) {
        let paired = matches!(self.pairing, Pairing::Paired);
        let ready = paired && !self.sending && !self.receiving && self.incoming.is_none();
        let can_choose = self.dialog.is_none();
        if self.queued.is_empty() {
            ui.vertical_centered(|ui| {
                ui.add_space(36.0);
                transfer_icon(ui, Page::Send);
                ui.add_space(7.0);
                let label = if ui.input(|input| !input.raw.hovered_files.is_empty()) {
                    "Drop to add your files"
                } else {
                    "Drop files or folders here"
                };
                heading(ui, label);
                ui.add_space(7.0);
                self.add_items_menu(ui, frame, true);
            });
        } else {
            ui.add_space(8.0);
            heading(ui, &format!("{} selected", item_count(self.queued.len())));
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
                self.queued.remove(index);
            }
            let count = self.queued.len();
            ui.horizontal_wrapped(|ui| {
                let send_label = format!("Send {}", item_count(count));
                if primary_button(ui, &send_label, ready && count > 0 && can_choose).clicked()
                    && self.send(self.queued.clone())
                {
                    self.queued.clear();
                }
                self.add_items_menu(ui, frame, false);
            });
            if self.incoming.is_some() {
                ui.weak("Review the incoming transfer in Receive.");
            } else if !paired && count > 0 {
                ui.weak("Choose Pair on both computers to send.");
            }
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

    fn add_items_menu(&mut self, ui: &mut Ui, frame: &eframe::Frame, primary: bool) {
        let enabled = self.dialog.is_none();
        let label = "Add items…";
        let button = if primary {
            primary_button(ui, label, enabled)
        } else {
            ui.add_enabled(enabled, egui::Button::new(label))
        };
        egui::Popup::menu(&button).show(|ui| {
            for (label, pick) in [("Files…", Pick::Files), ("Folder…", Pick::Folder)] {
                if ui.button(label).clicked() {
                    ui.close();
                    self.open_dialog(pick, frame);
                }
            }
        });
    }

    fn receive_ui(&mut self, ui: &mut Ui, frame: &eframe::Frame, cable_ready: bool) {
        if !self.receiving {
            ui.vertical_centered(|ui| {
                ui.add_space(35.0);
                transfer_icon(ui, Page::Receive);
                ui.add_space(7.0);
                let (title, detail) = match self.pairing {
                    Pairing::Paired => ("Ready to receive", "Send files from the other computer. They’ll appear here for your approval."),
                    Pairing::Searching(started) if started.elapsed() >= PAIRING_HINT_DELAY => (
                        "Pairing the computers",
                        "If Pair is open on both computers, check the app’s access to the direct cable in your firewall settings.",
                    ),
                    Pairing::Searching(_) => ("Pairing the computers", "Choose Pair on the other computer too."),
                    Pairing::WaitingForConfirmation => ("Pairing the computers", "Confirm the matching code on the other computer."),
                    _ if cable_ready => ("Pair to receive", "Choose Pair on both computers."),
                    _ => ("Pair to receive", "Connect both computers with a data cable and choose Pair on each one."),
                };
                heading(ui, title);
                ui.weak(detail);
                ui.add_space(32.0);
            });
            ui.separator();
        }
        self.save_folder_ui(ui, frame);
    }

    fn save_folder_ui(&mut self, ui: &mut Ui, frame: &eframe::Frame) {
        let can_choose = self.dialog.is_none() && !self.receiving && !self.discarding;
        let clicked = ui
            .horizontal(|ui| {
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    let label = if self.destination.is_some() {
                        "Change…"
                    } else {
                        "Choose…"
                    };
                    let clicked = ui
                        .add_enabled(can_choose, egui::Button::new(label))
                        .on_hover_text("Choose where to save incoming files")
                        .clicked();
                    ui.with_layout(Layout::left_to_right(Align::Center), |ui| {
                        ui.label(RichText::new("Save folder").strong());
                        if let Some(folder) = &self.destination {
                            ui.add(
                                egui::Label::new(RichText::new(display_name(folder)).weak())
                                    .truncate(),
                            )
                            .on_hover_text(folder.display().to_string());
                        } else {
                            ui.weak("Choose when files arrive");
                        }
                    });
                    clicked
                })
                .inner
            })
            .inner;
        if clicked {
            self.open_dialog(Pick::Destination, frame);
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
        let can_respond = self.dialog.is_none()
            && !self.receiving
            && !self.discarding
            && matches!(self.pairing, Pairing::Paired);
        ui.horizontal_wrapped(|ui| {
            let label = if has_folder {
                "Accept transfer"
            } else {
                "Choose save folder…"
            };
            if primary_button(ui, label, can_respond).clicked() {
                if has_folder {
                    self.respond_to_transfer(true);
                } else {
                    self.open_dialog(Pick::Destination, frame);
                }
            }
            if ui
                .add_enabled(can_respond, egui::Button::new("Decline"))
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
            ui.scope(|ui| {
                if let Some(progress) = &self.progress {
                    heading(
                        ui,
                        if progress.receiving {
                            "Receiving files"
                        } else {
                            "Sending files"
                        },
                    );
                    let fraction = if progress.total == 0 {
                        1.0
                    } else {
                        (progress.done as f64 / progress.total as f64).min(1.0) as f32
                    };
                    let elapsed = progress.started.elapsed().as_secs_f64();
                    let transferred = progress.done.saturating_sub(progress.initial);
                    let mut label = format!("{} of {}", size(progress.done), size(progress.total));
                    if elapsed > 0.05 && transferred > 0 {
                        label.push_str(&format!(
                            " · {}/s",
                            size((transferred as f64 / elapsed) as u64)
                        ));
                    }
                    ui.weak(label);
                    ui.add(
                        egui::ProgressBar::new(fraction)
                            .desired_height(8.0)
                            .fill(BLUE),
                    );
                } else {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        heading(
                            ui,
                            if self.receiving {
                                "Receiving files…"
                            } else {
                                "Preparing transfer…"
                            },
                        );
                    });
                    if self.sending && !self.receiving {
                        ui.weak("The other computer needs to accept your transfer.");
                    }
                }
            });
            return;
        }
        let Some((receiving, summary)) = &self.result else {
            return;
        };
        let dismiss = ui
            .scope(|ui| {
                let dismiss = ui
                    .horizontal(|ui| {
                        ui.colored_label(
                            success_color(ui),
                            RichText::new(if *receiving {
                                "Transfer received"
                            } else {
                                "Transfer sent"
                            })
                            .strong(),
                        );
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            quiet_button(ui, "Dismiss").clicked()
                        })
                        .inner
                    })
                    .inner;
                if !summary.renamed.is_empty() || !summary.skipped.is_empty() {
                    egui::CollapsingHeader::new("Renamed and skipped items").show(ui, |ui| {
                        for name in &summary.renamed {
                            ui.label(format!("Renamed · {name}"));
                        }
                        for name in &summary.skipped {
                            ui.label(format!("Skipped · {name}"));
                        }
                    });
                }
                dismiss
            })
            .inner;
        if dismiss {
            self.result = None;
        }
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
        ui.add_space(8.0);
        heading(ui, "Receiving");
        self.save_folder_ui(ui, frame);
        egui::CollapsingHeader::new("Saved transfers").show(ui, |ui| {
            self.saved_transfers_ui(ui);
        });
        ui.add_space(10.0);
        ui.separator();
        ui.add_space(8.0);
        heading(ui, "Connection");
        egui::CollapsingHeader::new("Connection diagnostics").show(ui, |ui| {
            self.connection_help_ui(ui);
        });
        egui::CollapsingHeader::new("Manual pairing").show(ui, |ui| {
            self.manual_pairing_ui(ui);
        });
    }

    fn connection_help_ui(&self, ui: &mut Ui) {
        let Some(report) = &self.connection else {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label("Checking the cable…");
            });
            return;
        };
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
        ui.separator();
        if let Some(detail) = &self.session_detail {
            egui::CollapsingHeader::new("Last pairing message").show(ui, |ui| {
                ui.add(egui::Label::new(detail).selectable(true));
            });
        }
        egui::CollapsingHeader::new("Connection activity").show(ui, |ui| {
            egui::ScrollArea::vertical()
                .id_salt("connection-activity")
                .max_height(130.0)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    for event in &report.activity {
                        ui.add(egui::Label::new(event).selectable(true));
                    }
                });
        });
        egui::CollapsingHeader::new("Hardware and kernel details").show(ui, |ui| {
            egui::ScrollArea::vertical()
                .id_salt("connection-details")
                .max_height(200.0)
                .show(ui, |ui| {
                    for line in &report.details {
                        ui.add(egui::Label::new(line).selectable(true));
                    }
                });
        });
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

    fn saved_transfers_ui(&mut self, ui: &mut Ui) {
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
            && self.dialog.is_none();
        let mut discard = None;
        for item in &self.saved {
            ui.push_id(&item.id, |ui| {
                let short_id: String = item.id.chars().take(8).collect();
                ui.horizontal_wrapped(|ui| {
                    ui.strong(format!("Transfer {short_id}"))
                        .on_hover_text(&item.id);
                    ui.weak(format!("{} saved", size(item.bytes)));
                    if self.confirm_discard.as_ref() != Some(&item.id)
                        && ui
                            .add_enabled(can_discard, egui::Button::new("Discard…"))
                            .clicked()
                    {
                        self.confirm_discard = Some(item.id.clone());
                    }
                });
                if self.confirm_discard.as_ref() == Some(&item.id) {
                    ui.label("Discard this transfer’s saved progress?");
                    ui.horizontal_wrapped(|ui| {
                        if ui.button("Keep").clicked() {
                            self.confirm_discard = None;
                        }
                        if ui
                            .add_enabled(can_discard, egui::Button::new("Discard progress"))
                            .clicked()
                        {
                            discard = Some(item.id.clone());
                        }
                    });
                }
            });
        }
        if let Some(id) = discard {
            self.confirm_discard = None;
            self.discard(ui.ctx(), id);
        }
        if ui
            .add_enabled(
                !self.loading_pending && !self.discarding,
                egui::Button::new("Refresh"),
            )
            .clicked()
        {
            self.refresh_pending(ui.ctx());
        }
    }
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

fn heading(ui: &mut Ui, title: &str) {
    ui.heading(RichText::new(title).strong());
}

fn primary_button(ui: &mut Ui, label: &str, enabled: bool) -> egui::Response {
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
                egui::Button::new(RichText::new(label).strong()).min_size(vec2(80.0, 36.0)),
            )
        })
        .inner
    })
    .inner
}

fn quiet_button(ui: &mut Ui, label: &str) -> egui::Response {
    ui.add(egui::Button::new(label).frame_when_inactive(false))
}

fn cable_icon(ui: &mut Ui) {
    let (rect, _) = ui.allocate_exact_size(vec2(32.0, 32.0), egui::Sense::hover());
    let painter = ui.painter();
    painter.rect_filled(rect, 8, ui.visuals().faint_bg_color);
    let center = rect.center();
    let stroke = Stroke::new(1.5_f32, ui.visuals().hyperlink_color);
    for x in [-6.0, 6.0] {
        let end = egui::Rect::from_center_size(center + vec2(x, 0.0), vec2(6.0, 9.0));
        painter.rect_stroke(end, 1, stroke, egui::StrokeKind::Inside);
    }
    painter.line_segment([center - vec2(3.0, 0.0), center + vec2(3.0, 0.0)], stroke);
}

fn transfer_icon(ui: &mut Ui, page: Page) {
    let (rect, _) = ui.allocate_exact_size(vec2(68.0, 68.0), egui::Sense::hover());
    let color = ui.visuals().hyperlink_color;
    ui.painter().circle_filled(
        rect.center(),
        rect.width() / 2.0,
        ui.visuals().faint_bg_color,
    );
    let symbol = egui::Rect::from_center_size(rect.center(), vec2(30.0, 30.0));
    draw_page_icon(ui, page, symbol, color);
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

fn item_count(count: usize) -> String {
    if count == 1 {
        "1 item".into()
    } else {
        format!("{count} items")
    }
}
