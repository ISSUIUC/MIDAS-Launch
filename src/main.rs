#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod file_picker;
mod left;
mod computation;

use egui::{Align, Context, FontFamily, Layout, RichText, Visuals, Widget, Align2, Direction, WidgetText};
use egui_plot as plot;
use eframe::{Frame, Storage};
use egui_toast::{Toast, ToastKind, ToastOptions, Toasts};
use semver::Version;
use dataframe::{DataFrameView, VirtualColumn};
use crate::computation::Computation;
use crate::left::Left;

const RELEASES_URL: &'static str = "https://api.github.com/repos/ISSUIUC/MIDAS-Launch/releases";


#[derive(Copy, Clone, PartialEq, Eq)]
enum VisualState {
    Plot,
    Table
}

struct TableTab {

}

struct PlotTab {
    // plots: Option<PlotInfo>,

    x_idx: VirtualColumn,
    y_idx: VirtualColumn,
    resolution: f64,

    cache: Option<((u64, VirtualColumn, VirtualColumn, f64), Vec<[f64; 2]>)>
}

impl TableTab {
    fn new(_cc: &eframe::CreationContext) -> TableTab {
        TableTab {

        }
    }
}


impl PlotTab {
    fn new(_cc: &eframe::CreationContext) -> PlotTab {
        PlotTab {
            x_idx: VirtualColumn::RowIndex,
            y_idx: VirtualColumn::RowIndex,
            resolution: 4.0,

            cache: None
        }
    }
}

struct DataShared {
    complete_data: DataFrameView,
    shown_data: DataFrameView,

    version: u64
}


impl DataShared {
    fn new(data: DataFrameView) -> DataShared {
        DataShared {
            complete_data: data.clone(),
            shown_data: data,

            version: 0
        }
    }
}

enum UpdateInfo {
    LatestVersion,
    UpdateAvailable(Version)
}

struct App {
    left: Left,

    visual_state: VisualState,
    table_tab: TableTab,
    plot_tab: PlotTab,

    is_maximized: bool,

    shared: Option<DataShared>,

    check_for_update: Computation<UpdateInfo, ()>,
}

struct UpdateContext<'a> {
    ctx: &'a Context,
    toasts: &'a mut Toasts,
    data: &'a mut Option<DataShared>
}

impl<'a> UpdateContext<'a> {
    pub fn error_toast(&mut self, message: impl Into<WidgetText>) {
        self.toasts.add(Toast {
            kind: ToastKind::Error,
            text: message.into(),
            ..Default::default()
        });
    }
}

fn check_for_update() -> Option<UpdateInfo> {
    let mut response = ureq::get(RELEASES_URL).call().ok()?;
    let body = response.body_mut().read_json::<serde_json::Value>().ok()?;

    let tag_name = body.get(0)?.get("tag_name")?.as_str()?;
    let latest_version = Version::parse(tag_name.strip_prefix('v')?).ok()?;

    let this_version = Version::parse(env!("CARGO_PKG_VERSION")).ok()?;

    if this_version < latest_version {
        Some(UpdateInfo::UpdateAvailable(latest_version))
    } else {
        Some(UpdateInfo::LatestVersion)
    }
}

impl App {
    fn new(cc: &eframe::CreationContext) -> App {
        let was_maximized = cc.storage.and_then(|store| store.get_string("was-maximized")).map_or(false, |s| s == "true");
        if was_maximized {
            cc.egui_ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(true));
        }

        App {
            left: Left::new(cc),

            shared: None,

            visual_state: VisualState::Table,
            table_tab: TableTab::new(cc),
            plot_tab: PlotTab::new(cc),

            is_maximized: was_maximized,

            check_for_update: Computation::begin_new(cc.egui_ctx.clone(), || check_for_update().ok_or(()))
        }
    }
}


impl eframe::App for App {
    fn update(&mut self, ctx: &Context, _frame: &mut Frame) {
        ctx.set_visuals(Visuals::light());
        self.is_maximized = ctx.input(|state| state.viewport().maximized.unwrap_or(false));

        let mut toasts = Toasts::new()
            .anchor(Align2::LEFT_BOTTOM, (5.0, -5.0))
            .direction(Direction::BottomUp);


        self.check_for_update.check_complete();

        if let Some(result) = self.check_for_update.take_if_done() {
            match result {
                Ok(UpdateInfo::LatestVersion) => {
                    toasts.add(Toast {
                        text: "No updates available.".into(),
                        kind: ToastKind::Info,
                        options: ToastOptions::default()
                            .duration_in_seconds(5.0)
                            .show_progress(true),
                        ..Default::default()
                    })
                }
                Ok(UpdateInfo::UpdateAvailable(latest)) => {
                    toasts.add(Toast {
                        text: format!("Update to version {latest} available.").into(),
                        kind: ToastKind::Warning,
                        ..Default::default()
                    })
                }
                Err(()) => {
                    toasts.add(Toast {
                        text: "Could not fetch updates.".into(),
                        kind: ToastKind::Warning,
                        options: ToastOptions::default()
                            .duration_in_seconds(5.0)
                            .show_progress(true),
                        ..Default::default()
                    })
                }
            };
        }

        self.left.draw(UpdateContext { ctx, toasts: &mut toasts, data: &mut self.shared });

        if let Some(shared) = &mut self.shared {
            egui::SidePanel::right("plot-table-panel")
                .resizable(true)
                .default_width(180.0)
                .width_range(180.0..=480.0)
                .show_animated(ctx, true,|ui| {
                    ui.add_space(3.0);
                    ui.columns(2, |cols| {
                        cols[0].vertical_centered_justified(|ui| {
                            ui.selectable_value(&mut self.visual_state, VisualState::Table, "Table");
                        });
                        cols[1].vertical_centered_justified(|ui| {
                            ui.selectable_value(&mut self.visual_state, VisualState::Plot, "Plot");
                        })
                    });
                    ui.separator();

                    match self.visual_state {
                        VisualState::Table => {
                            let _ = &self.table_tab;
                        }
                        VisualState::Plot => {
                            egui::Frame::group(ui.style())
                                .show(ui, |ui| {
                                    egui::ComboBox::new("x-axis-combo","X axis")
                                        .selected_text(shared.shown_data.col_name(self.plot_tab.x_idx))
                                        .show_ui(ui, |ui| {
                                            ui.selectable_value(&mut self.plot_tab.x_idx, VirtualColumn::RowIndex, "<row number>");
                                            for (idx, col_name) in shared.shown_data.col_names().enumerate() {
                                                ui.selectable_value(&mut self.plot_tab.x_idx, VirtualColumn::Column(idx), col_name);
                                            }
                                        });

                                    egui::ComboBox::new("y-axis-combo","Y axis")
                                        .selected_text(shared.shown_data.col_name(self.plot_tab.y_idx))
                                        .show_ui(ui, |ui| {
                                            ui.selectable_value(&mut self.plot_tab.y_idx, VirtualColumn::RowIndex, "<row number>");
                                            for (idx, col_name) in shared.shown_data.col_names().enumerate() {
                                                ui.selectable_value(&mut self.plot_tab.y_idx, VirtualColumn::Column(idx), col_name);
                                            }
                                        });

                                    ui.horizontal(|ui| {
                                        ui.label("Resolution");

                                        ui.add(egui::Slider::new(&mut self.plot_tab.resolution, 0.1..=100.0)
                                            .logarithmic(true))
                                    });
                                });
                        }
                    }
                });
        } else {
            egui::SidePanel::right("plot-table-panel")
                .resizable(true)
                .default_width(180.0)
                .width_range(180.0..=480.0)
                .show_animated(ctx, false, |_| ());
        }


        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(data_shared) = &self.shared {
                match self.visual_state {
                    VisualState::Table => {
                        let data = &data_shared.shown_data;

                        egui::ScrollArea::horizontal().show(ui, |ui| {
                            egui_extras::TableBuilder::new(ui)
                                .auto_shrink([false, false])
                                .max_scroll_height(f32::INFINITY)
                                .resizable(true)
                                .striped(true)
                                .columns(egui_extras::Column::auto().clip(true), data.shape().cols)
                                .cell_layout(Layout::right_to_left(Align::Center))
                                .header(28.0, |mut row| {
                                    for col_name in data.col_names() {
                                        row.col(|ui| {
                                            egui::Label::new(RichText::new(col_name).family(FontFamily::Monospace).size(18.0)).truncate().ui(ui);
                                        });
                                    }
                                })
                                .body(|body| {
                                    let num_rows = data.shape().rows;
                                    body.rows(28.0, num_rows, |mut row| {
                                        let data_row = data.row(row.index()).iter();
                                        for item in data_row {
                                            row.col(|ui| {
                                                let text = item.to_string();
                                                ui.add(egui::Label::new(RichText::new(&text).size(15.0)).truncate());
                                            });
                                        }
                                    });
                                });
                        });
                    }
                    VisualState::Plot => {
                        let data = &data_shared.shown_data;

                        let x_data = data.col(self.plot_tab.x_idx);
                        let y_data = data.col(self.plot_tab.y_idx);

                        let key = (data_shared.version, self.plot_tab.x_idx, self.plot_tab.y_idx, self.plot_tab.resolution);
                        if !self.plot_tab.cache.as_ref().is_some_and(|(cached_key, _)| cached_key == &key) {
                            let total_rows = data.shape().rows;
                            let required_rows = ((ui.available_width() as f64 * self.plot_tab.resolution) as usize).min(total_rows);
                            let modulus = (total_rows / required_rows).max(1);
                            let mut points: Vec<[f64; 2]> = Vec::with_capacity(required_rows);
                            points.extend((0..data.shape().rows).step_by(modulus).filter_map(|row_idx| {
                                let x_point = x_data.get_row(row_idx);
                                let y_point = y_data.get_row(row_idx);
                                // let (x_point, y_point) = (x_data.get_row(row_idx), y_data.get_row(row_idx));
                                if let (Some(x), Some(y)) = (x_point.as_float(), y_point.as_float()) {
                                    Some([x as f64, y as f64])
                                } else {
                                    None
                                }
                            }));

                            self.plot_tab.cache = Some((key, points));
                        }

                        let line = plot::Line::new(self.plot_tab.cache.as_ref().unwrap().1.clone());

                        plot::Plot::new("plot")
                            .allow_drag(false)
                            .x_axis_label(x_data.name())
                            .y_axis_label(y_data.name())
                            .show(ui, |plot_ui| {
                                plot_ui.line(line);
                            });
                    }
                }
            } else {
                ui.centered_and_justified(|ui| {
                    ui.add(egui::Label::new(RichText::new("No Data").size(40.0)));
                });
            }
        });

        toasts.show(ctx);
    }

    fn save(&mut self, storage: &mut dyn Storage) {
        storage.set_string("was-maximized", self.is_maximized.to_string());

        self.left.save(storage);
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        let current_data = self.shared.take();
        std::mem::forget(current_data);
    }

    fn persist_egui_memory(&self) -> bool { false }
}


fn main() -> eframe::Result<()> {
    let icon_img = image::load_from_memory_with_format(include_bytes!("../iss-logo.png"), image::ImageFormat::Png).unwrap().into_rgba8();

    let viewport = egui::ViewportBuilder::default()
        .with_icon(egui::IconData {
            width: icon_img.width(),
            height: icon_img.height(),
            rgba: icon_img.into_vec()
        });
    let options = eframe::NativeOptions {
        centered: true,
        // persist_window: true,
        viewport,
        ..Default::default()
    };
    eframe::run_native("MIDAS Launch", options, Box::new(|cc| Ok(Box::new(App::new(cc)))))
}
