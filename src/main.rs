#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod file_picker;
mod task_button;
mod process;
mod import;
mod export;

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use egui::{Align, Context, FontFamily, Layout, panel::Side, RichText, Visuals, Widget};
use egui_plot as plot;
use eframe::{Frame, Storage};
// use egui_extras::image;

use dataframe::{Column, Data, DataFrameView};

use crate::import::ImportTab;
use crate::process::ProcessTab;
use crate::export::ExportTab;


#[derive(Copy, Clone, PartialEq, Eq)]
enum LeftState {
    Import,
    Filter,
    Export
}


#[derive(Copy, Clone, PartialEq, Eq)]
enum VisualState {
    Plot,
    Table
}

struct TableTab {

}

struct PlotTab {
    // plots: Option<PlotInfo>,

    x_idx: Option<usize>,
    y_idx: Option<usize>,
    resolution: f64,

    cache: Option<((u64, Option<usize>, Option<usize>, f64), Vec<[f64; 2]>)>
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
            x_idx: None,
            y_idx: None,
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


struct App {
    left_state: LeftState,
    import_tab: ImportTab,
    process_tab: ProcessTab,
    export_tab: ExportTab,

    shared: Option<DataShared>,

    visual_state: VisualState,
    table_tab: TableTab,
    plot_tab: PlotTab,

    is_maximized: bool
}

impl App {
    fn new(cc: &eframe::CreationContext) -> App {
        let was_maximized = cc.storage.and_then(|store| store.get_string("was-maximized")).map_or(false, |s| s == "true");
        if was_maximized {
            cc.egui_ctx.send_viewport_cmd(egui::ViewportCommand::Maximized(true));
        }

        App {
            left_state: LeftState::Import,
            import_tab: ImportTab::new(cc),
            process_tab: ProcessTab::new(cc),
            export_tab: ExportTab::new(cc),

            shared: None,

            visual_state: VisualState::Table,
            table_tab: TableTab::new(cc),
            plot_tab: PlotTab::new(cc),

            is_maximized: was_maximized
        }
    }
}


impl eframe::App for App {
    fn update(&mut self, ctx: &Context, _frame: &mut Frame) {
        self.is_maximized = ctx.input(|state| state.viewport().maximized.unwrap_or(false));

        ctx.set_visuals(Visuals::light());

        egui::SidePanel::new(Side::Left, "left-panel")
            .default_width(180.0)
            .min_width(240.0)
            .max_width(480.0)
            .show(ctx, |ui| {
                ui.add_space(3.0);
                ui.columns(3, |columns| {
                    columns[0].vertical_centered_justified(|ui| {
                        ui.selectable_value(&mut self.left_state, LeftState::Import, "Import")
                    });
                    columns[1].vertical_centered_justified(|ui| {
                        ui.add_enabled_ui(self.shared.is_some(), |ui| {
                            ui.selectable_value(&mut self.left_state, LeftState::Filter, "Filter")
                        });
                    });
                    columns[2].vertical_centered_justified(|ui| {
                        ui.add_enabled_ui(self.shared.is_some(), |ui| {
                            ui.selectable_value(&mut self.left_state, LeftState::Export, "Export")
                        });
                    });
                });
                ui.separator();

                match self.left_state {
                    LeftState::Import => {
                        self.import_tab.show(ui, &mut self.shared);
                    }
                    LeftState::Filter => {
                        self.process_tab.show(ui, &mut self.shared);
                    }
                    LeftState::Export => {
                        self.export_tab.show(ui, &mut self.shared);
                    }
                };
        });


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

                        }
                        VisualState::Plot => {
                            egui::Frame::group(ui.style())
                                .show(ui, |ui| {
                                    egui::ComboBox::new("x-axis-combo","X axis")
                                        .selected_text(self.plot_tab.x_idx.map_or("<row number>", |n| shared.shown_data.col_name(n)))
                                        .show_ui(ui, |ui| {
                                            ui.selectable_value(&mut self.plot_tab.x_idx, None, "<row number>");
                                            for (idx, col_name) in shared.shown_data.col_names().enumerate() {
                                                ui.selectable_value(&mut self.plot_tab.x_idx, Some(idx), col_name);
                                            }
                                        });

                                    egui::ComboBox::new("y-axis-combo","Y axis")
                                        .selected_text(self.plot_tab.y_idx.map_or("<row number>", |n| shared.shown_data.col_name(n)))
                                        .show_ui(ui, |ui| {
                                            ui.selectable_value(&mut self.plot_tab.y_idx, None, "<row number>");
                                            for (idx, col_name) in shared.shown_data.col_names().enumerate() {
                                                ui.selectable_value(&mut self.plot_tab.y_idx, Some(idx), col_name);
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
                                            egui::Label::new(RichText::new(col_name).family(FontFamily::Monospace).size(18.0)).truncate(true).ui(ui);
                                        });
                                    }
                                })
                                .body(|body| {
                                    let num_rows = data.shape().rows;
                                    body.rows(28.0, num_rows, |mut row| {
                                        let data_row = data.row_iter(row.index());
                                        for item in data_row {
                                            row.col(|ui| {
                                                let text = item.to_string();
                                                ui.add(egui::Label::new(RichText::new(&text).size(15.0)).truncate(true));
                                            });
                                        }
                                    });
                                });
                        });
                    }
                    VisualState::Plot => {
                        let data = &data_shared.shown_data;

                        let x_data = self.plot_tab.x_idx.map(|idx| data.col(idx));
                        let y_data = self.plot_tab.y_idx.map(|idx| data.col(idx));

                        let key = (data_shared.version, self.plot_tab.x_idx, self.plot_tab.y_idx, self.plot_tab.resolution);
                        if !self.plot_tab.cache.as_ref().is_some_and(|(cached_key, _)| cached_key == &key) {
                            let total_rows = data.shape().rows;
                            let required_rows = ((ui.available_width() as f64 * self.plot_tab.resolution) as usize).min(total_rows);
                            let modulus = (total_rows / required_rows).max(1);
                            let mut points: Vec<[f64; 2]> = Vec::with_capacity(required_rows);
                            points.extend((0..data.shape().rows).step_by(modulus).filter_map(|row_idx| {
                                let x_point = x_data.as_ref().map_or(Data::Integer(row_idx as i64), |x_data| x_data.get_row_data(row_idx));
                                let y_point = y_data.as_ref().map_or(Data::Integer(row_idx as i64), |y_data| y_data.get_row_data(row_idx));
                                // let (x_point, y_point) = (x_data.get_row_data(row_idx), y_data.get_row_data(row_idx));
                                if let (Some(x), Some(y)) = (x_point.as_float(), y_point.as_float()) {
                                    Some([x, y])
                                } else {
                                    None
                                }
                            }));

                            self.plot_tab.cache = Some((key, points));
                        }

                        let line = plot::Line::new(self.plot_tab.cache.as_ref().unwrap().1.clone());

                        plot::Plot::new("plot")
                            .allow_drag(false)
                            .x_axis_label(x_data.as_ref().map_or("<row number>", |x_data| x_data.name()))
                            .y_axis_label(y_data.as_ref().map_or("<row number>", |x_data| x_data.name()))
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
    }

    fn save(&mut self, storage: &mut dyn Storage) {
        storage.set_string("was-maximized", self.is_maximized.to_string());

        self.import_tab.save(storage);
        self.process_tab.save(storage);
        self.export_tab.save(storage);
    }

    fn persist_egui_memory(&self) -> bool { false }
}

#[derive(Clone)]
struct Progress(Arc<Mutex<(Context, f32, String)>>);

impl Progress {
    fn set_text(&self, text: String) {
        let mut lock = self.0.lock().unwrap();
        lock.2 = text;
        lock.0.request_repaint_after(Duration::from_millis(16));
    }

    fn set(&self, amount: f32) {
        let mut lock = self.0.lock().unwrap();
        lock.1 = amount;
        lock.0.request_repaint_after(Duration::from_millis(16));
    }
}

struct ProgressTask<T> {
    handle: JoinHandle<T>,
    progress: Progress
}

impl<T> ProgressTask<T> where T: Send + 'static {
    fn new(ctx: &Context, f: impl FnOnce(&Progress) -> T + Send + 'static) -> ProgressTask<T> {
        let progress = Progress(Arc::new(Mutex::new((ctx.clone(), 0.0, "".to_string()))));
        let progress_clone = progress.clone();

        let handle = std::thread::spawn(move || {
            let res = f(&progress_clone);
            progress_clone.0.lock().unwrap().0.request_repaint_after(Duration::from_millis(16));
            res
        });

        ProgressTask { handle, progress }
    }

    fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    fn progress(&self) -> f32 {
        self.progress.0.lock().unwrap().1
    }

    fn text(&self) -> String {
        self.progress.0.lock().unwrap().2.clone()
    }
}


fn main() -> eframe::Result<()> {
    // let v = egui::include_image!("../iss-logo.png");
    let icon_img = image::load_from_memory_with_format(include_bytes!("../iss-logo.png"), image::ImageFormat::Png).unwrap().into_rgba8();

    let mut viewport = egui::ViewportBuilder::default()
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
    eframe::run_native("MIDAS Launch", options, Box::new(|cc| Box::new(App::new(cc))))
}
