#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod file_picker;
mod task_button;

use std::fs::{File, OpenOptions};
use std::{io, io::{BufReader, Read, Write}};
use std::io::BufWriter;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use egui::{Align, Color32, Context, FontFamily, Layout, panel::Side, RichText, Ui, Visuals, Widget};
use egui_plot as plot;
use eframe::{Frame, Storage};

use dataframe::{Column, ColumnMut, Data, DataFrame, DataFrameView};
use launch_file::LogFormat;

use file_picker::FilePicker;


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

    cache: Option<((u64, usize, usize, f64), Vec<[f64; 2]>)>
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
                                        .selected_text(self.plot_tab.x_idx.map_or("", |n| shared.shown_data.col_name(n)))
                                        .show_ui(ui, |ui| {
                                            ui.selectable_value(&mut self.plot_tab.x_idx, None, "");
                                            for (idx, col_name) in shared.shown_data.col_names().enumerate() {
                                                ui.selectable_value(&mut self.plot_tab.x_idx, Some(idx), col_name);
                                            }
                                        });

                                    egui::ComboBox::new("y-axis-combo","Y axis")
                                        .selected_text(self.plot_tab.y_idx.map_or("", |n| shared.shown_data.col_name(n)))
                                        .show_ui(ui, |ui| {
                                            ui.selectable_value(&mut self.plot_tab.y_idx, None, "");
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

                        if let (Some(x_idx), Some(y_idx)) = (self.plot_tab.x_idx, self.plot_tab.y_idx) {
                            let (x_data, y_data) = (data.col(x_idx), data.col(y_idx));

                            let key = (data_shared.version, x_idx, y_idx, self.plot_tab.resolution);
                            if !self.plot_tab.cache.as_ref().is_some_and(|(cached_key, _)| cached_key == &key) {
                                let total_rows = data.shape().rows;
                                let required_rows = ((ui.available_width() as f64 * self.plot_tab.resolution) as usize).min(total_rows);
                                let modulus = (total_rows / required_rows).max(1);
                                let mut points: Vec<[f64; 2]> = Vec::with_capacity(required_rows);
                                points.extend((0..data.shape().rows).step_by(modulus).filter_map(|row_idx| {
                                    let (x_point, y_point) = (x_data.get_row_data(row_idx), y_data.get_row_data(row_idx));
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
                                .x_axis_label(x_data.name())
                                .y_axis_label(y_data.name())
                                .show(ui, |plot_ui| {
                                    plot_ui.line(line);
                                });
                        }
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


struct ImportTab {
    source_path: String,
    inspect_source_task: Option<JoinHandle<Result<u32, String>>>,
    inspected_checksum: Option<u32>,
    inspect_message: Option<String>,

    format_path: String,
    python_command: String,
    loading_format_task: Option<JoinHandle<Result<LogFormat, String>>>,
    loaded_format: Option<LogFormat>,
    format_message: Option<String>,

    parsing: Option<ProgressTask<Result<DataFrame, io::Error>>>,
    parsing_message: Option<String>
}

impl ImportTab {
    fn new(cc: &eframe::CreationContext) -> ImportTab {
        let source_path = cc.storage.and_then(|storage| storage.get_string("import-source-path")).unwrap_or("".to_string());
        let format_path = cc.storage.and_then(|storage| storage.get_string("import-format-path")).unwrap_or("".to_string());
        let python_command = cc.storage.and_then(|storage| storage.get_string("import-python-command")).unwrap_or("python".to_string());

        ImportTab {
            source_path,
            inspect_source_task: None,
            inspected_checksum: None,
            inspect_message: None,

            format_path,
            python_command,
            loading_format_task: None,
            loaded_format: None,
            format_message: None,

            parsing: None,
            parsing_message: None
        }
    }

    fn save(&self, storage: &mut dyn Storage) {
        storage.set_string("import-source-path", self.source_path.clone());
        storage.set_string("import-format-path", self.format_path.clone());
        storage.set_string("import-python-command", self.python_command.clone());
    }

    fn show(&mut self, ui: &mut Ui, shared: &mut Option<DataShared>) {
        let data_file_header = self.inspected_checksum.map_or("Data File".to_string(), |c| format!("Data File - 0x{:0>8x}", c));
        egui::CollapsingHeader::new(data_file_header).id_source("data-file-header").default_open(true).show(ui, |ui| {
            ui.add(FilePicker::new("data-file-picker", &mut self.source_path)
                .dialog_title("Data File")
                .add_filter("Launch", &["launch"])
            );

            ui.horizontal(|ui| {
                if let Some(task) = &self.inspect_source_task {
                    if task.is_finished() {
                        let result = self.inspect_source_task.take().unwrap().join().unwrap();
                        match result {
                            Ok(checksum) => { self.inspected_checksum = Some(checksum); }
                            Err(msg) => { self.inspect_message = Some(msg); }
                        }
                        ui.ctx().request_repaint();
                    }
                };

                if self.inspect_source_task.is_none() {
                    let response  = ui.add_enabled(!self.source_path.is_empty(), egui::Button::new("Inspect Source"))
                        .on_disabled_hover_text("Choose source file");
                    if response.clicked() {
                        let path = PathBuf::from(self.source_path.clone());

                        self.inspect_message = None;
                        self.inspect_source_task = Some(std::thread::spawn(move || {
                            let mut file = File::open(&path).map_err(|_| "Could not open file.".to_string())?;
                            let mut buf = [0; 4];
                            file.read_exact(&mut buf).map_err(|_| "Could not read from file.".to_string())?;
                            Ok(u32::from_le_bytes(buf))
                        }));
                    }
                } else {
                    ui.add_enabled(false, egui::Button::new("Inspecting Source"));
                }

                if let Some(msg) = &self.inspect_message {
                    ui.colored_label(Color32::RED, "!").on_hover_text(msg);
                }
            });
        });

        let data_format_header = self.loaded_format.as_ref().map_or("Data Format".to_string(), |f| format!("Data Format - 0x{:0>8x}", f.checksum));
        egui::CollapsingHeader::new(data_format_header).id_source("data-format-header").default_open(true).show(ui, |ui| {
            ui.add(FilePicker::new("data-format-picker", &mut self.format_path)
                .dialog_title("Data Format")
                .add_filter("C++ Header", &["h", "hpp"])
                .add_filter("C++ Source", &["c", "cc", "cpp"])
            );
            ui.horizontal(|ui| {
                ui.label("Python Command:");
                ui.text_edit_singleline(&mut self.python_command);
            });

            ui.horizontal(|ui| {
                if ui.button("⟳").clicked() {
                    std::thread::spawn(LogFormat::clear_scripts);
                }

                if let Some(handle) = &self.loading_format_task {
                    if handle.is_finished() {
                        let format_res = self.loading_format_task.take().unwrap().join().unwrap();
                        match format_res {
                            Ok(format) => {
                                self.loaded_format = Some(format);
                            }
                            Err(msg) => { self.format_message = Some(msg); }
                        }
                        ui.ctx().request_repaint();
                    }
                }

                if self.loading_format_task.is_none() {
                    let response = ui.add_enabled(!self.format_path.is_empty(), egui::Button::new("Load Format"))
                        .on_disabled_hover_text("Choose format file.");
                    if response.clicked() {
                        self.format_message = None;

                        let python = PathBuf::from(self.python_command.clone());
                        let path = PathBuf::from(self.format_path.clone());
                        let ctx_clone = ui.ctx().clone();
                        self.loading_format_task = Some(std::thread::spawn(move || {
                            let result = LogFormat::from_file(&path, python);
                            ctx_clone.request_repaint_after(Duration::from_millis(100));
                            result
                        }));
                    }
                } else {
                    ui.add_enabled(false, egui::Button::new("Loading Format"));
                }

                if let Some(msg) = &self.format_message {
                    ui.colored_label(Color32::RED, "!").on_hover_text(msg);
                }
            });
        });

        ui.add_space(3.0);

        ui.horizontal(|ui| {
            if let Some(task) = &self.parsing {
                if task.is_finished() {
                    let result = self.parsing.take().unwrap().handle.join().unwrap();
                    match result {
                        Ok(dataframe) => {
                            shared.replace(DataShared::new(DataFrameView::from_dataframe(dataframe)));
                        }
                        Err(e) => {
                            self.parsing_message = Some(e.to_string());
                        }
                    }
                }
            }

            if let Some(task) = &self.parsing {
                ui.add_enabled(false, egui::Button::new("Loading"));

                ui.add(egui::ProgressBar::new(task.progress()).show_percentage());
            } else {
                if let (Some(loaded_format), true) = (&self.loaded_format, !self.source_path.is_empty()) {
                    let response = ui.add_enabled(true, egui::Button::new("Load Data"));

                    if response.clicked() {
                        self.parsing_message = None;
                        shared.take();
                        let format = loaded_format.clone();
                        let source_path = self.source_path.clone();

                        self.parsing = Some(ProgressTask::new(ui.ctx(), move |progress| {
                            let mut file = BufReader::new(File::open(source_path)?);
                            let size: u64 = file.get_ref().metadata().map_or(0, |m| m.len());

                            format.read_file(&mut file, |offset| {
                                progress.set(offset as f32 / size as f32);
                            })
                        }));
                    }
                } else {
                    ui.add_enabled(false, egui::Button::new("Load Data")).on_disabled_hover_text("Choose data and load format.");
                }
            }

            if let Some(msg) = &self.parsing_message {
                ui.colored_label(Color32::RED, "!").on_hover_text(msg);
            }
        });
    }
}


#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum StepType {
    Fill,
    ColEq,
    Within,
    Sort,
    Decimate,
}

impl StepType {
    fn name(&self) -> &'static str {
        match self {
            StepType::Fill => "Fill",
            StepType::ColEq => "Select",
            StepType::Within => "Within",
            StepType::Sort => "Sort",
            StepType::Decimate => "Decimate"
        }
    }

    fn to_step(&self, id: u64) -> Step {
        match self {
            StepType::Fill => Step::Fill(id, true, true),
            StepType::ColEq => Step::ColEq(id, 0, "".to_string()),
            StepType::Within => Step::Within(id, 0, false, "".to_string(), false, "".to_string()),
            StepType::Sort => Step::Sort(id, false, 0),
            StepType::Decimate => Step::Decimate(id, 2)
        }
    }
}

#[derive(Clone)]
enum Step {
    Fill(u64, bool, bool),
    ColEq(u64, usize, String),
    Within(u64, usize, bool, String, bool, String),
    Sort(u64, bool, usize),
    Decimate(u64, usize),
}

impl Step {
    fn ty(&self) -> StepType {
        match self {
            Step::Fill(_, _, _) => StepType::Fill,
            Step::ColEq(_, _, _) => StepType::ColEq,
            Step::Within(_, _, _, _, _, _) => StepType::Within,
            Step::Sort(_, _, _) => StepType::Sort,
            Step::Decimate(_, _) => StepType::Decimate,
        }
    }

    fn id(&self) -> u64 {
        match self {
            Step::Fill(id, _, _) => *id,
            Step::ColEq(id, _, _) => *id,
            Step::Within(id, _, _, _, _, _) => *id,
            Step::Sort(id, _, _) => *id,
            Step::Decimate(id, _) => *id,
        }
    }

    fn apply(&self, mut df: DataFrameView, progress: &Progress) -> DataFrameView {
        match self {
            Step::Fill(_, _, and_before) => {
                let shape = df.shape();

                for col_idx in 0..shape.cols {
                    let mut col = df.col_mut(col_idx);
                    let mut prev_value = Data::Null;
                    if *and_before {
                        for row_idx in 0..shape.rows {
                            let data = col.get_row_data(row_idx);
                            if !data.is_null() {
                                prev_value = unsafe { std::mem::transmute(data) };
                                break;
                            }
                        }
                    }

                    for row_idx in 0..shape.rows {
                        let data = col.get_row_data(row_idx);
                        if data.is_null() {
                            col.set_row_data(row_idx, &prev_value);
                        } else {
                            prev_value = unsafe { std::mem::transmute(data) };
                        }
                    }

                    progress.set(col_idx as f32 / shape.cols as f32);
                }

                df
            }
            Step::ColEq(_, col_idx, value) => {
                let equal_to = df.df.cols()[*col_idx].data_type().parse_str(value);
                let rows = df.shape().rows as f32;

                progress.set(0.0);
                df.filter_by(*col_idx, |i, data| {
                    let ret = data.eq(&equal_to);
                    if i % 3000 == 0 {
                        progress.set(i as f32 / rows);
                    }
                    ret
                });
                progress.set(1.0);

                df
            }
            Step::Within(_, col_idx, has_lower_bound, lower_bound, has_upper_bound, upper_bound) => {
                let dtype = df.df.cols()[*col_idx].data_type();
                let rows = df.shape().rows as f32;

                let bounds = (
                    if *has_lower_bound { Bound::Included(dtype.parse_str(lower_bound)) } else { Bound::Unbounded },
                    if *has_upper_bound { Bound::Included(dtype.parse_str(upper_bound)) } else { Bound::Unbounded },
                );

                progress.set(0.0);
                df.filter_by(*col_idx, |i, data| {
                    let ret = data.in_bounds(bounds);
                    if i % 3000 == 0 {
                        progress.set(i as f32 / rows);
                    }
                    ret
                });
                progress.set(1.0);

                df
            }
            Step::Sort(_, descending, col_idx) => {
                progress.set(0.0);
                if *descending {
                    df.sort_by_desc(*col_idx);
                } else {
                    df.sort_by_asc(*col_idx);
                }
                progress.set(1.0);
                df
            }
            Step::Decimate(_, factor) => {
                let rows = df.shape().rows as f32;

                progress.set(0.0);
                df.filter_by(0, |i, _| {
                    if i % 3000 == 0 {
                        progress.set(i as f32 / rows);
                    }
                    i % factor == 0
                });
                progress.set(1.0);

                df
            }
        }
    }
}

struct ProcessTab {
    steps: Vec<Step>,
    step_id: u64,
    add_step_type: StepType,

    task: Option<ProgressTask<Result<DataFrameView, String>>>
}

impl ProcessTab {
    fn new(_cc: &eframe::CreationContext) -> ProcessTab {
        ProcessTab {
            steps: vec![
                Step::Sort(0, false, 1),
                Step::Fill(1, true, true),
            ],
            step_id: 2,
            add_step_type: StepType::Fill,

            task: None
        }
    }

    fn save(&self, _storage: &mut dyn Storage) { }

    fn show(&mut self, ui: &mut Ui, shared: &mut Option<DataShared>) {
        let Some(shared) = shared else { return; };

        ui.add_space(3.0);

        ui.allocate_ui(ui.available_size(), |ui| {
        egui::Frame::group(ui.style())
            .show(ui, |ui| {
                egui::ScrollArea::vertical()
                    .auto_shrink([false, true])
                    .max_height(500.0)
                    .show(ui, |ui| {
                        let mut swaps = vec![];
                        let mut dels = vec![];

                        for i in 0..self.steps.len() {
                            let step = &self.steps[i];

                            let id = ui.make_persistent_id(format!("step-{}", step.id()));
                            egui::collapsing_header::CollapsingState::load_with_default_open(ui.ctx(), id, true)
                                .show_header(ui, |ui| {
                                    ui.label(format!("{}", step.ty().name()));

                                    if ui.add_enabled(true, egui::Button::new("-").frame(false)).clicked() {
                                        dels.push(i);
                                    }
                                    if ui.add_enabled(i > 0, egui::Button::new("^").frame(false)).clicked() {
                                        swaps.push((i, i-1));
                                    }
                                    if ui.add_enabled(i < self.steps.len()-1, egui::Button::new("v").frame(false)).clicked() {
                                        swaps.push((i, i+1));
                                    }
                                })
                                .body(|ui| {
                                    let step = &mut self.steps[i];
                                    match step {
                                        Step::Fill(_, is_down, and_before) => {
                                            ui.horizontal(|ui| {
                                                ui.label("Direction (todo)");
                                                ui.selectable_value(is_down, true, "Down");
                                                ui.selectable_value(is_down, false, "Up");
                                            });

                                            ui.horizontal(|ui| {
                                                ui.label("Backfill");
                                                ui.add(egui::Checkbox::without_text(and_before));
                                            });
                                        }
                                        Step::ColEq(id, col_idx, eq_value) => {
                                            ui.horizontal(|ui| {
                                                ui.label("Where");

                                                egui::ComboBox::from_id_source(format!("combo-where-{id}"))
                                                    .wrap(true)
                                                    .show_index(ui, col_idx, shared.complete_data.shape().cols, |idx| shared.complete_data.col_name(idx));
                                            });

                                            ui.horizontal(|ui| {
                                                ui.label("Equals");

                                                ui.add(egui::TextEdit::singleline(eq_value)
                                                    .id_source(format!("text-{id}"))
                                                    .hint_text("...")
                                                    .clip_text(true));
                                            });
                                        }
                                        Step::Within(id, col_idx, has_lower_bound, lower_bound, has_upper_bound, upper_bound) => {
                                            ui.horizontal(|ui| {
                                                ui.label("Where");

                                                egui::ComboBox::from_id_source(format!("combo-within-{id}"))
                                                    .wrap(true)
                                                    .show_index(ui, col_idx, shared.complete_data.shape().cols, |idx| shared.complete_data.col_name(idx));
                                            });

                                            ui.horizontal(|ui| {
                                                // ui.label("Lower Bound");

                                                ui.checkbox(has_lower_bound, "Lower");
                                                ui.text_edit_singleline(lower_bound);
                                            });

                                            ui.horizontal(|ui| {
                                                // ui.label("Upper Bound");

                                                ui.checkbox(has_upper_bound, "Upper");
                                                ui.text_edit_singleline(upper_bound);
                                            });
                                        }
                                        Step::Sort(id, is_desc, col_idx) => {
                                            ui.horizontal(|ui| {
                                                ui.label("Sort");
                                                egui::ComboBox::from_id_source(format!("combo-sort-{id}"))
                                                    .selected_text(if *is_desc { "Descending" } else { "Ascending" })
                                                    .show_ui(ui, |ui| {
                                                        ui.selectable_value(is_desc, false, "Ascending");
                                                        ui.selectable_value(is_desc, true, "Descending");
                                                    });
                                            });

                                            ui.horizontal(|ui| {
                                                ui.label("By");

                                                egui::ComboBox::from_id_source(format!("combo-by-{id}"))
                                                    .show_index(ui, col_idx, shared.complete_data.shape().cols, |idx| shared.complete_data.col_name(idx));
                                            });
                                        }
                                        Step::Decimate(_, factor) => {
                                            ui.horizontal(|ui| {
                                                ui.label("Factor");
                                                ui.add(egui::DragValue::new(factor).clamp_range(1..=50000))
                                            });
                                        }
                                    }
                                });
                        }

                        ui.horizontal(|ui| {
                            if ui.button("Add").clicked() {
                                self.steps.push(self.add_step_type.to_step(self.step_id));
                                self.step_id +=1;
                            }

                            egui::ComboBox::from_id_source("add-type")
                                .selected_text(self.add_step_type.name())
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut self.add_step_type, StepType::Fill, StepType::Fill.name());
                                    ui.selectable_value(&mut self.add_step_type, StepType::ColEq, StepType::ColEq.name());
                                    ui.selectable_value(&mut self.add_step_type, StepType::Within, StepType::Within.name());
                                    ui.selectable_value(&mut self.add_step_type, StepType::Sort, StepType::Sort.name());
                                });
                        });

                        for (a, b) in swaps {
                            self.steps.swap(a, b);
                        }
                        for del in dels {
                            self.steps.remove(del);
                        }
                    });
            });

            ui.add_space(3.0);

            ui.horizontal(|ui| {
                if let Some(task) = &self.task {
                    if task.is_finished() {
                        let result = self.task.take().unwrap().handle.join().unwrap();
                        match result {
                            Ok(dataframe) => {
                                shared.shown_data = dataframe;
                                shared.version += 1;
                            }
                            Err(_) => { }
                        }
                    }
                }

                if let Some(task) = &self.task {
                    ui.add_enabled(false, egui::Button::new("Applying"));

                    let text = task.text();
                    let text = if text.is_empty() {
                        format!("{}%", (task.progress() * 100.0) as u32)
                    } else {
                        format!("{} {}%", text, (task.progress() * 100.0) as u32)
                    };

                    ui.add(egui::ProgressBar::new(task.progress()).text(text));
                } else {
                    if ui.button("Apply").clicked() {
                        let steps = self.steps.clone();
                        let old_data = shared.complete_data.clone();

                        self.task = Some(ProgressTask::new(ui.ctx(), move |progress| {
                            let mut data = old_data;
                            for (i, step) in steps.iter().enumerate() {
                                progress.set_text(format!("Step {}/{}", i+1, steps.len()));
                                progress.set(0.0);
                                data = step.apply(data, progress);
                            }

                            Ok(data)
                        }));
                    }
                }
            });
        });
    }
}


#[derive(Copy, Clone, Eq, PartialEq)]
enum ExportFormats {
    Json,
    Csv
}

struct JsonExport {
    path: String,
    omit_null: bool,

    export: Option<ProgressTask<Result<(), io::Error>>>,
    msg: Option<String>
}

struct CsvExport {
    path: String,
    append_mode: bool,

    export: Option<ProgressTask<Result<(), io::Error>>>,
    msg: Option<String>
}

struct ExportTab {
    export: ExportFormats,
    json: JsonExport,
    csv: CsvExport
}

impl ExportTab {
    fn new(_cc: &eframe::CreationContext) -> ExportTab {
        ExportTab {
            export: ExportFormats::Csv,
            json: JsonExport {
                path: String::new(),
                omit_null: true,

                export: None,
                msg: None
            },
            csv: CsvExport {
                path: String::new(),
                append_mode: false,

                export: None,
                msg: None
            }
        }
    }

    fn save(&self, _storage: &mut dyn Storage) { }

    fn show(&mut self, ui: &mut Ui, shared: &mut Option<DataShared>) {
        ui.add_space(3.0);

        ui.columns(2, |cols| {
            cols[0].vertical_centered_justified(|ui| {
                ui.selectable_value(&mut self.export, ExportFormats::Csv, "CSV")
            });
            cols[1].vertical_centered_justified(|ui| {
                ui.selectable_value(&mut self.export, ExportFormats::Json, "JSON")
            });
        });

        ui.add_space(3.0);

        if let Some(csv_export) = &self.csv.export {
            if csv_export.is_finished() {
                let result = self.csv.export.take().unwrap().handle.join().unwrap();
                match result {
                    Ok(()) => (),
                    Err(e) => {
                        self.csv.msg = Some(e.to_string());
                    }
                }
            }
        }

        if let Some(json_export) = &self.json.export {
            if json_export.is_finished() {
                let result = self.json.export.take().unwrap().handle.join().unwrap();
                match result {
                    Ok(()) => (),
                    Err(e) => {
                        self.json.msg = Some(e.to_string());
                    }
                }
            }
        }

        match self.export {
            ExportFormats::Csv => {
                ui.horizontal(|ui| {
                    ui.label("Path");
                    ui.add(FilePicker::new("csv-picker", &mut self.csv.path)
                        .add_filter("CSV", &["csv"])
                        .set_is_save(true)
                        .dialog_title("Save"));
                });

                ui.horizontal(|ui| {
                    ui.checkbox(&mut self.csv.append_mode, "Append");
                });

                ui.horizontal(|ui| {
                    if let Some(export) = &self.csv.export {
                            ui.add_enabled(false, egui::Button::new("Exporting"));

                            ui.add(egui::ProgressBar::new(export.progress()).show_percentage());
                    } else {
                        if ui.button("Export").clicked() {
                            self.csv.msg = None;

                            let data = shared.as_ref().unwrap().shown_data.clone();
                            let path = PathBuf::from(self.csv.path.clone());
                            let is_append = self.csv.append_mode;

                            self.csv.export = Some(ProgressTask::new(ui.ctx(), move |progress| {
                                let mut file;
                                if is_append {
                                    file = BufWriter::new(OpenOptions::new().write(true).append(true).open(&path)?);
                                } else {
                                    file = BufWriter::new(File::create(&path)?);

                                    let mut col_iterator = data.col_names();
                                    if let Some(name) = col_iterator.next() {
                                        write!(&mut file, "{}", name)?;

                                        while let Some(name) = col_iterator.next() {
                                            write!(&mut file, ",{}", name)?;
                                        }

                                        file.write(&[b'\n'])?;
                                    }
                                }

                                let total_rows = data.shape().rows;
                                for idx in 0..total_rows {
                                    let mut row_iterator = data.row_iter(idx);
                                    if let Some(data) = row_iterator.next() {
                                        write!(&mut file, "{}", data)?;

                                        while let Some(data) = row_iterator.next() {
                                            write!(&mut file, ",{}", data)?;
                                        }
                                    }
                                    file.write(&[b'\n'])?;

                                    progress.set(idx as f32 / total_rows as f32);
                                }

                                file.flush()?;

                                Ok(())
                            }));
                        }

                        if let Some(msg) = &self.csv.msg {
                            ui.colored_label(Color32::RED, msg);
                        }
                    }
                });
            }

            ExportFormats::Json => {
                ui.horizontal(|ui| {
                    ui.label("Path");
                    ui.add(FilePicker::new("json-picker", &mut self.json.path)
                        .add_filter("JSON", &["json"])
                        .set_is_save(true)
                        .dialog_title("Save"));
                });

                ui.horizontal(|ui| {
                    if let Some(export) = &self.json.export {
                            ui.add_enabled(false, egui::Button::new("Export"));

                            ui.add(egui::ProgressBar::new(export.progress()));
                    } else {
                        // if ui.button("Export").clicked() {
                        //     let data = shared.as_ref().unwrap().shown_data.clone().unwrap_or_else(|| shared.as_ref().unwrap().complete_data.clone());
                        //     let path = PathBuf::from(self.json.path.clone());
                        //
                        //     self.json.export = Some(ProgressTask::new(ui.ctx(), |progress| {
                        //         let mut file = BufWriter::new(File::create(&path)?);
                        //
                        //         let mut col_iterator = data.col_names();
                        //         if let Some(name) = col_iterator.next() {
                        //             write!(&mut file, "{}", name)?;
                        //
                        //             while let Some(name) = col_iterator.next() {
                        //                 write!(&mut file, ",{}", name)?;
                        //             }
                        //
                        //             file.write(&[b'\n'])?;
                        //         }
                        //
                        //         let total_rows = data.shape().rows;
                        //         for idx in 0..total_rows {
                        //             let mut row_iterator = data.row_iter(idx);
                        //             if let Some(data) = row_iterator.next() {
                        //                 write!(&mut file, "{}", data)?;
                        //
                        //                 while let Some(data) = row_iterator.next() {
                        //                     write!(&mut file, ",{}", data)?;
                        //                 }
                        //             }
                        //             file.write(&[b'\n'])?;
                        //
                        //             if idx % 3000 == 0 {
                        //                 progress.set(idx as f32 / total_rows as f32);
                        //             }
                        //         }
                        //
                        //         file.flush()?;
                        //
                        //         Ok(())
                        //     }));
                        // }

                        if let Some(msg) = &self.json.msg {
                            ui.colored_label(Color32::RED, msg);
                        }
                    }
                });
            }
        }
    }
}


fn main() -> eframe::Result<()> {
    // let mut viewport = egui::ViewportBuilder::default();
    let options = eframe::NativeOptions {
        centered: true,
        // persist_window: true,
        // viewport,
        ..Default::default()
    };
    eframe::run_native("MIDAS Launch", options, Box::new(|cc| Box::new(App::new(cc))))
}
