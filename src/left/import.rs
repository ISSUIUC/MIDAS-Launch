use std::fs::File;
use std::{io, io::BufReader};
use std::path::PathBuf;
use std::sync::Arc;

use egui::{Color32, Ui};
use eframe::Storage;

use launch_file::{FormatType, LogFormat};
use dataframe::DataFrameView;

use crate::{DataShared, UpdateContext};
use crate::computation::{Computation, ProgressTask};
use crate::file_picker::{FilePicker, MultipleFilePicker, SelectedPath};

#[derive(Eq, PartialEq, Copy, Clone)]
enum ImportFrom {
    Launch,
    CSV
}

pub struct ImportTab {
    state: ImportFrom,

    import_launch_tab: ImportLaunchTab,
    import_csv_tab: ImportCSVTab
}

impl ImportTab {
    pub fn new(cc: &eframe::CreationContext) -> Self {
        Self {
            state: ImportFrom::Launch,
            import_launch_tab: ImportLaunchTab::new(cc),
            import_csv_tab: ImportCSVTab::new(cc)
        }
    }

    pub fn save(&self, storage: &mut dyn Storage) {
        self.import_launch_tab.save(storage);
        self.import_csv_tab.save(storage);
    }

    pub fn show(&mut self, ui: &mut Ui, ctx: UpdateContext) {
        ui.horizontal(|ui| {
            ui.label("Source type:");
            ui.selectable_value(&mut self.state, ImportFrom::Launch, ".launch File");
            ui.selectable_value(&mut self.state, ImportFrom::CSV, ".csv File");
        });

        match self.state {
            ImportFrom::Launch => self.import_launch_tab.show(ui, ctx),
            ImportFrom::CSV => self.import_csv_tab.show(ui, ctx)
        }
    }
}

struct ImportLaunchTab {
    source_paths: Vec<SelectedPath>,

    format_path: String,
    python_command: String,

    format_loading: Computation<(u32, Arc<LogFormat>), String>,
    // loading_format_task: Option<JoinHandle<Result<(u32, LogFormat), String>>>,
    // loaded_format: Option<(u32, Arc<LogFormat>)>,
    // format_message: Option<String>,

    parsing: Option<ProgressTask<Result<DataFrameView, io::Error>>>,
    parsing_message: Option<String>
}

impl ImportLaunchTab {
    pub fn new(cc: &eframe::CreationContext) -> ImportLaunchTab {
        let ctx = cc.egui_ctx.clone();
        let source_path = cc.storage.and_then(|storage| {
            let stored = storage.get_string("import-source-paths")?;
            let paths = ron::from_str::<'_, Vec<PathBuf>>(&stored).ok()?;
            let selected = paths.into_iter().map(|path| SelectedPath::from_path(ctx.clone(), path)).collect();
            Some(selected)
        }).unwrap_or(Vec::new());
        let format_path = cc.storage.and_then(|storage| storage.get_string("import-format-path")).unwrap_or("".to_string());
        let python_command = cc.storage.and_then(|storage| storage.get_string("import-python-command")).unwrap_or("python".to_string());

        ImportLaunchTab {
            source_paths: source_path,

            format_path,
            python_command,
            format_loading: Computation::Empty,

            parsing: None,
            parsing_message: None
        }
    }

    pub fn save(&self, storage: &mut dyn Storage) {
        storage.set_string("import-source-paths", ron::to_string(&self.source_paths.iter().map(|path| path.path.clone()).collect::<Vec<_>>()).unwrap());
        storage.set_string("import-format-path", self.format_path.clone());
        storage.set_string("import-python-command", self.python_command.clone());
    }

    pub fn show(&mut self, ui: &mut Ui, mut ctx: UpdateContext) {
        egui::CollapsingHeader::new("Data File".to_string()).id_salt("data-file-header").default_open(true).show(ui, |ui| {
            ui.add(MultipleFilePicker::new("data-file-picker", &mut self.source_paths)
                .dialog_title("Data File")
                .add_filter("Launch", &["launch"])
            );
        });

        let data_format_header = self.format_loading.value().map_or("Data Format".to_string(), |f| format!("Data Format - 0x{:0>8x}", f.0));
        egui::CollapsingHeader::new(data_format_header).id_salt("data-format-header").default_open(true).show(ui, |ui| {
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

                self.format_loading.check_complete();

                if self.format_loading.is_computing() {
                    ui.add_enabled(false, egui::Button::new("Loading Format"));
                } else {
                    let response = ui.add_enabled(!self.format_path.is_empty(), egui::Button::new("Load Format"))
                        .on_disabled_hover_text("Choose format file.");

                    if response.clicked() {
                        let python = PathBuf::from(self.python_command.clone());
                        let path = PathBuf::from(self.format_path.clone());
                        self.format_loading.begin(ui.ctx().clone(), move || {
                            LogFormat::from_format_file(&path, python)
                                .map(|(checksum, format)| (checksum, Arc::new(format)))
                        })
                    }
                }

                if let Some(msg) = self.format_loading.take_error() {
                    ctx.error_toast(msg);
                    // ui.colored_label(Color32::RED, "!").on_hover_text(msg);
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
                            ctx.data.replace(DataShared::new(dataframe));
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
                let format = 'verify_format: {
                    // first check if all the files have inline headers and that all the headers are the same
                    if let Some(compare_to) = self.source_paths.first() {
                        if let Some(FormatType::Inline(compare_to_format)) = compare_to.loading_format.value() {
                            let all_equal = self.source_paths.iter().all(|selected| {
                                selected.loading_format.value() == compare_to.loading_format.value()
                            });
                            if all_equal {
                                break 'verify_format Some(compare_to_format.clone());
                            }
                        }
                    }

                    // otherwise check if all the files have external formats and that it's the loaded format
                    if let Some((checksum, loaded)) = self.format_loading.value() {
                        let all_equal = self.source_paths.iter().all(|selected| {
                            selected.loading_format.value() == Some(&FormatType::External { checksum: *checksum })
                        });
                        if all_equal {
                            break 'verify_format Some(loaded.clone());
                        }
                    }

                    None
                };

                let response = ui.add_enabled(format.is_some(), egui::Button::new("Load Data")).on_disabled_hover_text("Choose data and load format.");
                if let (true, Some(format)) = (response.clicked(), format) {
                    self.parsing_message = None;
                    ctx.data.take();
                    let source_paths: Vec<PathBuf> = self.source_paths.iter().map(|path| path.path.clone()).collect();
                    self.parsing = Some(ProgressTask::new(ui.ctx(), move |progress| {
                        let mut file_sizes = vec![None; source_paths.len()];
                        let mut total_file_size = 0;
                        for (i, selected_path) in source_paths.iter().enumerate() {
                            if let Ok(metadata) = std::fs::metadata(&selected_path) {
                                file_sizes[i] = Some(metadata.len());
                                total_file_size += metadata.len();
                            }
                        }

                        let mut reader = format.reader(Some(total_file_size));

                        let mut current_offset = 0;
                        for (i, selected_path) in source_paths.iter().enumerate() {
                            let mut file = BufReader::new(File::open(&selected_path)?);

                            if let Some(file_size) = file_sizes[i] {
                                reader.read_file(&mut file, |offset| {
                                    progress.set((offset + current_offset) as f32 / total_file_size as f32);
                                })?;
                                current_offset += file_size;
                            } else {
                                let mut this_file_size = 0;
                                reader.read_file(&mut file, |offset| {
                                    progress.set((offset + current_offset) as f32 / (total_file_size + offset) as f32);
                                    this_file_size = offset;
                                })?;
                                total_file_size += this_file_size;
                                current_offset += this_file_size;
                            }
                        }

                        Ok(reader.finish())
                    }));
                }
            }

            if let Some(msg) = &self.parsing_message {
                ui.colored_label(Color32::RED, "!").on_hover_text(msg);
            }
        });
    }
}


struct ImportCSVTab {
    source_path: String,

    parsing: Option<ProgressTask<Result<DataFrameView, io::Error>>>,
    parsing_message: Option<String>
}

impl ImportCSVTab {
    pub fn new(_cc: &eframe::CreationContext) -> Self {
        Self {
            source_path: String::new(),
            parsing: None,
            parsing_message: None
        }
    }

    pub fn save(&self, _storage: &mut dyn Storage) { }

    pub fn show(&mut self, ui: &mut Ui, ctx: UpdateContext) {
        ui.add(FilePicker::new("data-csv-file-picker", &mut self.source_path)
            .dialog_title("Data File")
            .add_filter("CSV", &["csv"])
            // .add_filter("Any", &[])
        );

        ui.add_space(3.0);

        ui.horizontal(|ui| {
            if let Some(task) = &self.parsing {
                if task.is_finished() {
                    let result = self.parsing.take().unwrap().handle.join().unwrap();
                    match result {
                        Ok(dataframe) => {
                            ctx.data.replace(DataShared::new(dataframe));
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
                if !self.source_path.is_empty() {
                    let response = ui.add_enabled(true, egui::Button::new("Load Data"));

                    if response.clicked() {
                        self.parsing_message = None;
                        ctx.data.take();
                        let source_path = self.source_path.clone();

                        self.parsing = Some(ProgressTask::new(ui.ctx(), move |progress| {
                            let mut file = BufReader::new(File::open(source_path)?);
                            let size: u64 = file.get_ref().metadata().map_or(0, |m| m.len());

                            DataFrameView::from_csv(&mut file, |offset| {
                                progress.set(offset as f32 / size as f32);
                            })
                        }));
                    }
                } else {
                    ui.add_enabled(false, egui::Button::new("Load Data")).on_disabled_hover_text("Choose data.");
                }
            }

            if let Some(msg) = &self.parsing_message {
                ui.colored_label(Color32::RED, "!").on_hover_text(msg);
            }
        });
    }
}
