use std::fs::File;
use std::{io, io::{BufReader, Read}};
use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use egui::{Color32, Ui};
use eframe::Storage;

use launch_file::LogFormat;
use dataframe::{DataFrame, DataFrameView};

use crate::DataShared;
use crate::ProgressTask;
use crate::file_picker::FilePicker;

pub struct ImportTab {
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
    pub fn new(cc: &eframe::CreationContext) -> ImportTab {
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

    pub fn save(&self, storage: &mut dyn Storage) {
        storage.set_string("import-source-path", self.source_path.clone());
        storage.set_string("import-format-path", self.format_path.clone());
        storage.set_string("import-python-command", self.python_command.clone());
    }

    pub fn show(&mut self, ui: &mut Ui, shared: &mut Option<DataShared>) {
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
                            let mut v = vec![];
                            File::open(source_path)?.read_to_end(&mut v)?;
                            let size = v.len();;
                            let mut file = std::io::Cursor::new(v);
                            // let mut file = BufReader::new(File::open(source_path)?);
                            // let size: u64 = file.get_ref().metadata().map_or(0, |m| m.len());

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
