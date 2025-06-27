use std::fs::File;
use std::sync::{Arc, Mutex};
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use egui::{Align, Color32, Layout, Response, RichText, Sense, Ui};
use egui_extras::Column;
use futures_lite::future::block_on;
use rfd::AsyncFileDialog;

use launch_file::{FormatType, LogFormat};

type FilePickerHandle = Option<JoinHandle<Option<PathBuf>>>;

pub struct FilePicker<'a> {
    id_source: egui::Id,
    async_file_dialog: AsyncFileDialog,
    path: &'a mut String,
    save_dialog: bool
}

impl<'a> FilePicker<'a> {
    pub fn new(id: impl Into<egui::Id>, path: &'a mut String) -> Self {
        FilePicker {
            id_source: id.into(),
            async_file_dialog: AsyncFileDialog::new(),
            path,
            save_dialog: false
        }
    }

    pub fn dialog_title(mut self, title: impl Into<String>) -> Self {
        self.async_file_dialog = self.async_file_dialog.set_title(title);
        self
    }

    pub fn add_filter(mut self, name: impl Into<String>, extensions: &[impl ToString]) -> Self {
        self.async_file_dialog = self.async_file_dialog.add_filter(name, extensions);
        self
    }

    pub fn set_is_save(mut self, is_save: bool) -> Self {
        self.save_dialog = is_save;
        self
    }
}

impl<'a> egui::Widget for FilePicker<'a> {
    fn ui(self, ui: &mut Ui) -> Response {
        let maybe_handle = ui.data_mut(|ui|
            ui.get_temp_mut_or_default::<Arc<Mutex<FilePickerHandle>>>(self.id_source).clone()
        );
        let mut lock = maybe_handle.lock().unwrap();

        ui.horizontal(|ui| {
            let mut chose_enabled = true;
            if let Some(handle) = lock.as_ref() {
                if handle.is_finished() {
                    let maybe_path = lock.take().unwrap().join().unwrap();
                    if let Some(p) = maybe_path {
                        *self.path = p.to_string_lossy().into_owned();
                    }
                } else {
                    chose_enabled = false;
                }
            }

            if ui.add_enabled(chose_enabled, egui::Button::new("Choose File")).clicked() {
                let dialog = if let Some(dir) = Path::new(self.path.as_str()).parent() {
                    self.async_file_dialog.set_directory(dir)
                } else {
                    self.async_file_dialog
                };

                let ctx_clone = ui.ctx().clone();

                if self.save_dialog {
                    let pick_task = dialog.save_file();

                    *lock = Some(thread::spawn(move || {
                        let file_path = block_on(pick_task);
                        let file_path = file_path.map(|handle| handle.path().to_owned());
                        ctx_clone.request_repaint_after(Duration::from_millis(100));
                        file_path
                    }));
                } else {
                    let pick_task = dialog.pick_file();

                    *lock = Some(thread::spawn(move || {
                        let file_path = block_on(pick_task);
                        let file_path = file_path.map(|handle| handle.path().to_owned());
                        ctx_clone.request_repaint_after(Duration::from_millis(100));
                        file_path
                    }));
                }
            }
            ui.add(egui::TextEdit::singleline(self.path).hint_text("..."));
        }).response
    }
}

#[derive(Default)]
struct MultipleFilePickerData {
    selection: Option<usize>,
    file_dialog_handle: Option<JoinHandle<Option<Vec<SelectedPath>>>>
}

pub enum HeaderReadingStatus {
    InProgress(JoinHandle<Result<FormatType, String>>),
    HeaderType(FormatType),
    Error(String)
}

impl HeaderReadingStatus {
    pub fn checksum(&self) -> Option<u32> {
        match self {
            HeaderReadingStatus::HeaderType(FormatType::External { checksum }) => Some(*checksum),
            _ => None,
        }
    }

    pub fn inline_header(&self) -> Option<&Arc<LogFormat>> {
        match self {
            HeaderReadingStatus::HeaderType(FormatType::Inline(format)) => Some(format),
            _ => None
        }
    }

    fn is_done(&self) -> bool {
        if let HeaderReadingStatus::InProgress(handle) = self {
            handle.is_finished()
        } else {
            false
        }
    }

    fn unwrap_handle(&mut self) -> Option<JoinHandle<Result<FormatType, String>>> {
        if let HeaderReadingStatus::InProgress(_) = self {
            let HeaderReadingStatus::InProgress(handle) = std::mem::replace(self, HeaderReadingStatus::Error(String::new())) else { unreachable!() };
            Some(handle)
        } else {
            None
        }
    }
}

pub struct SelectedPath {
    pub path: PathBuf,
    pub short_name: String,
    pub format_status: HeaderReadingStatus
}

impl SelectedPath {
    pub fn from_path(path: impl Into<PathBuf>) -> SelectedPath {
        let path = path.into();
        let short_name = path.file_name().map_or(String::new(), |name| name.to_string_lossy().into_owned());
        let format_status = HeaderReadingStatus::InProgress(thread::spawn({
            let path = path.clone();
            move || {
                let mut file = File::open(&path).map_err(|_| "Could not open file.".to_string())?;
                FormatType::from_file(&mut file).map_err(|_| "Could not open file.".to_string())
            }
        }));

        SelectedPath { path, short_name, format_status }
    }
}

pub struct MultipleFilePicker<'a> {
    id_source: egui::Id,
    async_file_dialog: AsyncFileDialog,
    paths: &'a mut Vec<SelectedPath>
}

impl<'a> MultipleFilePicker<'a> {
    pub fn new(id: impl Into<egui::Id>, paths: &'a mut Vec<SelectedPath>) -> Self {
        MultipleFilePicker {
            id_source: id.into(),
            async_file_dialog: AsyncFileDialog::new(),
            paths
        }
    }

    pub fn dialog_title(mut self, title: impl Into<String>) -> Self {
        self.async_file_dialog = self.async_file_dialog.set_title(title);
        self
    }

    pub fn add_filter(mut self, name: impl Into<String>, extensions: &[impl ToString]) -> Self {
        self.async_file_dialog = self.async_file_dialog.add_filter(name, extensions);
        self
    }
}

impl<'a> egui::Widget for MultipleFilePicker<'a> {
    fn ui(self, ui: &mut Ui) -> Response {
        let maybe_handle = ui.data_mut(|ui|
            ui.get_temp_mut_or_default::<Arc<Mutex<MultipleFilePickerData>>>(self.id_source).clone()
        );
        let mut file_picker_data = maybe_handle.lock().unwrap();

        ui.group(|ui| {
            ui.horizontal(|ui| {
                ui.label("Source Files");
                ui.with_layout(Layout::right_to_left(Align::TOP), |ui| {
                    ui.add_enabled_ui(file_picker_data.selection.is_some(), |ui| {
                        if ui.button("🗑").clicked() {
                            self.paths.remove(file_picker_data.selection.unwrap());
                            file_picker_data.selection = None;
                        }
                    });

                    ui.add_enabled_ui(file_picker_data.selection.is_some_and(|index| index < self.paths.len() - 1), |ui| {
                        if ui.button("⬇").clicked() {
                            let index = file_picker_data.selection.unwrap();
                            self.paths.swap(index, index + 1);
                            file_picker_data.selection = Some(index + 1);
                        }
                    });

                    ui.add_enabled_ui(file_picker_data.selection.is_some_and(|index| index > 0), |ui| {
                        if ui.button("⬆").clicked() {
                            let index = file_picker_data.selection.unwrap();
                            self.paths.swap(index, index - 1);
                            file_picker_data.selection = Some(index - 1);
                        }
                    });
                });
            });
            egui_extras::TableBuilder::new(ui)
                .sense(Sense::click())
                .auto_shrink(true)
                .max_scroll_height(400.0)
                .striped(true)
                .column(Column::exact(120.0))
                .column(Column::exact(60.0))
                .header(20.0, |mut header| {
                    header.col(|ui| { ui.horizontal_centered(|ui| ui.strong("File Name")); });
                    header.col(|ui| { ui.horizontal_centered(|ui| ui.strong("Format")); });
                })
                .body(|mut body| {
                    for path in self.paths.iter_mut() {
                        body.row(20.0, |mut row| {
                            let row_index = row.index();

                            if file_picker_data.selection.is_some_and(|index| index == row_index) {
                                row.set_selected(true);
                            }

                            row.col(|ui| {
                                ui.horizontal_centered(|ui| ui.add(egui::Label::new(&path.short_name).selectable(false).truncate()));
                            });
                            row.col(|ui| {
                                if path.format_status.is_done() {
                                    let handle = path.format_status.unwrap_handle().unwrap();
                                    match handle.join().unwrap() {
                                        Ok(header_type) => path.format_status = HeaderReadingStatus::HeaderType(header_type),
                                        Err(message) => path.format_status = HeaderReadingStatus::Error(message),
                                    }
                                }
                                ui.horizontal_centered(|ui| {
                                    match &path.format_status {
                                        HeaderReadingStatus::InProgress(_) => {
                                            ui.spinner();
                                        },
                                        HeaderReadingStatus::HeaderType(checksum) => {
                                            match checksum {
                                                FormatType::External { checksum } => {
                                                    ui.add(egui::Label::new(format!("0x{:0>8x}", checksum)).selectable(false));
                                                }
                                                FormatType::Inline(_) => {
                                                    ui.add(egui::Label::new("Inline").selectable(false));
                                                }
                                            }
                                        }
                                        HeaderReadingStatus::Error(message) => {
                                            ui.add(egui::Label::new(RichText::new(message).color(Color32::RED)).truncate().selectable(false));
                                        }
                                    }
                                });
                            });

                            if row.response().clicked() {
                                if file_picker_data.selection.is_some_and(|index| index == row_index) {
                                    file_picker_data.selection = None;
                                } else {
                                    file_picker_data.selection = Some(row.index());
                                }
                            }
                        })
                    }
                });

            if let Some(handle) = file_picker_data.file_dialog_handle.take_if(|handle| handle.is_finished()) {
                let maybe_path = handle.join().unwrap();
                if let Some(paths) = maybe_path {
                    self.paths.extend(paths.into_iter());
                }
            }

            ui.add_space(6.0);

            ui.horizontal(|ui| {
                let choose_enabled = file_picker_data.file_dialog_handle.is_none();

                if ui.add_enabled(choose_enabled, egui::Button::new("Add Files")).clicked() {
                    // todo self.async_file_dialog.set_directory(dir)

                    let ctx_clone = ui.ctx().clone();
                    let pick_task = self.async_file_dialog.pick_files();

                    file_picker_data.file_dialog_handle = Some(thread::spawn(move || {
                        let file_paths = block_on(pick_task)
                            .map(|handles|
                                handles.into_iter().map(|handle| SelectedPath::from_path(handle.path())).collect()
                            );
                        ctx_clone.request_repaint_after(Duration::from_millis(100));
                        file_paths
                    }));
                }

                if ui.add_enabled(choose_enabled, egui::Button::new("Clear Files")).clicked() {
                    self.paths.clear();
                    file_picker_data.selection = None;
                }
            });

        }).response
    }
}
