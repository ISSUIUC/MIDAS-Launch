use std::sync::{Arc, Mutex};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::Duration;

use egui::{Response, Ui};
use futures_lite::future::block_on;
use rfd::AsyncFileDialog;

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

                    *lock = Some(std::thread::spawn(move || {
                        let file_path = block_on(pick_task);
                        let file_path = file_path.map(|handle| handle.path().to_owned());
                        ctx_clone.request_repaint_after(Duration::from_millis(100));
                        file_path
                    }));
                } else {
                    let pick_task = dialog.pick_file();

                    *lock = Some(std::thread::spawn(move || {
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