use std::fs::{File, OpenOptions};
use std::{io, io::Write};
use std::io::BufWriter;
use std::path::PathBuf;

use egui::{Color32, Ui};
use eframe::Storage;

use crate::{UpdateContext, ProgressTask};
use crate::file_picker::FilePicker;


#[derive(Copy, Clone, Eq, PartialEq)]
enum ExportFormats {
    Csv
}

struct CsvExport {
    path: String,
    append_mode: bool,

    export: Option<ProgressTask<Result<(), io::Error>>>,
    msg: Option<String>
}

pub struct ExportTab {
    export: ExportFormats,
    csv: CsvExport
}

impl ExportTab {
    pub fn new(_cc: &eframe::CreationContext) -> ExportTab {
        ExportTab {
            export: ExportFormats::Csv,
            csv: CsvExport {
                path: String::new(),
                append_mode: false,

                export: None,
                msg: None
            }
        }
    }

    pub fn save(&self, _storage: &mut dyn Storage) { }

    pub fn show(&mut self, ui: &mut Ui, ctx: UpdateContext) {
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

                            let data = ctx.data.as_ref().unwrap().shown_data.clone();
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
                                    let mut row_iterator = data.row(idx).iter();
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
        }
    }
}
