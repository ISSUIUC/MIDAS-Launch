use std::fs::{File, OpenOptions};
use std::{io, io::Write};
use std::io::BufWriter;
use std::path::PathBuf;
use std::process::{Command, Stdio};

//Make some python script that takes the weird broken exported data and 
//transforms it into a beautiful thing that makes gnc and structures happy

use egui::{Color32, Ui};
use eframe::Storage;

use crate::UpdateContext;
use crate::computation::ProgressTask;
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

                                //declared python mut file so python gets the data as well
                                let log_file = File::create("python_output.log")?;
                                let mut child = Command::new("python3")
                                    .arg("PythonScripts/makecsv.py") // adjust path if needed
                                    .stdin(Stdio::piped())
                                    .stdout(Stdio::from(log_file))
                                    .spawn()?;
                                    

                                let mut py_stdin = child.stdin.take().expect("Failed to open stdin for Python");

                                let mut file;

                                if is_append {//if currently adding already

                                    file = BufWriter::new(OpenOptions::new().write(true).append(true).open(&path)?);

                                } else {//if new file has to be made
                                    file = BufWriter::new(File::create(&path)?);

                                    let mut col_iterator = data.col_names(); //go thru columns and add col names
                                    if let Some(name) = col_iterator.next() {
                                        write!(&mut file, "{}", name)?;

                                        while let Some(name) = col_iterator.next() {
                                            write!(&mut file, ",{}", name)?;
                                        }

                                        file.write(&[b'\n'])?; //write those names
                                    }
                                }

                                //send those same headers to python
                                writeln!(py_stdin, "HEADERS:{}", data.col_names().collect::<Vec<_>>().join(","))?;

                                let total_rows = data.shape().rows;
                                for idx in 0..total_rows {

                                    let row_str = data.row(idx)
                                        .iter()
                                        .map(|v| v.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",");

                                    let mut row_iterator = data.row(idx).iter();

                                    if let Some(data) = row_iterator.next() {

                                        //row_str.push_str(&data.to_string()); //i hope this doesnt break
                                        //writeln!(&mut file, "{}", row_str)?;
                                        write!(&mut file, "{}", data)?;

                                        while let Some(data) = row_iterator.next() {
                                            write!(&mut file, ",{}", data)?;
                                        }
                                    }
                                    file.write(&[b'\n'])?;


                                    //send row to Python
                                    writeln!(py_stdin, "ROW:{}", row_str)?;

                                    progress.set(idx as f32 / total_rows as f32);
                                }

                                file.flush()?;
                                child.wait()?;

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
