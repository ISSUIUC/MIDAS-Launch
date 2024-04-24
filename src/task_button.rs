// use std::collections::HashMap;
// use std::fs::File;
// use std::io;
// use std::io::BufReader;
// use byteorder::LittleEndian;
//
// use egui::{Response, Ui, WidgetText};
//
// use crate::dataframe::{DataFrame, DataType};
// use crate::format::FastFormatBuilder;
// use crate::ProgressTask;
//
// struct TaskButton<T, F> {
//     text: WidgetText,
//     processing_text: WidgetText,
//     disabled_text: WidgetText,
//
//     enabled: bool,
//
//     task: F,
//     progress_task: Option<ProgressTask<T>>
// }
//
//
// impl<T> egui::Widget for TaskButton<T> {
//     fn ui(self, ui: &mut Ui) -> Response {
//         if let Some(task) = &self.progress_task {
//             ui.horizontal(|ui| {
//                 ui.add_enabled(false, egui::Button::new(&self.processing_text));
//                 ui.add(egui::ProgressBar::new(task.progress()).show_percentage());
//             }).response
//         } else {
//             if let (Some(loaded_format), true) = (&self.loaded_format, !self.source_path.is_empty()) {
//                 let response = ui.add_enabled(true, egui::Button::new(&self.text));
//
//                 if response.clicked() {
//                     self.parsing_message = None;
//                     shared.take();
//                     let format = loaded_format.clone();
//                     let source_path = self.source_path.clone();
//
//                     self.parsing = Some(ProgressTask::new(ui.ctx(), move |progress| {
//                         let mut dataframe = DataFrame::new();
//                         dataframe.add_null_col("sensor", DataType::Enum);
//                         dataframe.add_null_col("timestamp", DataType::Integer);
//
//                         let mut variants = HashMap::new();
//                         for (name, (disc, format)) in format.variants {
//                             let mut builder = FastFormatBuilder::new(&mut dataframe);
//                             format.to_fast(&mut builder, &name);
//                             let fast_format = builder.finish();
//                             variants.insert(disc, (name, fast_format));
//                         }
//
//                         let num_cols = dataframe.shape().cols;
//
//                         let mut file = BufReader::new(File::open(source_path)?);
//                         let mut offset: u64 = 0;
//                         let size = file.get_ref().metadata().map_or(u64::MAX, |meta| meta.len());
//
//                         let _checksum = file.read_u32::<LittleEndian>()?; offset += 4;
//
//                         let result: io::Error = crate::try_catch!({
//                                 let mut i = 0;
//                                 let mut row = vec![Data::Null; num_cols];
//                                 loop {
//                                     row.fill(Data::Null);
//
//                                     let determinant = file.read_u32::<LittleEndian>()?; offset += 4;
//                                     let timestamp_ms = file.read_u32::<LittleEndian>()?; offset += 4;
//
//                                     let (name, fast_format) = variants.get(&determinant)
//                                         .ok_or_else(|| io::Error::other(format!("No variant for discriminant {}", determinant)))?;
//
//                                     row[0] = Data::Str(name);
//                                     row[1] = Data::Integer(timestamp_ms as i64);
//
//                                     fast_format.parse(&mut file, &mut row)?;
//                                     offset += fast_format.size as u64;
//
//                                     dataframe.add_row(&row);
//
//                                     i += 1;
//                                     if i % 3000 == 0 {
//                                         progress.set(offset as f32 / size as f32);
//                                     }
//                                 }
//
//                                 Ok(())
//                             }).unwrap_err();
//
//                         dataframe.hint_complete();
//
//                         if result.kind() == io::ErrorKind::UnexpectedEof {
//                             Ok(dataframe)
//                         } else {
//                             Err(result)
//                         }
//                     }));
//                 }
//             } else {
//                 ui.add_enabled(false, egui::Button::new(&self.text)).on_disabled_hover_text(&self.disabled_text);
//             }
//         }
//     }
// }