use std::hash::Hash;
use std::num::NonZeroU32;
use std::ops::Bound;

use egui::Ui;
use eframe::Storage;

use dataframe::{DataFrameView, VirtualColumn};

use crate::DataShared;
use crate::{ProgressTask, Progress};

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
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
            StepType::ColEq => Step::ColEq(id, VirtualColumn::RowIndex, "".to_string()),
            StepType::Within => Step::Within(id, VirtualColumn::RowIndex, false, "".to_string(), false, "".to_string()),
            StepType::Sort => Step::Sort(id, false, VirtualColumn::RowIndex),
            StepType::Decimate => Step::Decimate(id, 2)
        }
    }
}

#[derive(Clone)]
enum Step {
    Fill(u64, bool, bool),
    ColEq(u64, VirtualColumn, String),
    Within(u64, VirtualColumn, bool, String, bool, String),
    Sort(u64, bool, VirtualColumn),
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
            &Step::Fill(_, _, and_before) => {
                let shape = df.shape();

                let mut prev_values: Vec<Option<NonZeroU32>> = vec![None; shape.cols];
                if and_before {
                    for row_idx in 0..shape.rows {
                        let row = df.row(row_idx);
                        let mut any_null = false;
                        for (i, &value) in row.raw_slice().iter().enumerate() {
                            if prev_values[i].is_none() {
                                if let Some(value) = value {
                                    prev_values[i] = Some(value);
                                } else {
                                    any_null = true;
                                }
                            }
                        }
                        if !any_null {
                            break;
                        }
                    }
                }

                for row_idx in 0..shape.rows {
                    let mut row = df.row_mut(row_idx);

                    for (i, prev_value) in prev_values.iter_mut().enumerate() {
                        if let Some(value) = row.get_col_raw(VirtualColumn::Column(i)) {
                            *prev_value = Some(value);
                        } else {
                            row.set_col_raw(i, *prev_value);
                        }
                    }

                    progress.set(row_idx as f32 / shape.rows as f32);
                }

                df
            }
            Step::ColEq(_, col_idx, value) => {
                let equal_to = df.df.col(*col_idx).data_type().parse_str(value);
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
                let dtype = df.df.col(*col_idx).data_type();
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
                df.filter_by(VirtualColumn::RowIndex, |i, _| {
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

pub fn column_select_combobox(ui: &mut Ui, id: impl Hash, selected_column: &mut VirtualColumn, df: &DataFrameView) {
    egui::ComboBox::from_id_salt(id)
        .wrap()
        .selected_text(df.col_name(*selected_column))
        .show_ui(ui, |ui| {
            ui.selectable_value(selected_column, VirtualColumn::RowIndex, "<row index>");
            for column_index in 0..df.shape().cols {
                ui.selectable_value(selected_column, VirtualColumn::Column(column_index), df.col_name(VirtualColumn::Column(column_index)));
            }
        });
}

pub struct ProcessTab {
    steps: Vec<Step>,
    step_id: u64,
    add_step_type: StepType,

    task: Option<ProgressTask<Result<DataFrameView, String>>>
}

impl ProcessTab {
    pub fn new(_cc: &eframe::CreationContext) -> ProcessTab {
        ProcessTab {
            steps: vec![
                Step::Sort(0, false, VirtualColumn::Column(1)),
                Step::Fill(1, true, true),
            ],
            step_id: 2,
            add_step_type: StepType::Fill,

            task: None
        }
    }

    pub fn save(&self, _storage: &mut dyn Storage) { }

    pub fn show(&mut self, ui: &mut Ui, shared: &mut Option<DataShared>) {
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

                                                column_select_combobox(ui, format!("combo-where-{id}"), col_idx, &shared.complete_data);
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

                                                column_select_combobox(ui, format!("combo-within-{id}"), col_idx, &shared.complete_data);
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
                                                egui::ComboBox::from_id_salt(format!("combo-sort-{id}"))
                                                    .selected_text(if *is_desc { "Descending" } else { "Ascending" })
                                                    .show_ui(ui, |ui| {
                                                        ui.selectable_value(is_desc, false, "Ascending");
                                                        ui.selectable_value(is_desc, true, "Descending");
                                                    });
                                            });

                                            ui.horizontal(|ui| {
                                                ui.label("By");

                                                column_select_combobox(ui, format!("combo-by-{id}"), col_idx, &shared.complete_data);
                                            });
                                        }
                                        Step::Decimate(_, factor) => {
                                            ui.horizontal(|ui| {
                                                ui.label("Factor");
                                                ui.add(egui::DragValue::new(factor).range(1..=50000))
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

                            egui::ComboBox::from_id_salt("add-type")
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
