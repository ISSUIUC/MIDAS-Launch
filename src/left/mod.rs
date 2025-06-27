use eframe::Storage;
use egui::panel::Side;
use crate::DrawContext;
use crate::left::export::ExportTab;
use crate::left::import::ImportTab;
use crate::left::process::ProcessTab;

mod process;
mod import;
mod export;

pub struct Left {
    state: LeftState,
    import_tab: ImportTab,
    process_tab: ProcessTab,
    export_tab: ExportTab,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum LeftState {
    Import,
    Filter,
    Export
}

impl Left {
    pub fn new(cc: &eframe::CreationContext) -> Self {
        Left {
            state: LeftState::Import,
            import_tab: ImportTab::new(cc),
            process_tab: ProcessTab::new(cc),
            export_tab: ExportTab::new(cc),
        }
    }

    pub fn draw(&mut self, ctx: DrawContext) {
        egui::SidePanel::new(Side::Left, "left-panel")
            // .default_width(180.0)
            .min_width(240.0)
            .max_width(400.0)
            .show(ctx.ctx, |ui| {
                ui.add_space(3.0);
                ui.columns(3, |columns| {
                    columns[0].vertical_centered_justified(|ui| {
                        ui.selectable_value(&mut self.state, LeftState::Import, "Import")
                    });
                    columns[1].vertical_centered_justified(|ui| {
                        ui.add_enabled_ui(ctx.data.is_some(), |ui| {
                            ui.selectable_value(&mut self.state, LeftState::Filter, "Filter")
                        });
                    });
                    columns[2].vertical_centered_justified(|ui| {
                        ui.add_enabled_ui(ctx.data.is_some(), |ui| {
                            ui.selectable_value(&mut self.state, LeftState::Export, "Export")
                        });
                    });
                });
                ui.separator();

                match self.state {
                    LeftState::Import => {
                        self.import_tab.show(ui, ctx);
                    }
                    LeftState::Filter => {
                        self.process_tab.show(ui, ctx);
                    }
                    LeftState::Export => {
                        self.export_tab.show(ui, ctx);
                    }
                };
            });
    }

    pub fn save(&mut self, storage: &mut dyn Storage) {
        self.import_tab.save(storage);
        self.process_tab.save(storage);
        self.export_tab.save(storage);
    }
}