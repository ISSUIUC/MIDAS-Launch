use std::cell::Cell;
use std::sync::{Arc, Mutex, atomic::{AtomicU32, Ordering}};
use std::thread::{spawn, JoinHandle};
use std::time::Duration;

use egui::Context;

pub enum Computation<T, E> {
    Empty,
    Computing(JoinHandle<(Context, Result<T, E>)>),
    Ok(T),
    Err(E)
}

impl<T, E> Computation<T, E> {
    pub fn check_complete(&mut self) -> bool {
        if let Computation::Computing(handle) = self {
            if handle.is_finished() {
                let Computation::Computing(handle) = std::mem::replace(self, Computation::Empty) else { unreachable!() };
                let (context, result) = handle.join().unwrap();
                match result {
                    Ok(value) => *self = Computation::Ok(value),
                    Err(error) => *self = Computation::Err(error)
                }
                context.request_repaint();
                return true;
            }
        }
        false
    }

    // pub fn reset(&mut self) {
    //     *self = Computation::Empty;
    // }

    pub fn is_computing(&self) -> bool {
        matches!(self, Computation::Computing(_))
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            Computation::Ok(value) => Some(value),
            _ => None
        }
    }

    pub fn take_if_done(&mut self) -> Option<Result<T, E>> {
        match std::mem::replace(self, Computation::Empty) {
            Computation::Ok(value) => Some(Ok(value)),
            Computation::Err(error) => Some(Err(error)),
            other => { *self = other; None }
        }
    }

    pub fn take_error(&mut self) -> Option<E> {
        match self {
            Computation::Err(_) => {
                let Computation::Err(error) = std::mem::replace(self, Computation::Empty) else { unreachable!() };
                Some(error)
            }
            _ => None
        }
    }
}

impl<T: Send + 'static, E: Send + 'static> Computation<T, E> {
    pub fn begin_new<F: 'static + Send + FnOnce() -> Result<T, E>>(ctx: Context, f: F) -> Self {
        Computation::Computing(spawn(move || {
            let value = f();
            ctx.request_repaint_after(Duration::from_millis(100));
            (ctx, value)
        }))
    }

    pub fn begin<F: 'static + Send + FnOnce() -> Result<T, E>>(&mut self, ctx: Context, f: F) {
        *self = Computation::Computing(spawn(move || {
            let value = f();
            ctx.request_repaint_after(Duration::from_millis(100));
            (ctx, value)
        }))
    }
}

#[derive(Clone)]
pub struct Progress {
    context: Context,
    contents: Arc<(AtomicU32, Mutex<String>)>,
    local_progress: Cell<f32>
}

impl Progress {
    pub fn set_text(&self, text: String) {
        let mut lock = self.contents.1.lock().unwrap();
        *lock = text;
        self.context.request_repaint_after(Duration::from_millis(16));
    }

    pub fn reset_progress(&self) {
        self.local_progress.set(0.0);
        self.contents.0.store(0.0f32.to_bits(), Ordering::SeqCst);
    }

    pub fn set(&self, amount: f32) {
        if (amount * 100.0).floor() > (self.local_progress.get() * 100.0).floor() {
            self.local_progress.set(amount);
            self.contents.0.store(amount.to_bits(), Ordering::SeqCst);
            self.context.request_repaint_after(Duration::from_millis(16));
        }
    }
}

pub struct ProgressTask<T> {
    pub handle: JoinHandle<T>, // todo unify with computation
    progress: Progress
}

impl<T> ProgressTask<T> where T: Send + 'static {
    pub fn new(ctx: &Context, f: impl FnOnce(&Progress) -> T + Send + 'static) -> ProgressTask<T> {
        let progress = Progress {
            context: ctx.clone(),
            contents: Arc::new((0.into(), Mutex::new("".into()))),
            local_progress: Cell::new(0.0)
        };
        let progress_clone = progress.clone();

        let handle = spawn(move || {
            let res = f(&progress_clone);
            progress_clone.context.request_repaint_after(Duration::from_millis(16));
            res
        });

        ProgressTask { handle, progress }
    }

    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub fn progress(&self) -> f32 {
        f32::from_bits(self.progress.contents.0.load(Ordering::SeqCst))
    }

    pub fn text(&self) -> String {
        self.progress.contents.1.lock().unwrap().clone()
    }
}