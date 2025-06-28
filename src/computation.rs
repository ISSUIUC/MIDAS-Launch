use std::thread::{spawn, JoinHandle};
use std::time::Duration;
use egui::Context;

pub enum Computation<T, E> {
    Empty,
    Computing(Context, JoinHandle<Result<T, E>>),
    Ok(T),
    Err(E)
}

impl<T, E> Computation<T, E> {
    pub fn check_complete(&mut self) -> bool {
        if let Computation::Computing(_, handle) = self {
            if handle.is_finished() {
                let Computation::Computing(context, handle) = std::mem::replace(self, Computation::Empty) else { unreachable!() };
                match handle.join().unwrap() {
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
        matches!(self, Computation::Computing(_, _))
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            Computation::Ok(value) => Some(value),
            _ => None
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
    pub fn begin<F: 'static + Send + FnOnce() -> Result<T, E>>(&mut self, ctx: &Context, f: F) {
        let ctx = ctx.clone();
        *self = Computation::Computing(ctx.clone(), spawn(move || {
            let value = f();
            ctx.request_repaint_after(Duration::from_millis(100));
            value
        }))
    }
}