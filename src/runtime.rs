use crate::coroutine::{self, CoroutineResult};
use crate::prelude::*;
use crate::switch::BoxableTask;
use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::prelude::*;
use tokio::sync::mpsc::{
    error::UnboundedSendError as SendError, unbounded_channel as channel,
    UnboundedReceiver as Receiver, UnboundedSender as Sender,
};
use tokio::sync::oneshot;
use tokio_io_pool as tip;

type SendableTask = Box<FnOnce() -> Box<Any + Send + 'static> + Send + 'static>;

type SendableReq = (oneshot::Sender<Box<Any + Send + 'static>>, SendableTask);

pub struct RoundRobin {
    current: AtomicUsize,
    senders: Vec<Sender<SendableReq>>,
}

impl RoundRobin {
    pub fn next_thread(&self) -> usize {
        self.current.fetch_add(1, Ordering::SeqCst)
    }

    pub fn next_sender(&self) -> Sender<SendableReq> {
        let id = self.next_thread();
        let len = self.senders.len();
        let ix = id % len;
        self.senders.get(ix).unwrap().clone()
    }

    // TODO: better error type here
    pub fn spawn<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(&self, f: F) -> Option<R> {
        let (sender, receiver) = oneshot::channel();
        let mut handle = self.next_sender();
        let task: SendableTask = Box::new(move || {
            let res: R = f();
            let boxed: Box<Any + Send + 'static> = Box::new(res);
            boxed
        });
        handle.coro_send((sender, task)).ok()?;
        let any = receiver.coro_wait().ok()?;
        let res: Box<R> = any.downcast().ok()?;
        Some(*res)
    }
}

type SetupFn = Fn() + Send + Sync + 'static;

pub struct Builder {
    pool_size: usize,
    name_prefix: String,
    after_start: Box<SetupFn>,
    before_stop: Box<SetupFn>,
    coro: Coroutine,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            pool_size: num_cpus::get(),
            name_prefix: "".to_string(),
            after_start: Box::new(|| {}),
            before_stop: Box::new(|| {}),
            coro: Coroutine::new(),
        }
    }
}

impl Builder {
    fn pool_size(&mut self, size: usize) -> &mut Self {
        self.pool_size = size;
        self
    }

    fn name_prefix<S: Into<String>>(&mut self, s: S) -> &mut Self {
        self.name_prefix = s.into();
        self
    }

    fn after_start<F: Fn() + Send + Sync + 'static>(&mut self, f: F) -> &mut Self {
        let mut inner: Box<SetupFn> = Box::new(|| {});
        std::mem::swap(&mut inner, &mut self.after_start);
        let g: Box<SetupFn> = Box::new(move || {
            inner();
            f();
        });
        self.after_start = g;
        self
    }

    fn before_stop<F: Fn() + Send + Sync + 'static>(&mut self, f: F) -> &mut Self {
        let mut inner: Box<SetupFn> = Box::new(|| {});
        std::mem::swap(&mut inner, &mut self.before_stop);
        let g: Box<SetupFn> = Box::new(move || {
            inner();
            f();
        });
        self.before_stop = g;
        self
    }

    fn build(self) -> Result<Runtime, std::io::Error> {
        let Builder {
            pool_size,
            name_prefix,
            after_start,
            before_stop,
            coro,
        } = self;
        let mut senders = Vec::with_capacity(pool_size);
        let mut receivers = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let (s, r) = channel();
            senders.push(s);
            receivers.push(Some(r));
        }
        let receivers = parking_lot::Mutex::new(receivers);
        let spawner = Arc::new(RoundRobin {
            current: AtomicUsize::new(0),
            senders,
        });
        let inner_spawner = spawner.clone();
        let pool = tip::Builder::default()
            .pool_size(pool_size)
            .name_prefix(name_prefix)
            .after_start(move || {
                coro.set_thread_local();
                let id = inner_spawner.next_thread();
                let tasks = {
                    let mut lock = receivers.lock();
                    lock.get_mut(id).and_then(Option::take).unwrap()
                };
                tokio_current_thread::spawn(
                    tasks
                        .map_err(|e| panic!("{}", e))
                        .for_each(|(o, t)| {
                            coroutine::spawn(move || {
                                o.send(Box::new(t())).unwrap_or_else(|e| panic!("{:?}", e));
                            })
                        })
                        .map_err(|e| panic!("{}", e)),
                );
                after_start();
            })
            .before_stop(before_stop)
            .build()?;
        Ok(Runtime { pool, spawner })
    }
}

pub struct Runtime {
    pool: tip::Runtime,
    spawner: Arc<RoundRobin>,
}

#[derive(Clone)]
pub struct Handle {
    inner: tip::Handle,
    spawner: Arc<RoundRobin>,
}
