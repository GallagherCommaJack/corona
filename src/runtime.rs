use crate::prelude::*;
use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel as channel, UnboundedSender as Sender};
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
        self.current.fetch_add(1, Ordering::Relaxed)
    }

    pub fn next_sender(&self) -> Sender<SendableReq> {
        let id = self.next_thread();
        let len = self.senders.len();
        let ix = id % len;
        self.senders
            .get(ix)
            .unwrap_or_else(|| panic!("{}", line!()))
            .clone()
    }

    // TODO: better error type here
    pub fn spawn<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(&self, f: F) -> R {
        let (sender, receiver) = oneshot::channel();
        let mut handle = self.next_sender();
        let task: SendableTask = Box::new(move || {
            let res: R = f();
            let boxed: Box<Any + Send + 'static> = Box::new(res);
            boxed
        });
        handle
            .coro_send((sender, task))
            .unwrap_or_else(|e| panic!("{}: {:?}", line!(), e));
        let any = receiver
            .coro_wait()
            .unwrap_or_else(|e| panic!("{}: {:?}", line!(), e));
        let res: Box<R> = any
            .downcast()
            .unwrap_or_else(|e| panic!("{}: {:?}", line!(), e));
        *res
    }
}

type SetupFn = Fn() + Send + Sync + 'static;

#[derive(Clone)]
pub struct Builder {
    pool_size: usize,
    name_prefix: String,
    after_start: Arc<SetupFn>,
    before_stop: Arc<SetupFn>,
    coro: Coroutine,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            pool_size: num_cpus::get(),
            name_prefix: "".to_string(),
            after_start: Arc::new(|| {}),
            before_stop: Arc::new(|| {}),
            coro: Coroutine::new(),
        }
    }
}

impl Builder {
    pub fn coro(&mut self, coro: Coroutine) -> &mut Self {
        self.coro = coro;
        self
    }

    pub fn pool_size(&mut self, size: usize) -> &mut Self {
        self.pool_size = size;
        self
    }

    pub fn name_prefix<S: Into<String>>(&mut self, s: S) -> &mut Self {
        self.name_prefix = s.into();
        self
    }

    pub fn after_start<F: Fn() + Send + Sync + 'static>(&mut self, f: F) -> &mut Self {
        let mut inner: Arc<SetupFn> = Arc::new(|| {});
        std::mem::swap(&mut inner, &mut self.after_start);
        let g: Arc<SetupFn> = Arc::new(move || {
            inner();
            f();
        });
        self.after_start = g;
        self
    }

    pub fn before_stop<F: Fn() + Send + Sync + 'static>(&mut self, f: F) -> &mut Self {
        let mut inner: Arc<SetupFn> = Arc::new(|| {});
        std::mem::swap(&mut inner, &mut self.before_stop);
        let g: Arc<SetupFn> = Arc::new(move || {
            inner();
            f();
        });
        self.before_stop = g;
        self
    }

    pub fn build(&self) -> Result<Runtime, std::io::Error> {
        let Builder {
            pool_size,
            name_prefix,
            after_start,
            before_stop,
            coro,
        } = self.clone();
        let mut senders = Vec::with_capacity(pool_size);
        let mut receivers = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let (s, r) = channel();
            senders.push(s);
            receivers.push(Some(r));
        }
        let receivers = Arc::new(parking_lot::Mutex::new(receivers));
        let spawner = Arc::new(RoundRobin {
            current: AtomicUsize::new(0),
            senders,
        });
        let pool = tip::Builder::default()
            .pool_size(pool_size)
            .name_prefix(name_prefix)
            .after_start(move || after_start())
            .before_stop(move || before_stop())
            .build()
            .unwrap_or_else(|e| panic!("{}: {}", line!(), e));
        for _ in 0..pool_size {
            let coro = coro.clone();
            let inner_spawner = spawner.clone();
            let receivers = receivers.clone();
            pool.spawn(futures::future::lazy(move || {
                coro.set_thread_local()
                    .unwrap_or_else(|e| panic!("{}: {}", line!(), e));
                let id = inner_spawner.next_thread();
                let tasks = {
                    let mut lock = receivers.lock();
                    lock.get_mut(id)
                        .unwrap_or_else(|| panic!("{}", line!()))
                        .take()
                        .unwrap_or_else(|| panic!("{}", line!()))
                };
                let c2 = coro.clone();
                coro.spawn(move || {
                    for (o, t) in tasks.iter_ok() {
                        o.send(t())
                            .unwrap_or_else(|e| panic!("{}: {:?}", line!(), e));
                    }
                })
                .unwrap_or_else(|e| panic!("{}: {}", line!(), e));
                futures::future::ok::<(), ()>(())
            }))
            .unwrap_or_else(|e| panic!("{}: {}", line!(), e));
        }
        // .after_start(move || {
        //     after_start();
        // })
        Ok(Runtime { pool, spawner })
    }
}

pub struct Runtime {
    pub pool: tip::Runtime,
    pub spawner: Arc<RoundRobin>,
}

impl Runtime {}

#[derive(Clone)]
pub struct Handle {
    pub inner: tip::Handle,
    pub spawner: Arc<RoundRobin>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn spawn_some() {
        let s1 = Arc::new(AtomicBool::new(false));
        let s2 = Arc::new(AtomicBool::new(false));
        let s1c = s1.clone();
        let s2c = s2.clone();

        let mut coro = Coroutine::new();
        coro.stack_size(40960);

        let runtime = Builder::default()
            .coro(coro.clone())
            .build()
            .unwrap_or_else(|e| panic!("{}: {}", line!(), e));

        let c2 = coro.clone();
        let result = coro.run(move || {
            let c = c2.clone();
            runtime.spawner.spawn(move || {
                let result = c
                    .spawn(move || {
                        s2c.store(true, Ordering::Relaxed);
                        42usize
                    })
                    .unwrap_or_else(|e| panic!("{}: {}", line!(), e))
                    .coro_wait()
                    .unwrap_or_else(|e| panic!("{}: {}", line!(), e));
                s1c.store(true, Ordering::Relaxed);
                result
            })
        });

        // Both coroutines run to finish
        assert!(s1.load(Ordering::Relaxed), "The outer closure didn't run");
        assert!(s2.load(Ordering::Relaxed), "The inner closure didn't run");
        // The result gets propagated through.
        assert_eq!(42, result.unwrap_or_else(|e| panic!("{}: {}", line!(), e)));
    }
}
