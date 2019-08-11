use crate::prelude::*;
use futures::future::Future;
use std::any::Any;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use tokio::sync::mpsc::{unbounded_channel as channel, UnboundedSender as Sender};
use tokio::sync::oneshot;

macro_rules! womp {
    () => {
        &format!("{}:{}:{}", file!(), line!(), column!())
    };
    ($message:expr) => {
        &format!("{}:{}:{} {}", file!(), line!(), column!(), $message)
    };
}

thread_local! {
    static POOL: RefCell<Arc<RoundRobin>> = RefCell::new(Arc::new(RoundRobin {
        current: AtomicUsize::new(0),
        senders: Vec::new()
    }));
}

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
        self.senders.get(ix).expect(womp!()).clone()
    }

    // TODO: better error type here
    pub fn spawn<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(
        &self,
        f: F,
    ) -> impl Future<Item = R, Error = oneshot::error::RecvError> {
        let (sender, receiver) = oneshot::channel();
        let mut handle = self.next_sender();
        let task: SendableTask = Box::new(move || {
            let res: R = f();
            let boxed: Box<Any + Send + 'static> = Box::new(res);
            boxed
        });
        handle
            .try_send((sender, task))
            .unwrap_or_else(|_| panic!("failed to send - did you drop the runtime?",));
        receiver.map(|any| *any.downcast().expect(womp!()))
    }

    pub fn with_thread_lcoal() -> Arc<Self> {
        POOL.with(|p| p.borrow().clone())
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
        let mut pool = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let coro = coro.clone();
            let inner_spawner = spawner.clone();
            let receivers = receivers.clone();
            let after_start = after_start.clone();
            let before_stop = before_stop.clone();
            let id = inner_spawner.next_thread();
            dbg!(id);
            let handle = thread::Builder::new()
                .name(format!("{}_{}", name_prefix, id))
                .spawn(move || {
                    coro.set_thread_local().expect(womp!());

                    let sp2 = inner_spawner.clone();
                    POOL.with(move |o| o.replace(sp2));

                    let tasks = {
                        let mut lock = receivers.lock();
                        lock.get_mut(id).expect(womp!()).take().expect(womp!())
                    };
                    coro.run(move || {
                        after_start();
                        for (o, t) in tasks.iter_ok() {
                            let res = t();
                            o.send(res).expect(womp!())
                        }
                        before_stop();
                    })
                })?;
            pool.push(handle);
        }
        Ok(Runtime { pool, spawner })
    }
}

pub struct Runtime {
    pub pool: Vec<thread::JoinHandle<Result<(), crate::errors::StackError>>>,
    pub spawner: Arc<RoundRobin>,
}

#[derive(Clone)]
pub struct Handle {
    // pub inner: Vec<thread::JoinHandle>,
    pub spawner: Arc<RoundRobin>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::clock;
    use tokio::timer::Delay;

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
            .expect(womp!());

        let c2 = coro.clone();
        let result = coro
            .run(move || {
                let c = c2.clone();
                runtime
                    .spawner
                    .spawn(move || {
                        let result = c
                            .spawn(move || {
                                s2c.store(true, Ordering::Relaxed);
                                42usize
                            })
                            .expect(womp!())
                            .coro_wait()
                            .expect(womp!());
                        s1c.store(true, Ordering::Relaxed);
                        result
                    })
                    .coro_wait()
            })
            .expect(womp!());

        // Both coroutines run to finish
        assert!(s1.load(Ordering::Relaxed), "The outer closure didn't run");
        assert!(s2.load(Ordering::Relaxed), "The inner closure didn't run");
        // The result gets propagated through.
        assert_eq!(42, result.expect(womp!()));
    }

    /// Wait for a future to complete.
    #[test]
    fn future_wait() {
        let runtime = Builder::default().pool_size(4).build().expect(womp!());
        let (sender, receiver) = oneshot::channel();
        let all_done = runtime
            .spawner
            .spawn(move || receiver.coro_wait().expect(womp!()));
        let delayed = runtime.spawner.spawn(move || {
            let timeout = Delay::new(clock::now() + Duration::from_millis(50));
            timeout.coro_wait().expect(womp!());
            sender.send(42).expect(womp!());
        });
        let res = Future::wait(delayed.join(all_done));
        // drop(runtime);
        assert_eq!(42, res.expect(womp!()).1);
    }
}
