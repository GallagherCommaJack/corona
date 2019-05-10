extern crate corona;
extern crate futures;
extern crate futures_cpupool;
#[macro_use]
extern crate lazy_static;
extern crate may;
extern crate net2;
extern crate num_cpus;
extern crate tokio;

use std::env;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::sync::mpsc;
use std::thread;

use corona::io::BlockingWrapper;
use corona::prelude::*;
// use futures::prelude::await;
use futures::prelude::*;
use futures::{stream, Future, Stream};
use futures_cpupool::CpuPool;
use may::coroutine;
use may::net::TcpListener as MayTcpListener;
use net2::TcpBuilder;
use tokio::io;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::reactor::Handle;
use tokio::runtime::current_thread;

const BUF_SIZE: usize = 512;

fn get_var(name: &str, default: usize) -> usize {
    env::var(name)
        .map_err(|_| ())
        .and_then(|s| s.parse().map_err(|_| ()))
        .unwrap_or(default)
}

lazy_static! {
    static ref POOL: CpuPool = CpuPool::new_num_cpus();

    // Configuration bellow
    /// The number of connections to the server.
    ///
    /// This is what the clients aim for. But a client may be deleting or creating the connections
    /// at a time, so this is the upper limit. With multiple clients, this is split between them.
    static ref PARALLEL: usize = get_var("PARALLEL", 128);
    /// How many ping-pongs are done over each connection.
    static ref EXCHANGES: usize = get_var("EXCHANGES", 4);
    /// How many batches should happen before starting to measure.
    ///
    /// This allows the servers to get up to speed.
    static ref WARMUP: usize = get_var("WARMUP", 2);
    /// How many times to connect and disconnect all the connections in one testmark iteration.
    static ref BATCH: usize = get_var("BATCH", 4);
    /// Into how many client threads the client workload is spread.
    static ref CLIENT_THREADS: usize = get_var("CLIENT_THREADS", 32);
    /// Number of server instances in the `_many` scenarios.
    static ref SERVER_THREADS: usize = get_var("SERVER_THREADS", 2);
}

/// The client side
fn batter(addr: SocketAddr) {
    let mut streams = (0..*PARALLEL / *CLIENT_THREADS)
        .map(|_| TcpStream::connect(&addr).unwrap())
        .collect::<Vec<_>>();
    let input = [1u8; BUF_SIZE];
    let mut output = [0u8; BUF_SIZE];
    for _ in 0..*EXCHANGES {
        for stream in &mut streams {
            stream.write_all(&input[..]).unwrap();
        }
        for stream in &mut streams {
            stream.read_exact(&mut output[..]).unwrap();
        }
    }
}

fn gen_futures(listener: TcpListener) -> impl Future<Item = (), Error = ()> {
    TokioTcpListener::from_std(listener, &Handle::default())
        .unwrap()
        .incoming()
        .map_err(|e: std::io::Error| panic!("{}", e))
        .for_each(move |connection| {
            let buf = vec![0u8; BUF_SIZE];
            let perform = stream::iter_ok(0..*EXCHANGES)
                .fold((connection, buf), |(connection, buf), _i| {
                    io::read_exact(connection, buf)
                        .and_then(|(connection, buf)| io::write_all(connection, buf))
                })
                .map(|_| ())
                .map_err(|e: std::io::Error| panic!("{}", e));
            tokio::spawn(perform)
        })
}

fn run_futures(listener: TcpListener) {
    current_thread::block_on_all(gen_futures(listener)).unwrap();
}

fn run_futures_workstealing(listener: TcpListener) {
    tokio::run(gen_futures(listener));
}

fn test(paral: usize, body: fn(TcpListener)) {
    let listener = TcpBuilder::new_v4()
        .unwrap()
        .reuse_address(true)
        .unwrap()
        .bind("127.0.0.1:0")
        .unwrap()
        .listen(4096)
        .unwrap();
    let addr = listener.local_addr().unwrap();
    for _ in 0..paral {
        let listener = listener.try_clone().unwrap();
        thread::spawn(move || body(listener));
    }
    let (sender, receiver) = mpsc::sync_channel(*CLIENT_THREADS * 10);
    for _ in 0..*CLIENT_THREADS {
        let sender = sender.clone();
        let addr = addr;
        thread::spawn(move || {
            while let Ok(_) = sender.send(()) {
                for _ in 0..*BATCH {
                    batter(addr);
                }
            }
        });
    }
    for _ in 0..*WARMUP * *CLIENT_THREADS {
        receiver.recv().unwrap();
    }

    for _ in 0..*CLIENT_THREADS {
        receiver.recv().unwrap();
    }
}

fn run_corona(listener: TcpListener) {
    Coroutine::new()
        .run(move || {
            let incoming = TokioTcpListener::from_std(listener, &Handle::default())
                .unwrap()
                .incoming()
                .iter_ok();
            for mut connection in incoming {
                corona::spawn(move || {
                    let mut buf = [0u8; BUF_SIZE];
                    for _ in 0..*EXCHANGES {
                        io::read_exact(&mut connection, &mut buf[..])
                            .coro_wait()
                            .unwrap();
                        io::write_all(&mut connection, &buf[..])
                            .coro_wait()
                            .unwrap();
                    }
                });
            }
        })
        .unwrap();
}

fn run_corona_wrapper(listener: TcpListener) {
    Coroutine::new()
        .run(move || {
            let incoming = TokioTcpListener::from_std(listener, &Handle::default())
                .unwrap()
                .incoming()
                .iter_ok();
            for connection in incoming {
                corona::spawn(move || {
                    let mut buf = [0u8; BUF_SIZE];
                    let mut connection = BlockingWrapper::new(connection);
                    for _ in 0..*EXCHANGES {
                        connection.read_exact(&mut buf[..]).unwrap();
                        connection.write_all(&buf[..]).unwrap();
                    }
                });
            }
        })
        .unwrap();
}

fn run_may(listener: TcpListener) {
    // May can't change config later on… so all tests need to have the same config. Let's use the
    // same thing (number of real CPUs) as with the futures-cpupool, to have some illusion of
    // fairness.
    may::config()
        .set_workers(num_cpus::get())
        .set_io_workers(num_cpus::get());
    // May doesn't seem to support direct conversion
    let raw_fd = listener.into_raw_fd();
    let listener = unsafe { MayTcpListener::from_raw_fd(raw_fd) };
    while let Ok((mut connection, _address)) = listener.accept() {
        unsafe {
            coroutine::spawn(move || {
                let mut buf = [0u8; BUF_SIZE];
                for _ in 0..*EXCHANGES {
                    connection.read_exact(&mut buf[..]).unwrap();
                    connection.write_all(&buf[..]).unwrap();
                }
            })
        };
    }
}

/// Futures
fn futures() {
    test(1, run_futures);
    test(*SERVER_THREADS, run_futures);
    test(*SERVER_THREADS, run_futures_workstealing);
}

/// Our own corona.
fn corona() {
    test(1, run_corona);
    test(*SERVER_THREADS, run_corona);
    test(num_cpus::get(), run_corona);
}

/// Corona, but with the blocking wrapper
fn corona_blocking() {
    test(1, run_corona_wrapper);
    test(*SERVER_THREADS, run_corona_wrapper);
    test(num_cpus::get(), run_corona_wrapper);
}

/// May
fn may() {
    test(1, run_may);
    test(*SERVER_THREADS, run_may);
    test(num_cpus::get(), run_may);
}

fn main() {
    futures();
    corona();
    corona_blocking();
    may();
}
