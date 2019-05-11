#![feature(generators, proc_macro_hygiene, test)]

//! Minimal benchmarks and comparison of some IO manipulation.
//!
//! This tries to compare speed of different methods how to implement a networked server. The
//! servers differ, while the client is always the same.
//!
//! The client opens `PARALLEL` connections to the server, then `EXCHANGES` times sends a buffer of
//! data through each of the connection and expects an answer back.
//!
//! Note that we leave the server threads running after the benchmark terminates, to avoid the need
//! to synchronize shut down. As they just sit there inactive, this should have no real effect on
//! the performance.
//!
//! The `*_many` variants run the listener in multiple independent threads.

extern crate corona;
extern crate futures;
extern crate futures_cpupool;
#[macro_use]
extern crate lazy_static;
// extern crate may;
extern crate net2;
extern crate num_cpus;
extern crate test;
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
// use may::coroutine;
// use may::net::TcpListener as MayTcpListener;
use net2::TcpBuilder;
use test::Bencher;
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
    /// How many times to connect and disconnect all the connections in one benchmark iteration.
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

/// Performs one benchmark, with the body as the server implementation
///
/// There's a short warm-up before the actual benchmark starts ‒ both to initialize whatever
/// buffers or caches the library uses and to make sure the server already started after the
/// barrier.
///
/// We run the clients in multiple threads (so the server is kept busy). To not start and stop a
/// lot of client threads, we report the progress through a sync channel.
fn bench(b: &mut Bencher, paral: usize, body: fn(TcpListener)) {
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
    b.iter(move || {
        // One iteration is when all the threads perform the whole batch. This is approximate (they
        // don't do it at the same time), but it should cancel out over the many iterations.
        for _ in 0..*CLIENT_THREADS {
            receiver.recv().unwrap();
        }
    });
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

/// Our own corona.
#[bench]
fn corona(b: &mut Bencher) {
    bench(b, 1, run_corona);
}

#[bench]
fn corona_many(b: &mut Bencher) {
    bench(b, *SERVER_THREADS, run_corona);
}

#[bench]
fn corona_cpus(b: &mut Bencher) {
    bench(b, num_cpus::get(), run_corona);
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

/// Corona, but with the blocking wrapper
#[bench]
fn corona_blocking_wrapper(b: &mut Bencher) {
    bench(b, 1, run_corona_wrapper);
}

#[bench]
fn corona_blocking_wrapper_many(b: &mut Bencher) {
    bench(b, *SERVER_THREADS, run_corona_wrapper);
}

#[bench]
fn corona_blocking_wrapper_cpus(b: &mut Bencher) {
    bench(b, num_cpus::get(), run_corona_wrapper);
}

fn run_threads(listener: TcpListener) {
    while let Ok((mut connection, _address)) = listener.accept() {
        thread::spawn(move || {
            let mut buf = [0u8; BUF_SIZE];
            for _ in 0..*EXCHANGES {
                connection.read_exact(&mut buf[..]).unwrap();
                connection.write_all(&buf[..]).unwrap();
            }
        });
    }
    drop(listener); // Just to prevent clippy warning
}

/// Runs a fresh thread for each connection
///
/// This might happen to be slightly faster because it may use more CPU parallelism. Or maybe
/// there's just less overhead due to the event loop ‒ but there's the price to switch threads.
#[bench]
fn threads(b: &mut Bencher) {
    bench(b, 1, run_threads);
}

#[bench]
fn threads_many(b: &mut Bencher) {
    bench(b, *SERVER_THREADS, run_threads);
}

#[bench]
fn threads_cpus(b: &mut Bencher) {
    bench(b, num_cpus::get(), run_threads);
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

/// Just plain futures.
///
/// While faster than corona, it is probably harder to read and write.
#[bench]
fn futures(b: &mut Bencher) {
    bench(b, 1, run_futures);
}

#[bench]
fn futures_many(b: &mut Bencher) {
    bench(b, *SERVER_THREADS, run_futures);
}

#[bench]
fn futures_cpus(b: &mut Bencher) {
    bench(b, *SERVER_THREADS, run_futures);
}

#[bench]
fn futures_workstealing(b: &mut Bencher) {
    bench(b, 1, run_futures_workstealing);
}

fn run_futures_cpupool(listener: TcpListener) {
    let main = TokioTcpListener::from_std(listener, &Handle::default())
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
                .map(|_| ());
            let offloaded = POOL
                .spawn(perform)
                .map(|_| ())
                .map_err(|e: std::io::Error| panic!("{}", e));
            tokio::spawn(offloaded)
        });
    current_thread::block_on_all(main).unwrap();
}

/// Like `futures`, but uses cpu pool.
#[bench]
fn futures_cpupool(b: &mut Bencher) {
    bench(b, 1, run_futures_cpupool);
}

#[bench]
fn futures_cpupool_many(b: &mut Bencher) {
    bench(b, *SERVER_THREADS, run_futures_cpupool);
}

#[bench]
fn futures_cpupool_cpus(b: &mut Bencher) {
    bench(b, num_cpus::get(), run_futures_cpupool);
}
