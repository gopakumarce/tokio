#![warn(rust_2018_idioms)]
#![cfg(all(feature = "full", not(target_os = "wasi"), not(miri)))]

use std::io;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::thread;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

fn parse_env_ipv4(key: &str, default: &str) -> io::Result<Ipv4Addr> {
    let value = std::env::var(key).unwrap_or_else(|_| default.to_owned());
    value.parse::<Ipv4Addr>().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid value for {}: {} ({})", key, value, e),
        )
    })
}

fn stack_ip() -> io::Result<Ipv4Addr> {
    parse_env_ipv4("TOKIO_NETSTACK_STACK_IP", "10.200.0.2")
}

fn host_ip() -> io::Result<Ipv4Addr> {
    parse_env_ipv4("TOKIO_NETSTACK_HOST_IP", "10.200.0.1")
}

async fn with_timeout<T, F>(dur: Duration, fut: F, label: &'static str) -> io::Result<T>
where
    F: std::future::Future<Output = io::Result<T>>,
{
    match timeout(dur, fut).await {
        Ok(res) => res,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("timeout waiting for {}", label),
        )),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn netstack_backend_roundtrip() -> io::Result<()> {
    let (listener, addr) = if cfg!(feature = "netstack-backend") {
        let ip = stack_ip()?;
        let addr = SocketAddr::from((ip, 18080));
        (TcpListener::bind(addr).await?, addr)
    } else {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        (listener, addr)
    };

    let client = thread::spawn(move || -> io::Result<()> {
        thread::sleep(Duration::from_millis(100));
        let mut stream = std::net::TcpStream::connect(addr)?;
        stream.write_all(b"ping")?;

        let mut echoed = [0_u8; 4];
        stream.read_exact(&mut echoed)?;
        if &echoed != b"ping" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected echo: {:?}", echoed),
            ));
        }
        Ok(())
    });

    let (mut server, _) =
        with_timeout(Duration::from_secs(6), listener.accept(), "accept()").await?;
    let mut buf = [0_u8; 4];
    with_timeout(
        Duration::from_secs(6),
        server.read_exact(&mut buf),
        "server read",
    )
    .await?;
    with_timeout(
        Duration::from_secs(6),
        server.write_all(&buf),
        "server write",
    )
    .await?;

    match client.join() {
        Ok(res) => res,
        Err(_) => Err(io::Error::other("client thread panicked")),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn netstack_backend_unsupported_ops() -> io::Result<()> {
    let listener = if cfg!(feature = "netstack-backend") {
        let ip = stack_ip()?;
        TcpListener::bind(SocketAddr::from((ip, 18081))).await?
    } else {
        TcpListener::bind("127.0.0.1:0").await?
    };

    if cfg!(feature = "netstack-backend") {
        let err = listener
            .into_std()
            .expect_err("netstack backend should not support into_std");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    } else {
        let _std_listener = listener
            .into_std()
            .expect("default backend should support into_std");
    }

    let std_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let std_addr = std_listener.local_addr()?;

    let t = thread::spawn(move || {
        let _ = std::net::TcpStream::connect(std_addr);
    });
    let (std_stream, _) = std_listener.accept()?;
    let _ = t.join();

    if cfg!(feature = "netstack-backend") {
        let err = TcpStream::from_std(std_stream)
            .expect_err("netstack backend should not support from_std");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    } else {
        std_stream.set_nonblocking(true)?;
        let _stream =
            TcpStream::from_std(std_stream).expect("default backend should support from_std");
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn netstack_backend_client_roundtrip() -> io::Result<()> {
    let std_listener = if cfg!(feature = "netstack-backend") {
        std::net::TcpListener::bind("0.0.0.0:0")?
    } else {
        std::net::TcpListener::bind("127.0.0.1:0")?
    };
    let addr = std_listener.local_addr()?;
    let client_addr = if cfg!(feature = "netstack-backend") {
        SocketAddr::from((host_ip()?, addr.port()))
    } else {
        SocketAddr::from((Ipv4Addr::LOCALHOST, addr.port()))
    };

    let server = thread::spawn(move || -> io::Result<()> {
        let (mut conn, _) = std_listener.accept()?;
        let mut buf = [0_u8; 5];
        conn.read_exact(&mut buf)?;
        if &buf != b"hello" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected client payload: {:?}", buf),
            ));
        }
        conn.write_all(b"world")?;
        Ok(())
    });

    let mut stream = with_timeout(
        Duration::from_secs(6),
        TcpStream::connect(client_addr),
        "client connect",
    )
    .await?;

    with_timeout(
        Duration::from_secs(6),
        stream.write_all(b"hello"),
        "client write",
    )
    .await?;

    let mut echoed = [0_u8; 5];
    with_timeout(
        Duration::from_secs(6),
        stream.read_exact(&mut echoed),
        "client read",
    )
    .await?;
    assert_eq!(&echoed, b"world");

    match server.join() {
        Ok(res) => res,
        Err(_) => Err(io::Error::other("server thread panicked")),
    }
}
