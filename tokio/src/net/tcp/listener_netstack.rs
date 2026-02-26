use crate::net::tcp::TcpStream;
use crate::net::tcp::netstack_runtime::{poll_once, runtime};

cfg_not_wasi! {
    use crate::net::{to_socket_addrs, ToSocketAddrs};
}

use netstack::TcpListener as NetTcpListener;

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::net::{self, SocketAddr};
use std::task::{Context, Poll};

pub struct TcpListener {
    io: NetTcpListener,
    local_addr: SocketAddr,
}

impl TcpListener {
    cfg_not_wasi! {
        pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<TcpListener> {
            let addrs = to_socket_addrs(addr).await?;
            let mut last_err = None;

            for addr in addrs {
                match TcpListener::bind_addr(addr) {
                    Ok(listener) => return Ok(listener),
                    Err(e) => last_err = Some(e),
                }
            }

            Err(last_err.unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "could not resolve to any address",
                )
            }))
        }

        fn bind_addr(addr: SocketAddr) -> io::Result<TcpListener> {
            let _rt = runtime()?;
            let listener = NetTcpListener::bind(addr)?;
            Ok(TcpListener {
                io: listener,
                local_addr: addr,
            })
        }
    }

    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let stream = poll_fn(|cx| match self.io.accept() {
            Ok(Some(stream)) => Poll::Ready(Ok(stream)),
            Ok(None) => {
                poll_once();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        })
        .await?;
        let peer = SocketAddr::from(([0, 0, 0, 0], 0));
        Ok((TcpStream::from_netstack(stream, self.local_addr, peer), peer))
    }

    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        match self.io.accept() {
            Ok(Some(stream)) => {
                let peer = SocketAddr::from(([0, 0, 0, 0], 0));
                Poll::Ready(Ok((
                    TcpStream::from_netstack(stream, self.local_addr, peer),
                    peer,
                )))
            }
            Ok(None) => {
                poll_once();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    #[track_caller]
    pub fn from_std(_listener: net::TcpListener) -> io::Result<TcpListener> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "from_std is not supported by netstack backend",
        ))
    }

    pub fn into_std(self) -> io::Result<std::net::TcpListener> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "into_std is not supported by netstack backend",
        ))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    pub fn ttl(&self) -> io::Result<u32> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IP_TTL is not supported by netstack backend",
        ))
    }

    pub fn set_ttl(&self, _ttl: u32) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IP_TTL is not supported by netstack backend",
        ))
    }
}

impl TryFrom<net::TcpListener> for TcpListener {
    type Error = io::Error;

    fn try_from(stream: net::TcpListener) -> Result<Self, Self::Error> {
        Self::from_std(stream)
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpListener")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}
