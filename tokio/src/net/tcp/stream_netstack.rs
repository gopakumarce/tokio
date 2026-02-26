cfg_not_wasi! {
    use crate::net::{to_socket_addrs, ToSocketAddrs};
    use std::future::poll_fn;
}

use crate::io::{AsyncRead, AsyncWrite, Interest, ReadBuf, Ready};
use crate::net::tcp::netstack_runtime::{poll_once, runtime};
use crate::net::tcp::split::{split, ReadHalf, WriteHalf};
use crate::net::tcp::split_owned::{split_owned, OwnedReadHalf, OwnedWriteHalf};

use netstack::TcpStream as NetTcpStream;

use std::fmt;
use std::io;
use std::net::{Shutdown, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};

cfg_io_util! {
    use bytes::BufMut;
}

pub struct TcpStream {
    io: NetTcpStream,
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
}

impl TcpStream {
    pub(crate) fn from_netstack(io: NetTcpStream, local_addr: SocketAddr, peer_addr: SocketAddr) -> Self {
        Self {
            io,
            local_addr,
            peer_addr,
        }
    }

    cfg_not_wasi! {
        pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<TcpStream> {
            let addrs = to_socket_addrs(addr).await?;

            let mut last_err = None;

            for addr in addrs {
                match TcpStream::connect_addr(addr).await {
                    Ok(stream) => return Ok(stream),
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

        async fn connect_addr(addr: SocketAddr) -> io::Result<TcpStream> {
            let _rt = runtime()?;
            let io = NetTcpStream::connect(addr)?;
            poll_fn(|cx| -> Poll<io::Result<()>> {
                if io.is_connected() {
                    Poll::Ready(Ok(()))
                } else {
                    poll_once();
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await?;

            Ok(TcpStream {
                io,
                local_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
                peer_addr: addr,
            })
        }
    }

    #[track_caller]
    pub fn from_std(_stream: std::net::TcpStream) -> io::Result<TcpStream> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "from_std is not supported by netstack backend",
        ))
    }

    pub fn into_std(self) -> io::Result<std::net::TcpStream> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "into_std is not supported by netstack backend",
        ))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        Ok(None)
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer_addr)
    }

    pub fn poll_peek(
        &self,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "peek is not supported by netstack backend",
        )))
    }

    pub async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        let mut ready = Ready::EMPTY;
        if interest.is_readable() {
            ready |= Ready::READABLE;
        }
        if interest.is_writable() {
            ready |= Ready::WRITABLE;
        }
        Ok(ready)
    }

    pub async fn readable(&self) -> io::Result<()> {
        self.ready(Interest::READABLE).await?;
        Ok(())
    }

    pub fn poll_read_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self.io.recv(buf) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Err(io::Error::from(io::ErrorKind::WouldBlock)),
            Err(e) => Err(e),
            Ok(0) => Err(io::Error::from(io::ErrorKind::WouldBlock)),
            Ok(n) => Ok(n),
        }
    }

    pub fn try_read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        if let Some(buf) = bufs.iter_mut().find(|b| !b.is_empty()) {
            self.try_read(buf)
        } else {
            Ok(0)
        }
    }

    cfg_io_util! {
        pub fn try_read_buf<B: BufMut>(&self, buf: &mut B) -> io::Result<usize> {
            let mut tmp = [0_u8; 8192];
            let n = self.try_read(&mut tmp)?;
            buf.put_slice(&tmp[..n]);
            Ok(n)
        }
    }

    pub async fn writable(&self) -> io::Result<()> {
        self.ready(Interest::WRITABLE).await?;
        Ok(())
    }

    pub fn poll_write_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        match self.io.send(buf) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Err(io::Error::from(io::ErrorKind::WouldBlock)),
            Err(e) => Err(e),
            Ok(0) => Err(io::Error::from(io::ErrorKind::WouldBlock)),
            Ok(n) => Ok(n),
        }
    }

    pub fn try_write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        if let Some(buf) = bufs.iter().find(|b| !b.is_empty()) {
            self.try_write(buf)
        } else {
            Ok(0)
        }
    }

    pub async fn peek(&self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "peek is not supported by netstack backend",
        ))
    }

    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match how {
            Shutdown::Write | Shutdown::Both => {
                self.io.shutdown_write();
                Ok(())
            }
            Shutdown::Read => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "read-half shutdown is not supported by netstack backend",
            )),
        }
    }

    pub(super) fn shutdown_std(&self, how: Shutdown) -> io::Result<()> {
        self.shutdown(how)
    }

    pub fn nodelay(&self) -> io::Result<bool> {
        self.io.nodelay()
    }

    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.io.set_nodelay(nodelay)
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

    pub fn split<'a>(&'a mut self) -> (ReadHalf<'a>, WriteHalf<'a>) {
        split(self)
    }

    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        split_owned(self)
    }

    pub(crate) fn poll_read_priv(
        &self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let dst = unsafe {
            &mut *(buf.unfilled_mut() as *mut [std::mem::MaybeUninit<u8>] as *mut [u8])
        };

        match self.io.recv(dst) {
            Ok(0) => {
                poll_once();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Ok(n) => {
                unsafe { buf.assume_init(n) };
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                poll_once();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    pub(super) fn poll_write_priv(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.io.send(buf) {
            Ok(0) => {
                poll_once();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                poll_once();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    pub(super) fn poll_write_vectored_priv(
        &self,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if let Some(buf) = bufs.iter().find(|b| !b.is_empty()) {
            self.poll_write_priv(cx, buf)
        } else {
            Poll::Ready(Ok(0))
        }
    }

    pub(super) fn is_write_vectored(&self) -> bool {
        true
    }
}

impl TryFrom<std::net::TcpStream> for TcpStream {
    type Error = io::Error;

    fn try_from(stream: std::net::TcpStream) -> Result<Self, Self::Error> {
        Self::from_std(stream)
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_read_priv(cx, buf)
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_priv(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        self.poll_write_vectored_priv(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shutdown(Shutdown::Write)?;
        Poll::Ready(Ok(()))
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpStream")
            .field("local_addr", &self.local_addr)
            .field("peer_addr", &self.peer_addr)
            .finish_non_exhaustive()
    }
}
