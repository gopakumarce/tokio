//! TCP utility types.

#[cfg(not(feature = "netstack-backend"))]
pub(crate) mod listener;
#[cfg(feature = "netstack-backend")]
#[path = "listener_netstack.rs"]
pub(crate) mod listener;

cfg_not_wasi! {
    #[cfg(not(feature = "netstack-backend"))]
    pub(crate) mod socket;
}

mod split;
pub use split::{ReadHalf, WriteHalf};

mod split_owned;
pub use split_owned::{OwnedReadHalf, OwnedWriteHalf, ReuniteError};

#[cfg(feature = "netstack-backend")]
mod netstack_runtime;

#[cfg(not(feature = "netstack-backend"))]
pub(crate) mod stream;
#[cfg(feature = "netstack-backend")]
#[path = "stream_netstack.rs"]
pub(crate) mod stream;
pub(crate) use stream::TcpStream;
