use netstack::{init, NetStack};

use std::cell::RefCell;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(super) struct NetstackRuntime {
    pub(super) stack: Arc<NetStack>,
}

thread_local! {
    static RUNTIME: RefCell<Option<NetstackRuntime>> = const { RefCell::new(None) };
}

static RUNTIME_INIT_LOCK: Mutex<()> = Mutex::new(());

fn parse_env_i32(key: &str, default: i32) -> io::Result<i32> {
    match std::env::var(key) {
        Ok(value) => value.parse::<i32>().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid value for {}: {} ({})", key, value, e),
            )
        }),
        Err(_) => Ok(default),
    }
}

fn parse_env_ipv4(key: &str, default: &str) -> io::Result<Ipv4Addr> {
    let value = std::env::var(key).unwrap_or_else(|_| default.to_owned());
    value.parse::<Ipv4Addr>().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid value for {}: {} ({})", key, value, e),
        )
    })
}

pub(super) fn runtime() -> io::Result<NetstackRuntime> {
    if let Some(runtime) = RUNTIME.with(|rt| rt.borrow().clone()) {
        return Ok(runtime);
    }

    let _guard = RUNTIME_INIT_LOCK
        .lock()
        .map_err(|_| io::Error::other("netstack runtime init lock poisoned"))?;

    if let Some(runtime) = RUNTIME.with(|rt| rt.borrow().clone()) {
        return Ok(runtime);
    }

    let unit = parse_env_i32("TOKIO_NETSTACK_UNIT", 10)?;
    let host_ip = parse_env_ipv4("TOKIO_NETSTACK_HOST_IP", "10.200.0.1")?;
    let stack_ip = parse_env_ipv4("TOKIO_NETSTACK_STACK_IP", "10.200.0.2")?;
    let netmask = parse_env_ipv4("TOKIO_NETSTACK_NETMASK", "255.255.255.0")?;

    let runtime = NetstackRuntime {
        stack: {
            init().map_err(io::Error::other)?;
            Arc::new(NetStack::new(
                unit,
                IpAddr::V4(host_ip),
                IpAddr::V4(stack_ip),
                IpAddr::V4(netmask),
            )?)
        },
    };

    RUNTIME.with(|rt| {
        *rt.borrow_mut() = Some(runtime.clone());
    });

    Ok(runtime)
}

pub(super) fn poll_once() {
    if let Some(runtime) = RUNTIME.with(|rt| rt.borrow().clone()) {
        let _ = runtime.stack.poll_once();
    }
}
