//! Install service termination handlers before starting any query work.
use std::{future::Future, io};

#[cfg(unix)]
pub(super) fn listen() -> io::Result<impl Future<Output = io::Result<()>>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        let received = tokio::select! {
            received = interrupt.recv() => received,
            received = terminate.recv() => received,
        };
        received.ok_or_else(|| io::Error::other("worker signal stream closed"))
    })
}

#[cfg(not(unix))]
pub(super) fn listen() -> io::Result<impl Future<Output = io::Result<()>>> {
    Ok(tokio::signal::ctrl_c())
}
