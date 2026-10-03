use crate::{
    Config,
    transaction::{Decoder, Transaction},
};
use anyhow::{Context, Result, bail};
use pgwire_replication::{ReplicationClient, ReplicationEvent};
use postgres_replication::protocol::LogicalReplicationMessage;
use std::io::{self, Write};
use tokio::sync::{mpsc, oneshot};

/// Print complete committed source transactions as newline-delimited JSON.
///
/// This diagnostic listener does not advance durable replication progress.
/// Stop with Ctrl-C, or configure a nonzero transaction limit.
///
/// # Errors
/// Returns connection, protocol, unsupported-message, output, or shutdown errors.
pub async fn listen(config: Config) -> Result<()> {
    run(config, None, None).await
}

pub async fn run(
    config: Config,
    ready: Option<oneshot::Sender<()>>,
    observed: Option<mpsc::Sender<Transaction>>,
) -> Result<()> {
    config.validate()?;
    let mut client = ReplicationClient::connect(config.replication_config())
        .await
        .context("connecting replication stream")?;
    eprintln!("replication connected; diagnostic mode, durable acknowledgement disabled");
    if let Some(ready) = ready {
        let _ = ready.send(());
    }
    let outcome = receive(&config, &mut client, observed.as_ref()).await;
    let shutdown = client.shutdown().await.context("shutting down replication stream");
    crate::outcome::combine(outcome, shutdown, "replication shutdown")
}

async fn receive(
    config: &Config,
    client: &mut ReplicationClient,
    observed: Option<&mpsc::Sender<Transaction>>,
) -> Result<()> {
    let mut decoder = Decoder::default();
    let mut committed = 0;
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    loop {
        let event = tokio::select! {
            signal = &mut interrupted => { signal.context("listening for Ctrl-C")?; return Ok(()); }
            event = client.recv() => event?.context("replication stream ended unexpectedly")?,
        };
        match event {
            ReplicationEvent::Begin { xid, .. } => decoder.begin(xid)?,
            ReplicationEvent::XLogData { data, .. } => decoder.message(
                &LogicalReplicationMessage::parse(&data)?,
                config.listener.max_transaction_changes,
            )?,
            ReplicationEvent::Commit { lsn, end_lsn, .. } => {
                let transaction = decoder.commit(lsn.to_string(), end_lsn.to_string())?;
                emit(&transaction)?;
                if let Some(observed) = observed {
                    observed.send(transaction).await.context("harness stopped receiving")?;
                }
                committed += 1;
                if config.listener.max_transactions != 0
                    && committed >= config.listener.max_transactions
                {
                    return Ok(());
                }
            }
            ReplicationEvent::KeepAlive { .. } => {}
            other => bail!("unsupported replication event: {other:?}"),
        }
    }
}

fn emit(transaction: &Transaction) -> Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, transaction)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}
