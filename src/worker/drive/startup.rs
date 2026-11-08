use super::{Config, Context, Duration, Open, Result, Session, Settings, ensure, timeout, watch};
use crate::{acknowledged::Stream, catalog::Durable};

pub(super) struct Opened {
    pub(super) open: Open,
    pub(super) stream: Stream,
    pub(super) durable: Durable,
    pub(super) running: bool,
}

pub(super) async fn open(
    config: &Config,
    settings: &Settings,
    session: Session,
    control: (&mut watch::Receiver<bool>, &mut crate::worker::epoch::Count),
) -> Result<Opened> {
    let (stop, count) = control;
    let resources = config.capacity.clone();
    let recovery = match &resources {
        Some(resources) => Some(tokio::select! {
            permit=resources.process.recovery.clone().acquire_owned() => permit.context("recovery admission closed")?,
            signal=stop.changed() => {
                signal.context("worker stop monitor closed")?;
                anyhow::bail!("shutdown interrupted recovery admission");
            }
        }),
        None => None,
    };
    let opening = Open::connect_with_session(config, settings, session);
    let mut open = timeout(Duration::from_secs(settings.startup_timeout_secs), Box::pin(opening))
        .await
        .context("worker startup/recovery timed out")??;
    let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
    count.resolve(&durable)?;
    let mut stream = Stream::connect(&open.replication, &durable).await?;
    if !crate::worker::replay::monitored(&mut open, &mut stream, &durable, stop).await? {
        ensure!(
            durable.checkpoint_end == durable.end,
            "shutdown interrupted WAL replay before durable drain"
        );
        return Ok(Opened { open, stream, durable, running: false });
    }
    drop(recovery);
    let durable = open.writer.confirmed(&mut open.session.client, open.query.plan()).await?;
    Ok(Opened { open, stream, durable, running: true })
}
