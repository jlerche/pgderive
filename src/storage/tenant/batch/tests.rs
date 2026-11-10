//! Exercise the production gather loop with virtual time, without a database.
use super::*;
use std::time::Duration;

fn config(target: usize) -> Result<Config> {
    let mut config: Config = serde_json::from_value(serde_json::json!({
        "postgres":{"host":"localhost","port":5432,"database":"test","user":"test","tls":"disable"},
        "schema":"test","object_prefix":"test"
    }))?;
    config.batching.target_bytes = target;
    config.batching.delay_ms = 25;
    Ok(config)
}

async fn request(size: usize) -> Result<Request> {
    let admission = Arc::new(Semaphore::new(2));
    let (reply, _response) = oneshot::channel();
    Ok(Request {
        chunk: Chunk {
            catalog: Catalog { tenant: "tenant".into(), catalog: "catalog".into() },
            name: "object".into(),
            bytes: vec![0; size].into(),
            checksum: String::new(),
        },
        tenant_permit: admission.clone().acquire_owned().await?,
        global_permit: admission.acquire_owned().await?,
        reply,
    })
}

#[tokio::test(start_paused = true)]
async fn virtual_batch_deadline_accepts_arrivals_without_waiting_for_wall_time() -> Result<()> {
    let config = config(16)?;
    let (sender, mut queue) = mpsc::channel(2);
    let arrival = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        sender.send(request(4).await?).await.map_err(|_| anyhow::anyhow!("queue closed"))?;
        // Keep the contributor connected beyond the batch deadline.
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok::<_, anyhow::Error>(())
    });
    let started = tokio::time::Instant::now();
    let (batch, next) = gather(request(4).await?, &mut queue, &config).await;
    assert_eq!(started.elapsed(), Duration::from_millis(25));
    assert_eq!(batch.len(), 2);
    assert!(next.is_none());
    arrival.await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn virtual_batch_overflow_preserves_the_next_request_without_delay() -> Result<()> {
    let config = config(8)?;
    let (sender, mut queue) = mpsc::channel(2);
    sender.send(request(6).await?).await.map_err(|_| anyhow::anyhow!("queue closed"))?;
    let started = tokio::time::Instant::now();
    let (batch, next) = gather(request(4).await?, &mut queue, &config).await;
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(batch.len(), 1);
    assert_eq!(next.context("overflow request lost")?.chunk.bytes.len(), 6);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn virtual_batch_closed_queue_drains_immediately() -> Result<()> {
    let config = config(16)?;
    let (sender, mut queue) = mpsc::channel(2);
    drop(sender);
    let started = tokio::time::Instant::now();
    let (batch, next) = gather(request(4).await?, &mut queue, &config).await;
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(batch.len(), 1);
    assert!(next.is_none());
    Ok(())
}
