//! Exercise the actual S3 adapter's POST batching and per-object error handling.
use super::delete_candidates;
use anyhow::{Context, Result, ensure};
use object_store::{ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path};
use std::{
    collections::BTreeSet,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    time::{Duration, Instant},
};

fn paths(count: usize) -> Vec<Path> {
    (0..count).map(|index| Path::from(format!("pgderive/gc-test/{index}"))).collect()
}

fn store(listener: &TcpListener) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://{}", listener.local_addr()?))
            .with_bucket_name("gc-test")
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_skip_signature(true)
            .build()?,
    ))
}

fn request(socket: &mut TcpStream) -> Result<String> {
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut bytes = Vec::new();
    let mut block = [0_u8; 4096];
    loop {
        let count = socket.read(&mut block)?;
        ensure!(count > 0, "S3 request ended before its complete body");
        bytes.extend_from_slice(&block[..count]);
        ensure!(bytes.len() <= 128 * 1024, "unexpected oversized S3 request");
        let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") else {
            continue;
        };
        let header = std::str::from_utf8(&bytes[..end])?;
        ensure!(
            header.starts_with("POST /gc-test?delete HTTP/1.1\r\n"),
            "expected bulk-delete POST"
        );
        let length = header
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .context("missing Content-Length")?
            .1
            .trim()
            .parse::<usize>()?;
        if bytes.len() >= end + 4 + length {
            return Ok(std::str::from_utf8(&bytes[end + 4..end + 4 + length])?.into());
        }
    }
}

struct Capture {
    sizes: Vec<usize>,
    remaining: BTreeSet<String>,
    missing: usize,
}

fn delete_keys(body: &str, partial_error: bool, capture: &mut Capture) -> Result<()> {
    let mut count = 0;
    for fragment in body.split("<Key>").skip(1) {
        let (key, _) = fragment.split_once("</Key>").context("missing delete key end")?;
        count += 1;
        if partial_error && key == "pgderive/gc-test/0" {
            continue;
        }
        if !capture.remaining.remove(key) {
            capture.missing += 1;
        }
    }
    capture.sizes.push(count);
    Ok(())
}

fn serve(
    listener: &TcpListener,
    requests: usize,
    objects: usize,
    partial_error: bool,
) -> Result<Capture> {
    listener.set_nonblocking(true)?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut capture = Capture {
        sizes: Vec::new(),
        remaining: paths(objects).into_iter().map(|path| path.to_string()).collect(),
        missing: 0,
    };
    while capture.sizes.len() < requests {
        ensure!(Instant::now() < deadline, "S3 test server deadline exceeded");
        let mut socket = match listener.accept() {
            Ok((socket, _)) => socket,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(2));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let body = request(&mut socket)?;
        let fail = partial_error && capture.sizes.is_empty();
        delete_keys(&body, fail, &mut capture)?;
        let mut response = String::from("<DeleteResult>");
        for fragment in body.split("<Key>").skip(1) {
            let (key, _) = fragment.split_once("</Key>").context("missing response key")?;
            if fail && key == "pgderive/gc-test/0" {
                response.push_str("<Error><Key>pgderive/gc-test/0</Key><Code>AccessDenied</Code><Message>injected partial failure</Message></Error>");
            } else {
                response.push_str("<Deleted><Key>");
                response.push_str(key);
                response.push_str("</Key></Deleted>");
            }
        }
        response.push_str("</DeleteResult>");
        write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
            response.len()
        )?;
        socket.flush()?;
    }
    Ok(capture)
}

#[tokio::test]
async fn s3_gc_deletes_reduce_singleton_posts_to_bounded_batches() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let object_store = store(&listener)?;
    let server = tokio::task::spawn_blocking(move || serve(&listener, 513, 513, false));
    // Reproduce the exact former production loop through ObjectStoreExt::delete.
    for path in paths(513) {
        object_store.delete(&path).await?;
    }
    let baseline = server.await??;
    assert_eq!(baseline.sizes, vec![1; 513]);
    assert!(baseline.remaining.is_empty());
    assert_eq!(baseline.missing, 0);

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let object_store = store(&listener)?;
    let server = tokio::task::spawn_blocking(move || serve(&listener, 3, 513, false));
    delete_candidates(&object_store, Vec::new()).await?;
    delete_candidates(&object_store, paths(513)).await?;
    let batched = server.await??;
    assert_eq!(batched.sizes, vec![256, 256, 1]);
    assert!(batched.remaining.is_empty());
    assert_eq!(batched.missing, 0);
    Ok(())
}

#[tokio::test]
async fn s3_gc_partial_object_error_preserves_all_keys_for_idempotent_retry() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let object_store = store(&listener)?;
    let server = tokio::task::spawn_blocking(move || serve(&listener, 3, 257, true));
    // HTTP 200 reports one failed object while deleting the other 255. Failure
    // must prevent submitting the second application batch.
    assert!(delete_candidates(&object_store, paths(257)).await.is_err());
    delete_candidates(&object_store, paths(257)).await?;
    let capture = server.await??;
    assert_eq!(capture.sizes, vec![256, 256, 1]);
    assert!(capture.remaining.is_empty());
    // The retry explicitly re-deletes the 255 already removed keys. The failed
    // first key and previously unsubmitted final key are deleted successfully.
    assert_eq!(capture.missing, 255);
    Ok(())
}
