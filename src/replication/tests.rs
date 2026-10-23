use super::*;
use anyhow::ensure;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, timeout},
};

async fn fixture() -> Result<(Client, TcpStream)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let config = ReplicationConfig::new("127.0.0.1", "user", "secret", "db", "slot", "publication")
        .with_port(listener.local_addr()?.port())
        .with_tls(pgwire_replication::TlsConfig::disabled())
        .with_buffer_size(1)
        .with_status_interval(Duration::from_millis(10));
    let client = Client::connect(config);
    let (mut socket, _) = listener.accept().await?;
    let length = usize::try_from(socket.read_i32().await?)?;
    let mut startup = vec![0; length - 4];
    socket.read_exact(&mut startup).await?;
    ensure!(
        startup
            .windows(b"application_name\0pgwire-replication\0".len())
            .any(|w| w == b"application_name\0pgwire-replication\0")
    );
    Ok((client, socket))
}
async fn frame(socket: &mut TcpStream, tag: u8, payload: &[u8]) -> Result<()> {
    socket.write_u8(tag).await?;
    socket.write_i32(i32::try_from(payload.len())? + 4).await?;
    socket.write_all(payload).await?;
    Ok(())
}
async fn frontend(socket: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let tag = socket.read_u8().await?;
    let length = usize::try_from(socket.read_i32().await?)?;
    let mut payload = vec![0; length - 4];
    socket.read_exact(&mut payload).await?;
    Ok((tag, payload))
}
async fn authenticated(socket: &mut TcpStream) -> Result<()> {
    frame(socket, b'R', &0_i32.to_be_bytes()).await?;
    frame(socket, b'Z', b"I").await?;
    let (tag, query) = frontend(socket).await?;
    ensure!(tag == b'Q');
    ensure!(query.starts_with(b"START_REPLICATION SLOT slot LOGICAL 0/0 "));
    Ok(())
}
async fn streaming(socket: &mut TcpStream) -> Result<()> {
    authenticated(socket).await?;
    frame(socket, b'W', &[0, 0, 0]).await
}
async fn oversized(socket: &mut TcpStream) -> Result<()> {
    // No payload is sent: rejection must happen from the header alone.
    socket.write_u8(b'R').await?;
    socket.write_i32(i32::try_from(MAX_PAYLOAD)? + 5).await?;
    Ok(())
}
async fn sasl_continue(socket: &mut TcpStream) -> Result<()> {
    let mut mechanisms = 10_i32.to_be_bytes().to_vec();
    mechanisms.extend_from_slice(b"SCRAM-SHA-256\0\0");
    frame(socket, b'R', &mechanisms).await?;
    let (tag, initial) = frontend(socket).await?;
    ensure!(tag == b'p');
    let initial = String::from_utf8(initial)?;
    let (_, nonce) = initial.rsplit_once("r=").context("SCRAM nonce missing")?;
    let mut response = 11_i32.to_be_bytes().to_vec();
    response.extend_from_slice(format!("r={nonce}server,s=c2FsdA==,i=4096").as_bytes());
    frame(socket, b'R', &response).await?;
    ensure!(frontend(socket).await?.0 == b'p');
    Ok(())
}
#[tokio::test]
async fn oversized_headers_fail_before_payload_in_every_connection_phase() -> Result<()> {
    for phase in 0..5 {
        let (mut client, mut socket) = fixture().await?;
        match phase {
            1 => {
                let mut mechanisms = 10_i32.to_be_bytes().to_vec();
                mechanisms.extend_from_slice(b"SCRAM-SHA-256\0\0");
                frame(&mut socket, b'R', &mechanisms).await?;
                frontend(&mut socket).await?;
            }
            2 => sasl_continue(&mut socket).await?,
            3 => authenticated(&mut socket).await?,
            4 => streaming(&mut socket).await?,
            _ => {}
        }
        oversized(&mut socket).await?;
        let error = timeout(Duration::from_secs(2), client.recv())
            .await?
            .err()
            .context("accepted oversized header")?;
        ensure!(error.to_string().contains("large"), "phase {phase}: {error:#}");
        client.shutdown().await?;
    }
    Ok(())
}
#[tokio::test]
async fn cleartext_auth_is_supported_and_md5_remains_rejected() -> Result<()> {
    for code in [3_i32, 5] {
        let (mut client, mut socket) = fixture().await?;
        frame(&mut socket, b'R', &code.to_be_bytes()).await?;
        if code == 3 {
            let (tag, password) = frontend(&mut socket).await?;
            ensure!(tag == b'p' && password == b"secret\0");
            streaming(&mut socket).await?;
            client.shutdown().await?;
        } else {
            let error = client.recv().await.err().context("accepted MD5")?;
            ensure!(error.to_string().contains("unsupported auth method code: 5"));
        }
    }
    for offered in [b"SCRAM-SHA-256".as_slice(), b"\0SCRAM-SHA-256\0"] {
        let (mut client, mut socket) = fixture().await?;
        let mut payload = 10_i32.to_be_bytes().to_vec();
        payload.extend_from_slice(offered);
        frame(&mut socket, b'R', &payload).await?;
        let error = timeout(Duration::from_secs(2), client.recv())
            .await?
            .err()
            .context("accepted malformed SASL offer")?;
        ensure!(error.to_string().contains("does not offer SCRAM-SHA-256"));
    }
    Ok(())
}
async fn keepalive(socket: &mut TcpStream, end: u64) -> Result<()> {
    let mut payload = vec![b'k'];
    payload.extend_from_slice(&end.to_be_bytes());
    payload.extend_from_slice(&0_i64.to_be_bytes());
    payload.push(0);
    frame(socket, b'd', &payload).await
}
async fn feedback(socket: &mut TcpStream) -> Result<u64> {
    let (tag, payload) = frontend(socket).await?;
    ensure!(tag == b'd' && payload.first() == Some(&b'r'));
    let write = u64::from_be_bytes(payload[1..9].try_into()?);
    ensure!(payload[1..9] == payload[9..17] && payload[9..17] == payload[17..25]);
    Ok(write)
}
#[tokio::test]
async fn blocked_delivery_retains_events_and_sends_only_monotone_durable_feedback() -> Result<()> {
    let (mut client, mut socket) = fixture().await?;
    streaming(&mut socket).await?;
    for end in 1..=64 {
        keepalive(&mut socket, end).await?;
    }
    for _ in 0..3 {
        ensure!(timeout(Duration::from_secs(2), feedback(&mut socket)).await?? == 0);
    }
    let acknowledge = client.durable_feedback();
    acknowledge(Lsn::from_u64(50));
    acknowledge(Lsn::from_u64(20));
    let mut reached = false;
    for _ in 0..5 {
        reached |= timeout(Duration::from_secs(2), feedback(&mut socket)).await?? == 50;
    }
    ensure!(reached, "durable feedback stalled under backpressure");
    for end in 1..=64 {
        let event = timeout(Duration::from_secs(2), client.recv()).await??;
        ensure!(
            matches!(event, Some(ReplicationEvent::KeepAlive{wal_end, ..}) if wal_end.as_u64()==end)
        );
    }
    client.shutdown().await?;
    while frontend(&mut socket).await?.0 != b'c' {}
    Ok(())
}
#[tokio::test]
async fn partial_header_survives_feedback_and_shutdown_cancels_startup() -> Result<()> {
    let (mut client, mut socket) = fixture().await?;
    streaming(&mut socket).await?;
    socket.write_all(&[b'd', 0, 0]).await?;
    for _ in 0..3 {
        timeout(Duration::from_secs(2), feedback(&mut socket)).await??;
    }
    socket.write_all(&[0, 22]).await?;
    let mut payload = vec![b'k'];
    payload.extend_from_slice(&42_u64.to_be_bytes());
    payload.extend_from_slice(&0_i64.to_be_bytes());
    payload.push(1);
    socket.write_all(&payload).await?;
    ensure!(matches!(
        client.recv().await?,
        Some(ReplicationEvent::KeepAlive { reply_requested: true, .. })
    ));
    client.shutdown().await?;
    let (mut client, _socket) = fixture().await?;
    timeout(Duration::from_secs(2), client.shutdown()).await??;
    Ok(())
}
#[tokio::test]
async fn server_errors_propagate_and_unix_tls_is_rejected() -> Result<()> {
    let (mut client, mut socket) = fixture().await?;
    frame(&mut socket, b'E', b"SERROR\0CXX000\0Mtest failure\0\0").await?;
    ensure!(client.recv().await.is_err());
    let config = ReplicationConfig::new("/tmp", "user", "secret", "db", "slot", "publication");
    ensure!(handshake::connect(&config).await.is_err());
    Ok(())
}

#[tokio::test]
async fn framing_accepts_exact_limit_and_rejects_invalid_signed_lengths() -> Result<()> {
    let mut bytes = vec![b'd'];
    bytes.extend_from_slice(&(i32::try_from(MAX_PAYLOAD)? + 4).to_be_bytes());
    bytes.resize(MAX_PAYLOAD + 5, 7);
    let mut input = bytes.as_slice();
    let mut reader = MessageReader::new().with_max_message_size(MAX_PAYLOAD);
    ensure!(reader.read(&mut input).await?.payload.len() == MAX_PAYLOAD);
    for length in [-1_i32, 0, 3, i32::MAX] {
        let mut header = vec![b'R'];
        header.extend_from_slice(&length.to_be_bytes());
        let mut input = header.as_slice();
        let mut reader = MessageReader::new().with_max_message_size(MAX_PAYLOAD);
        ensure!(reader.read(&mut input).await.is_err());
    }
    Ok(())
}
