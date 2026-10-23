use super::Socket;
use anyhow::{Result, bail, ensure};
use pgwire_replication::{
    ReplicationConfig,
    auth::ScramClient,
    protocol::{
        framing::{MessageReader, write_password_message, write_query, write_startup_message},
        messages::parse_auth_request,
    },
};
use tokio::io::{AsyncRead, AsyncWrite};

pub(super) async fn connect(config: &ReplicationConfig) -> Result<Socket> {
    #[cfg(unix)]
    if config.is_unix_socket() {
        ensure!(
            config.tls.mode == pgwire_replication::SslMode::Disable,
            "TLS is not supported over Unix sockets"
        );
        return Ok(Box::new(tokio::net::UnixStream::connect(config.unix_socket_path()).await?));
    }
    let tcp = tokio::net::TcpStream::connect((config.host.as_str(), config.port)).await?;
    tcp.set_nodelay(true)?;
    Ok(Box::new(
        pgwire_replication::tls::rustls::maybe_upgrade_to_tls(tcp, &config.tls, &config.host)
            .await?,
    ))
}
pub(super) async fn start<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    reader: &mut MessageReader,
    config: &ReplicationConfig,
) -> Result<()> {
    let mut params = vec![
        ("user", config.user.as_str()),
        ("database", config.database.as_str()),
        ("replication", "database"),
        ("client_encoding", "UTF8"),
        ("application_name", "pgwire-replication"),
    ];
    if let Some(options) = &config.options {
        params.push(("options", options));
    }
    write_startup_message(stream, 196_608, &params).await?;
    authenticate(stream, reader, config).await?;
    let sql = format!(
        "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}', messages 'true')",
        config.slot,
        config.start_lsn,
        config
            .publication
            .names()
            .iter()
            .map(|name| name.replace('\'', "''"))
            .collect::<Vec<_>>()
            .join(",")
    );
    write_query(stream, &sql).await?;
    loop {
        let message = reader.read(stream).await?;
        super::messages::check_error(&message)?;
        if message.tag == b'W' {
            return Ok(());
        }
    }
}
async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    reader: &mut MessageReader,
    config: &ReplicationConfig,
) -> Result<()> {
    loop {
        let message = reader.read(stream).await?;
        super::messages::check_error(&message)?;
        match message.tag {
            b'Z' => return Ok(()),
            b'R' => {
                let (code, data) = parse_auth_request(&message.payload)?;
                match code {
                    0 => {}
                    3 => {
                        let mut password = config.password.as_bytes().to_vec();
                        password.push(0);
                        write_password_message(stream, &password).await?;
                    }
                    10 => scram(stream, reader, config, data).await?,
                    _ => bail!("unsupported auth method code: {code}"),
                }
            }
            _ => {}
        }
    }
}
async fn scram<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    reader: &mut MessageReader,
    config: &ReplicationConfig,
    mechanisms: &[u8],
) -> Result<()> {
    ensure!(
        mechanisms
            .split_inclusive(|b| *b == 0)
            .take_while(|m| *m != b"\0")
            .any(|m| m == b"SCRAM-SHA-256\0"),
        "server does not offer SCRAM-SHA-256"
    );
    let client = ScramClient::new(&config.user);
    let mut initial = b"SCRAM-SHA-256\0".to_vec();
    initial.extend_from_slice(&i32::try_from(client.client_first.len())?.to_be_bytes());
    initial.extend_from_slice(client.client_first.as_bytes());
    write_password_message(stream, &initial).await?;
    let first = auth_data(stream, reader, 11).await?;
    let (final_message, authentication, salted) = client.client_final(&config.password, &first)?;
    write_password_message(stream, final_message.as_bytes()).await?;
    let final_message = auth_data(stream, reader, 12).await?;
    ScramClient::verify_server_final(&final_message, &salted, &authentication)?;
    Ok(())
}
async fn auth_data<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    reader: &mut MessageReader,
    expected: i32,
) -> Result<String> {
    loop {
        let message = reader.read(stream).await?;
        super::messages::check_error(&message)?;
        if message.tag == b'R' {
            let (code, data) = parse_auth_request(&message.payload)?;
            ensure!(code == expected, "unexpected auth code {code}, expected {expected}");
            return Ok(String::from_utf8_lossy(data).into_owned());
        }
    }
}
