//! Private, bounded credential documents; errors never include credential contents.
use anyhow::{Result, ensure};
use serde::Deserialize;
use std::{fs::File, io::Read};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Credentials {
    pub(super) access_key_id: String,
    pub(super) secret_access_key: String,
    #[serde(default)]
    pub(super) session_token: Option<String>,
}
pub(super) fn load(path: &std::path::Path) -> Result<Credentials> {
    let file = open(path)?;
    let metadata = file.metadata()?;
    ensure!(metadata.is_file(), "credential path must be a regular file");
    private(&metadata)?;
    let mut bytes = Vec::new();
    file.take(65_537).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 65_536, "credential document exceeds byte limit");
    let credentials: Credentials = serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("invalid credential document"))?;
    ensure!(
        !credentials.access_key_id.is_empty()
            && !credentials.secret_access_key.is_empty()
            && credentials.session_token.as_ref().is_none_or(|token| !token.is_empty()),
        "credential document has missing or empty credentials"
    );
    Ok(credentials)
}
#[cfg(unix)]
fn private(metadata: &std::fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    ensure!(
        metadata.permissions().mode().trailing_zeros() >= 6,
        "credential file must deny group and other permissions"
    );
    Ok(())
}
#[cfg(not(unix))]
fn private(_metadata: &std::fs::Metadata) -> Result<()> {
    anyhow::bail!("private credential files require Unix permission enforcement")
}

#[cfg(unix)]
fn open(path: &std::path::Path) -> Result<File> {
    use rustix::fs::{Mode, OFlags};
    // NONBLOCK prevents FIFO opens from waiting before the descriptor is attested.
    let descriptor =
        rustix::fs::open(path, OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC, Mode::empty())?;
    Ok(File::from(descriptor))
}
#[cfg(not(unix))]
fn open(_path: &std::path::Path) -> Result<File> {
    anyhow::bail!("private credential files require Unix permission enforcement")
}
