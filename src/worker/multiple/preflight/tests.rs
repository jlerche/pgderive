use super::{directory, validate};
use crate::Config;
use anyhow::Result;
use serde_json::json;

fn config(schema: &str, tenant: &str, prefix: &str) -> Result<Config> {
    Ok(serde_json::from_value(json!({
        "postgres":{"host":"localhost","port":5432,"database":"dev","user":"postgres","tls":"disable"},
        "replication":{"slot":schema,"publication":"sources"},
        "object_store":{"endpoint":"http://127.0.0.1:8333","bucket":"pgderive-test","credentials_file":"credentials.json"},
        "worker":{"tenant_id":tenant,"catalog_schema":schema,"query_id":"totals","sink_table":"totals",
            "object_prefix":prefix,"block_rows":64,"maintenance_ticks":0,"retry_attempts":2,"retry_delay_ms":10,
            "max_transactions":0,"query":{"sql":"SELECT * FROM source"}}
    }))?)
}

#[test]
fn conflicting_catalogs_and_tenants_are_rejected_before_io() -> Result<()> {
    let left = config("catalog", "alice", "root")?;
    assert!(validate(&[left.clone(), left.clone()]).is_err());
    assert!(validate(&[left.clone(), config("catalog", "bob", "root")?]).is_err());
    assert!(validate(&[left.clone(), config("peer", "bob", "root/child")?]).is_ok());
    assert!(validate(&[left, config("peer", "bob", "root-sibling")?]).is_ok());
    Ok(())
}

#[test]
fn same_catalog_queries_use_exactly_one_root() -> Result<()> {
    let left = config("catalog", "alice", "root")?;
    let mut right = left.clone();
    let worker = right.worker.as_mut().ok_or_else(|| anyhow::anyhow!("worker missing"))?;
    worker.query_id = "peer".into();
    worker.sink_table = "peer".into();
    assert!(validate(&[left.clone(), right.clone()]).is_ok());
    right.worker.as_mut().ok_or_else(|| anyhow::anyhow!("worker missing"))?.object_prefix =
        "other".into();
    assert!(validate(&[left.clone(), right.clone()]).is_err());
    drop(left);
    drop(right);
    Ok(())
}

#[test]
fn cache_paths_resolve_existing_symlinks_and_missing_children() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let actual = temp.path().join("actual");
    std::fs::create_dir(&actual)?;
    #[cfg(unix)]
    {
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(&actual, &alias)?;
        assert_eq!(directory(&alias.join("child"))?, directory(&actual.join("child"))?);
    }
    assert_eq!(directory(&actual.join("missing/../child"))?, actual.join("child"));
    Ok(())
}
