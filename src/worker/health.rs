//! Diagnostic policy and `PostgreSQL` observations, independent of logical tick time.
pub(super) mod attempts;
mod heartbeat;
use anyhow::{Result, ensure};
pub(super) use heartbeat::{Monitor, Reporter};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::GenericClient;

/// Worker observation intervals and physical WAL alert thresholds.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    /// Seconds between diagnostic heartbeats; never authorizes slot feedback.
    pub heartbeat_secs: u64,
    /// Age after which a heartbeat cannot establish current readiness.
    pub stale_secs: u64,
    /// Maximum publication or maintenance duration before recovery without ACK.
    pub operation_timeout_secs: u64,
    /// Optional retained WAL byte alert; `PostgreSQL` must enforce its own retention cap.
    pub retained_wal_alert_bytes: Option<u64>,
    /// Optional WAL byte distance from the durable source end to current WAL.
    pub backlog_alert_bytes: Option<u64>,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            heartbeat_secs: 5,
            stale_secs: 30,
            operation_timeout_secs: 300,
            retained_wal_alert_bytes: None,
            backlog_alert_bytes: None,
        }
    }
}
impl Policy {
    pub(super) fn validate(&self) -> Result<()> {
        ensure!((1..=60).contains(&self.heartbeat_secs), "invalid health heartbeat interval");
        ensure!(
            (self.heartbeat_secs * 2..=3600).contains(&self.stale_secs),
            "invalid health staleness interval"
        );
        ensure!(
            (1..=3600).contains(&self.operation_timeout_secs),
            "invalid health operation timeout"
        );
        ensure!(
            self.retained_wal_alert_bytes != Some(0) && self.backlog_alert_bytes != Some(0),
            "health WAL alert thresholds must be positive"
        );
        Ok(())
    }
}

pub(super) async fn observe(
    sql: &(impl GenericClient + Sync),
    settings: &crate::worker::Settings,
    registration: Option<&Value>,
    progress: Option<&Value>,
) -> Result<Value> {
    let policy = &settings.health;
    let worker = heartbeat::observed(sql, settings).await?;
    let slot = registration.and_then(|value| value.get("slot_name")).and_then(Value::as_str);
    let end = progress.and_then(|value| value.get("end_lsn")).and_then(Value::as_str);
    let checkpoint =
        progress.and_then(|value| value.get("checkpoint_lsn")).and_then(Value::as_str).or(end);
    let observed: Value = sql.query_one("SELECT jsonb_build_object('observed_at',clock_timestamp(),'current_wal_lsn',pg_current_wal_lsn()::text,'max_slot_wal_keep_size',current_setting('max_slot_wal_keep_size'),'backlog_wal_bytes',CASE WHEN $2::text IS NULL THEN NULL ELSE pg_wal_lsn_diff(pg_current_wal_lsn(),$2::text::pg_lsn)::text END,'slot',(SELECT jsonb_build_object('name',slot_name,'plugin',plugin,'database',database,'temporary',temporary,'active',active,'active_pid',active_pid,'restart_lsn',restart_lsn::text,'confirmed_flush_lsn',confirmed_flush_lsn::text,'wal_status',wal_status,'safe_wal_size',safe_wal_size::text,'invalidation_reason',invalidation_reason,'retained_wal_bytes',pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::text,'confirmed_past_durable',CASE WHEN $3::text IS NULL THEN NULL ELSE confirmed_flush_lsn>$3::text::pg_lsn END) FROM pg_catalog.pg_replication_slots WHERE slot_name=$1))", &[&slot,&end,&checkpoint]).await?.try_get(0)?;
    let mut alerts = Vec::new();
    let physical = observed.get("slot").filter(|slot| !slot.is_null());
    if progress.is_some() && slot.is_some() && physical.is_none() {
        alerts.push("slot_missing");
    }
    if let Some(physical) = physical {
        slot_alerts(physical, registration, &mut alerts);
    }
    if worker.as_ref().is_some_and(|worker| worker.get("recent") == Some(&Value::Bool(false))) {
        alerts.push("heartbeat_stale");
    }
    threshold(
        &observed,
        "backlog_wal_bytes",
        policy.backlog_alert_bytes,
        "wal_backlog",
        &mut alerts,
    )?;
    if let Some(physical) = physical {
        threshold(
            physical,
            "retained_wal_bytes",
            policy.retained_wal_alert_bytes,
            "wal_retention",
            &mut alerts,
        )?;
    }
    if worker.as_ref().is_some_and(|worker| {
        worker.pointer("/diagnostics/phase").and_then(Value::as_str) == Some("failed")
    }) {
        alerts.push("worker_failed");
    }
    if worker.as_ref().is_some_and(|worker| {
        worker.pointer("/diagnostics/failure/retry_budget_exhausted") == Some(&Value::Bool(true))
    }) {
        alerts.push("retry_budget_exhausted");
    }
    let ready = progress.is_some()
        && physical.is_some_and(|slot| slot.get("active") == Some(&Value::Bool(true)))
        && worker.as_ref().is_some_and(|worker| {
            ["recent", "owner_live", "fence_current"]
                .iter()
                .all(|field| worker.get(field) == Some(&Value::Bool(true)))
                && worker.pointer("/diagnostics/phase").and_then(Value::as_str) == Some("ready")
        })
        && !alerts.iter().any(|alert| {
            matches!(
                *alert,
                "slot_missing"
                    | "slot_invalidated"
                    | "slot_incompatible"
                    | "slot_wal_unsafe"
                    | "slot_ahead_of_durable_progress"
            )
        });
    Ok(
        json!({"version":1,"ready":ready,"worker":worker,"observation":observed,"alerts":alerts,"policy":policy}),
    )
}

fn failure_category(error: &anyhow::Error) -> &'static str {
    for cause in error.chain() {
        let text = cause.to_string();
        if text.contains("limit exceeded") || text.contains("budget exceeded") {
            return "resource_limit";
        }
        if text.contains("timed out") {
            return "operation_timeout";
        }
        if text.starts_with("health ") || text.starts_with("worker final diagnostics") {
            return "diagnostics";
        }
    }
    "execution_or_io"
}

fn slot_alerts(slot: &Value, registration: Option<&Value>, alerts: &mut Vec<&'static str>) {
    let database = registration.and_then(|value| value.pointer("/source/identity/database"));
    if slot.get("plugin").and_then(Value::as_str) != Some("pgoutput")
        || slot.get("temporary") != Some(&Value::Bool(false))
        || database.is_none()
        || slot.get("database") != database
    {
        alerts.push("slot_incompatible");
    }
    if !matches!(slot.get("wal_status").and_then(Value::as_str), Some("reserved" | "extended"))
        || slot.get("restart_lsn").is_none_or(Value::is_null)
        || slot.get("confirmed_flush_lsn").is_none_or(Value::is_null)
    {
        alerts.push("slot_wal_unsafe");
    }
    if slot.get("invalidation_reason").is_some_and(|reason| !reason.is_null()) {
        alerts.push("slot_invalidated");
    }
    if slot.get("confirmed_past_durable") == Some(&Value::Bool(true)) {
        alerts.push("slot_ahead_of_durable_progress");
    }
}

fn threshold(
    value: &Value,
    field: &str,
    limit: Option<u64>,
    alert: &'static str,
    alerts: &mut Vec<&'static str>,
) -> Result<()> {
    if let (Some(limit), Some(bytes)) = (limit, value.get(field).and_then(Value::as_str))
        && bytes.parse::<i128>()? > i128::from(limit)
    {
        alerts.push(alert);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Policy, failure_category, slot_alerts};
    use serde_json::json;

    #[test]
    fn slot_health_matches_resume_requirements() {
        let registration = json!({"source":{"identity":{"database":"owned"}}});
        let mut slot = json!({"plugin":"pgoutput","database":"owned","temporary":false,
            "wal_status":"reserved","restart_lsn":"0/1","confirmed_flush_lsn":"0/1",
            "invalidation_reason":null,"confirmed_past_durable":false});
        let mut alerts = Vec::new();
        slot_alerts(&slot, Some(&registration), &mut alerts);
        assert!(alerts.is_empty());
        slot["wal_status"] = json!("extended");
        slot_alerts(&slot, Some(&registration), &mut alerts);
        assert!(alerts.is_empty());
        slot["wal_status"] = json!("unreserved");
        slot_alerts(&slot, Some(&registration), &mut alerts);
        assert_eq!(alerts, vec!["slot_wal_unsafe"]);
        alerts.clear();
        slot["wal_status"] = json!("lost");
        slot["plugin"] = json!("other");
        slot["invalidation_reason"] = json!("wal_removed");
        slot["confirmed_past_durable"] = json!(true);
        slot_alerts(&slot, Some(&registration), &mut alerts);
        assert_eq!(
            alerts,
            vec![
                "slot_incompatible",
                "slot_wal_unsafe",
                "slot_invalidated",
                "slot_ahead_of_durable_progress"
            ]
        );
    }

    #[test]
    fn failure_categories_do_not_expose_error_details() {
        assert_eq!(
            failure_category(&anyhow::anyhow!("transaction change limit exceeded")),
            "resource_limit"
        );
        assert_eq!(
            failure_category(&anyhow::anyhow!("worker publication timed out")),
            "operation_timeout"
        );
        assert_eq!(failure_category(&anyhow::anyhow!("health task failed")), "diagnostics");
        assert_eq!(
            failure_category(&anyhow::anyhow!("missing /release-health/scratch directory")),
            "execution_or_io"
        );
    }

    #[test]
    fn policy_rejects_invalid_intervals_and_thresholds() {
        assert!(Policy::default().validate().is_ok());
        let mut policy = Policy { heartbeat_secs: 0, ..Policy::default() };
        assert!(policy.validate().is_err());
        policy.heartbeat_secs = 5;
        policy.stale_secs = 9;
        assert!(policy.validate().is_err());
        policy.stale_secs = 30;
        policy.backlog_alert_bytes = Some(0);
        assert!(policy.validate().is_err());
    }
}
