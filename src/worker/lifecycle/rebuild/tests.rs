use super::semantic_description;
use anyhow::Result;
use serde_json::json;

#[test]
fn advisory_placement_does_not_change_pending_rebuild_identity() -> Result<()> {
    let original = json!({"query_id":"q","plan_identity":"p","columns":["x"]});
    let mut described = original.clone();
    described["query_id"] = json!("other");
    described["placement"] = json!({"revision":99,"report_only":true});
    assert_eq!(semantic_description(original.clone())?, semantic_description(described)?);
    let mut changed = original.clone();
    changed["columns"] = json!(["y"]);
    assert_ne!(semantic_description(original)?, semantic_description(changed)?);
    assert!(semantic_description(json!([])).is_err());
    Ok(())
}
