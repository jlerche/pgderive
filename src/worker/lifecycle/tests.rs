use super::{Base, Generation, Phase, State};

fn generation(query: &str) -> Generation {
    Generation { query: query.into(), slot: format!("slot_{query}"), sink: format!("sink_{query}") }
}
fn state() -> State {
    State {
        version: 1,
        base: Base {
            slot: "slot".into(),
            publication: "publication".into(),
            sink: "sink".into(),
            prefix: "objects".into(),
        },
        active: Some(generation("active")),
        pending: Some(generation("pending")),
        signature: "query semantics".into(),
        phase: Phase::Dropping,
        retired: vec![generation("old"), generation("older")],
        removal: None,
    }
}
#[test]
fn retirement_advances_only_the_completed_generation() {
    let mut state = state();
    state.forget("old");
    assert_eq!(state.retired.len(), 1);
    assert_eq!(state.retired[0].query, "older");
    assert_eq!(state.active.as_ref().map(|generation| generation.query.as_str()), Some("active"));
    assert_eq!(state.pending.as_ref().map(|generation| generation.query.as_str()), Some("pending"));
    state.forget("active");
    assert!(state.active.is_none());
    assert!(state.pending.is_some());
    state.forget("pending");
    assert!(state.pending.is_none());
    assert!(state.phase == Phase::Dropping);
    assert_eq!(state.signature, "query semantics");
}
