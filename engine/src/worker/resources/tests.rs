use super::*;

#[test]
fn process_limits_reject_zero_inconsistent_and_unknown_settings() -> Result<()> {
    Limits::default().validate()?;
    assert!(Limits { max_queries: 0, ..Limits::default() }.validate().is_err());
    assert!(Limits { cache_bytes_per_tenant: usize::MAX, ..Limits::default() }.validate().is_err());
    assert!(serde_json::from_value::<Limits>(serde_json::json!({"unknown":1})).is_err());
    Ok(())
}

#[test]
fn tenant_pressure_does_not_notify_other_tenants() -> Result<()> {
    let cache = Pool::new(16, 8)?;
    let memtable = Pool::new(16, 8)?;
    let alice = watch::channel(0).0;
    let bob = watch::channel(0).0;
    let mut alice_watch = alice.subscribe();
    let mut bob_watch = bob.subscribe();
    let resources = Resources {
        query: Query { cache: cache.tenant("alice"), memtable: memtable.tenant("alice") },
        process: Arc::new(Process {
            cache,
            memtable,
            pressure: [("alice".into(), alice.clone()), ("bob".into(), bob)].into(),
            recovery: Arc::new(Semaphore::new(1)),
        }),
        encoded: None,
        pressure: alice,
    };
    resources.pressure(true);
    assert!(alice_watch.has_changed()?);
    assert!(!bob_watch.has_changed()?);
    alice_watch.borrow_and_update();
    resources.pressure(false);
    assert!(alice_watch.has_changed()?);
    assert!(bob_watch.has_changed()?);
    bob_watch.borrow_and_update();
    Ok(())
}
