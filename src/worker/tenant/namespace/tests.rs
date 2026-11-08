use super::*;
use crate::engine::tests::store::Store;

fn owner(tenant: &str) -> Owner {
    Owner::Catalog {
        version: 1,
        tenant_id: tenant.into(),
        cluster: "cluster".into(),
        database: 1,
        schema: "catalog".into(),
        catalog_id: "immutable-id".into(),
        prefix: "shared/root".into(),
    }
}

#[tokio::test]
async fn same_catalog_claim_is_idempotent_but_foreign_tenant_is_rejected() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let path = Path::from(".pgderive-ownership/test.json");
    let expected = owner("alice");
    let (left, right) = tokio::join!(
        claim_marker(store.clone(), path.clone(), &expected),
        claim_marker(store.clone(), path.clone(), &expected)
    );
    left?;
    right?;
    assert!(claim_marker(store.clone(), path.clone(), &owner("bob")).await.is_err());
    assert_eq!(read(store.as_ref(), &path).await?, Some(expected));
    Ok(())
}

#[tokio::test]
async fn parent_and_descendant_claims_have_exactly_one_winner() -> Result<()> {
    let store = Arc::new(Store::unpaced());
    let path = Path::from(".pgderive-ownership/test.json");
    let catalog = owner("alice");
    let branch = Owner::Branch { version: 1, prefix: "shared/root".into() };
    let (left, right) = tokio::join!(
        claim_marker(store.clone(), path.clone(), &catalog),
        claim_marker(store.clone(), path.clone(), &branch)
    );
    assert_ne!(left.is_ok(), right.is_ok());
    let saved = read(store.as_ref(), &path).await?.context("missing winning claim")?;
    assert!(saved == catalog || saved == branch);
    Ok(())
}
