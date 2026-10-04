use super::{Column, Contract, Identity, Relation};
use anyhow::{Result, bail};
use postgres_replication::protocol::LogicalReplicationMessage;

fn contract() -> Contract {
    Contract {
        identity: Identity { system: "123".into(), timeline: 1, database: "test".into() },
        publication: "pgderive_pub".into(),
        slot: "pgderive_slot".into(),
        relations: vec![Relation {
            oid: 42,
            schema: "public".into(),
            table: "items".into(),
            columns: vec![Column {
                position: 1,
                name: "id".into(),
                oid: 20,
                modifier: -1,
                nullable: false,
                primary: true,
                collation: 0,
            }],
        }],
    }
}
fn announcement(values: (u32, u8, &str, u32, i32, u8)) -> Result<LogicalReplicationMessage> {
    let (oid, identity, name, type_id, modifier, flags) = values;
    let mut wire = vec![b'R'];
    wire.extend(oid.to_be_bytes());
    wire.extend(b"public\0items\0");
    wire.push(identity);
    wire.extend(1_u16.to_be_bytes());
    wire.push(flags);
    wire.extend(name.as_bytes());
    wire.push(0);
    wire.extend(type_id.to_be_bytes());
    wire.extend(modifier.to_be_bytes());
    Ok(LogicalReplicationMessage::parse(&wire.into())?)
}
#[test]
fn frozen_codec_rejects_wire_drift() -> Result<()> {
    let contract = contract();
    let LogicalReplicationMessage::Relation(valid) = announcement((42, b'f', "id", 20, -1, 1))?
    else {
        bail!("invalid test message")
    };
    contract.check_relation(&valid)?;
    for (oid, identity, name, type_id, modifier, flags) in [
        (43, b'f', "id", 20, -1, 1),
        (42, b'd', "id", 20, -1, 1),
        (42, b'f', "renamed", 20, -1, 1),
        (42, b'f', "id", 23, -1, 1),
        (42, b'f', "id", 20, 12, 1),
        (42, b'f', "id", 20, -1, 0),
    ] {
        let LogicalReplicationMessage::Relation(message) =
            announcement((oid, identity, name, type_id, modifier, flags))?
        else {
            bail!("invalid test message")
        };
        assert!(contract.check_relation(&message).is_err());
    }
    let mut renamed = contract.clone();
    renamed.relations[0].schema = "other".into();
    assert!(renamed.check_relation(&valid).is_err());
    let mut longer = contract;
    let column = longer.relations[0].columns[0].clone();
    longer.relations[0].columns.push(column);
    assert!(longer.check_relation(&valid).is_err());
    Ok(())
}
#[test]
fn binding_digest_covers_native_metadata() -> Result<()> {
    let contract = contract();
    let digest = contract.digest()?;
    let mut drift = contract.clone();
    drift.relations[0].columns[0].primary = false;
    assert_ne!(digest, drift.digest()?);
    drift = contract.clone();
    drift.identity.timeline += 1;
    assert_ne!(digest, drift.digest()?);
    let restored: Contract = serde_json::from_slice(&serde_json::to_vec(&contract)?)?;
    assert_eq!(digest, restored.digest()?);
    Ok(())
}
