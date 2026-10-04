use super::Decoder;

#[test]
fn enforce_transaction_boundaries() -> anyhow::Result<()> {
    let mut decoder = Decoder::default();
    assert!(decoder.commit("0/1".into(), "0/2".into()).is_err());
    decoder.begin(42)?;
    assert!(decoder.begin(43).is_err());
    let tx = decoder.commit("0/1".into(), "0/2".into())?;
    assert_eq!(tx.xid, 42);
    assert!(tx.changes.is_empty());
    decoder.begin(43)?;
    Ok(())
}

fn insert(tag: u8) -> anyhow::Result<postgres_replication::protocol::LogicalReplicationMessage> {
    let mut bytes = vec![b'I'];
    bytes.extend(1_u32.to_be_bytes());
    bytes.push(b'N');
    bytes.extend(1_u16.to_be_bytes());
    bytes.push(tag);
    if matches!(tag, b't' | b'b') {
        bytes.extend(1_i32.to_be_bytes());
        bytes.push(b'1');
    }
    Ok(postgres_replication::protocol::LogicalReplicationMessage::parse(&bytes.into())?)
}

fn with_relation() -> Decoder {
    let mut decoder = Decoder::default();
    decoder.relations.insert(
        1,
        super::Relation {
            schema: "source".into(),
            table: "rows".into(),
            columns: vec!["id".into()],
        },
    );
    decoder
}

#[test]
fn reject_missing_context_and_transaction_limit() -> anyhow::Result<()> {
    let message = insert(b't')?;
    assert!(Decoder::default().message(&message, 10).is_err());
    let mut decoder = with_relation();
    assert!(decoder.message(&message, 10).is_err());
    let mut decoder = with_relation();
    decoder.begin(1)?;
    decoder.message(&message, 1)?;
    assert!(decoder.message(&message, 1).is_err());
    Ok(())
}

#[test]
fn unsupported_tuple_encodings_and_column_mismatch_fail_closed() -> anyhow::Result<()> {
    for tag in [b'u', b'b'] {
        let mut decoder = with_relation();
        decoder.begin(1)?;
        assert!(decoder.message(&insert(tag)?, 10).is_err());
    }
    let mut decoder = with_relation();
    decoder.begin(1)?;
    decoder
        .relations
        .get_mut(&1)
        .ok_or_else(|| anyhow::anyhow!("missing fixture relation"))?
        .columns
        .clear();
    assert!(decoder.message(&insert(b't')?, 10).is_err());
    Ok(())
}

#[test]
fn source_byte_budget_failure_cannot_commit_partial_transaction() -> anyhow::Result<()> {
    let mut decoder = with_relation();
    decoder.limits.output_bytes = 1;
    decoder.begin(1)?;
    assert!(decoder.message(&insert(b't')?, 10).is_err());
    assert!(decoder.commit("0/1".into(), "0/2".into()).is_err());
    assert!(decoder.message(&insert(b't')?, 10).is_err());
    let mut decoder = with_relation();
    decoder.limits.record_bytes = 1;
    decoder.begin(2)?;
    assert!(decoder.message(&insert(b't')?, 10).is_err());
    assert!(decoder.commit("0/3".into(), "0/4".into()).is_err());
    Ok(())
}
