use super::{Decoder, Operation, Relation};
use anyhow::Result;
use postgres_replication::protocol::LogicalReplicationMessage;

fn tuple(bytes: &mut Vec<u8>, values: &[Option<&str>]) -> Result<()> {
    bytes.extend(u16::try_from(values.len())?.to_be_bytes());
    for value in values {
        match value {
            None => bytes.push(b'n'),
            Some("@unchanged") => bytes.push(b'u'),
            Some(value) => {
                bytes.push(b't');
                bytes.extend(i32::try_from(value.len())?.to_be_bytes());
                bytes.extend(value.as_bytes());
            }
        }
    }
    Ok(())
}
fn update(old: Option<&[Option<&str>]>, new: &[Option<&str>]) -> Result<LogicalReplicationMessage> {
    let mut bytes = vec![b'U'];
    bytes.extend(1_u32.to_be_bytes());
    if let Some(old) = old {
        bytes.push(b'O');
        tuple(&mut bytes, old)?;
    }
    bytes.push(b'N');
    tuple(&mut bytes, new)?;
    Ok(LogicalReplicationMessage::parse(&bytes.into())?)
}
fn decoder() -> Decoder {
    let mut decoder = Decoder::default();
    decoder.relations.insert(
        1,
        Relation {
            schema: "source".into(),
            table: "rows".into(),
            columns: vec!["id".into(), "text".into(), "other".into()],
        },
    );
    decoder
}
#[test]
fn toast_uses_same_update_old_image_with_changed_key_and_repeated_updates() -> Result<()> {
    let mut decoder = decoder();
    decoder.begin(1)?;
    decoder.message(
        &update(
            Some(&[Some("1"), Some("large α text"), None]),
            &[Some("2"), Some("@unchanged"), Some("new")],
        )?,
        10,
    )?;
    decoder.message(
        &update(
            Some(&[Some("2"), Some("large α text"), Some("new")]),
            &[Some("2"), Some("changed text"), None],
        )?,
        10,
    )?;
    decoder.message(
        &update(
            Some(&[Some("2"), Some("changed text"), None]),
            &[Some("3"), Some("@unchanged"), None],
        )?,
        10,
    )?;
    let transaction = decoder.commit("0/1".into(), "0/2".into())?;
    assert_eq!(transaction.changes.len(), 3);
    assert_eq!(transaction.changes[0].operation, Operation::Update);
    let first = transaction.changes[0].new.as_ref().ok_or_else(|| anyhow::anyhow!("no new row"))?;
    assert_eq!(first["text"].as_deref(), Some("large α text"));
    let last =
        transaction.changes[2].new.as_ref().ok_or_else(|| anyhow::anyhow!("no final row"))?;
    assert_eq!(last["text"].as_deref(), Some("changed text"));
    assert_eq!(last["other"], None);
    assert_eq!(transaction.batch.updates.len(), 2);
    assert_eq!(transaction.batch.updates[0].weight, -1);
    assert_eq!(transaction.batch.updates[1].weight, 1);
    Ok(())
}
#[test]
fn incomplete_or_null_old_toast_images_poison_transaction() -> Result<()> {
    let invalid = [
        update(None, &[Some("1"), Some("@unchanged"), None])?,
        update(Some(&[Some("1"), Some("@unchanged"), None]), &[Some("1"), Some("value"), None])?,
        update(Some(&[Some("1"), None, None]), &[Some("1"), Some("@unchanged"), None])?,
        update(Some(&[Some("1")]), &[Some("1"), Some("@unchanged"), None])?,
    ];
    for message in invalid {
        let mut decoder = decoder();
        decoder.begin(1)?;
        assert!(decoder.message(&message, 10).is_err());
        assert!(decoder.commit("0/1".into(), "0/2".into()).is_err());
    }
    Ok(())
}
#[test]
fn reconstructed_rows_and_transaction_budget_include_inherited_text() -> Result<()> {
    let text = "α".repeat(100);
    let message =
        update(Some(&[Some("1"), Some(&text), None]), &[Some("1"), Some("@unchanged"), None])?;
    let extra = "β".repeat(100);
    let larger = update(
        Some(&[Some("1"), Some(&text), None]),
        &[Some("1"), Some("@unchanged"), Some(&extra)],
    )?;
    let mut decoder = decoder();
    decoder.limits.record_bytes = 300;
    decoder.begin(1)?;
    assert!(decoder.message(&larger, 10).is_err());
    assert!(decoder.commit("0/1".into(), "0/2".into()).is_err());
    let mut decoder = self::decoder();
    decoder.limits.output_bytes = 600;
    decoder.begin(1)?;
    decoder.message(&message, 10)?;
    assert!(decoder.message(&message, 10).is_err());
    assert!(decoder.commit("0/1".into(), "0/2".into()).is_err());
    Ok(())
}
#[test]
fn unchanged_insert_delete_and_key_only_update_are_rejected() -> Result<()> {
    for tag in [b'I', b'D', b'U'] {
        let mut bytes = vec![tag];
        bytes.extend(1_u32.to_be_bytes());
        bytes.push(match tag {
            b'I' => b'N',
            b'D' => b'O',
            _ => b'K',
        });
        tuple(&mut bytes, &[Some("1"), Some("@unchanged"), None])?;
        if tag == b'U' {
            bytes.push(b'N');
            tuple(&mut bytes, &[Some("1"), Some("value"), None])?;
        }
        let message = LogicalReplicationMessage::parse(&bytes.into())?;
        let mut decoder = decoder();
        decoder.begin(1)?;
        assert!(decoder.message(&message, 10).is_err());
        assert!(decoder.commit("0/1".into(), "0/2".into()).is_err());
    }
    Ok(())
}
