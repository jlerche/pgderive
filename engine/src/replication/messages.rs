use anyhow::{Result, bail};
use pgwire_replication::{
    Lsn, ReplicationEvent,
    protocol::{
        framing::BackendMessage,
        messages::parse_error_response,
        replication::{ReplicationCopyData, parse_copy_data},
    },
};
use postgres_replication::protocol::LogicalReplicationMessage;
pub(super) fn check_error(message: &BackendMessage) -> Result<()> {
    if message.tag == b'E' {
        bail!(
            "{}",
            pgwire_replication::PgWireError::Server(parse_error_response(&message.payload))
        );
    }
    Ok(())
}
pub(super) fn event(message: BackendMessage) -> Result<(ReplicationEvent, bool)> {
    match parse_copy_data(message.payload)? {
        ReplicationCopyData::KeepAlive { wal_end, server_time_micros, reply_requested } => Ok((
            ReplicationEvent::KeepAlive { wal_end, server_time_micros, reply_requested },
            reply_requested,
        )),
        ReplicationCopyData::XLogData { wal_start, wal_end, server_time_micros, data } => {
            let event = match data.first() {
                Some(b'B' | b'C' | b'M') => boundary(LogicalReplicationMessage::parse(&data)?)?,
                _ => ReplicationEvent::XLogData { wal_start, wal_end, server_time_micros, data },
            };
            Ok((event, false))
        }
    }
}
fn boundary(message: LogicalReplicationMessage) -> Result<ReplicationEvent> {
    Ok(match message {
        LogicalReplicationMessage::Begin(body) => ReplicationEvent::Begin {
            final_lsn: Lsn::from_u64(body.final_lsn()),
            xid: body.xid(),
            commit_time_micros: body.timestamp(),
        },
        LogicalReplicationMessage::Commit(body) => ReplicationEvent::Commit {
            lsn: Lsn::from_u64(body.commit_lsn()),
            end_lsn: Lsn::from_u64(body.end_lsn()),
            commit_time_micros: body.timestamp(),
        },
        LogicalReplicationMessage::Message(body) => ReplicationEvent::Message {
            transactional: body.flags() & 1 != 0,
            lsn: Lsn::from_u64(body.message_lsn()),
            prefix: body.prefix()?.to_owned(),
            content: body.content().clone(),
        },
        _ => bail!("unexpected replication boundary"),
    })
}
