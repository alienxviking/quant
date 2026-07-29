//! The task that applies metadata events, and never lets them matter too much.

use tokio::sync::mpsc::Receiver;
use uuid::Uuid;

use crate::rows::{SegmentRow, SessionClose, SessionRow};
use crate::store::Meta;

/// Depth of the metadata channel.
///
/// Small on purpose. Events are rare -- one per session boundary and one per sealed
/// segment, so a handful a day per instrument -- and a deep queue would only serve
/// to hide a database that has stopped responding. If 256 events back up, the
/// interesting fact is that the database is broken, not that we buffered more.
pub const DEFAULT_METADATA_CAPACITY: usize = 256;

/// Something worth recording about a capture session.
#[derive(Debug, Clone)]
pub enum MetaEvent {
    SessionOpened(SessionRow),
    SegmentSealed(SegmentRow),
    SessionClosed { id: Uuid, close: SessionClose },
}

impl MetaEvent {
    /// For log lines, so a failure says which kind of event was lost.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::SessionOpened(_) => "session_opened",
            Self::SegmentSealed(_) => "segment_sealed",
            Self::SessionClosed { .. } => "session_closed",
        }
    }
}

/// What the metadata task managed to do.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetaStats {
    pub applied: u64,
    /// Events the database rejected or could not receive. Non-zero means the index
    /// is incomplete and needs rebuilding from the raw tier.
    pub failed: u64,
}

/// Apply events until the channel closes.
///
/// Returns [`MetaStats`] rather than a `Result`, and that is the whole design:
/// **nothing in here can fail the recorder.** A database that is down, a network
/// partition, a constraint violation from a bug in this crate -- all of them get
/// logged, counted, and stepped over. Market data is irreplaceable and an index row
/// is not, so the index is allowed to be wrong and the capture is not allowed to
/// stop.
///
/// The cost is that `failed > 0` means the tables no longer describe the data
/// directory. That is recoverable precisely because everything here is derivable
/// from the capture files themselves -- see the crate docs.
pub async fn run_metadata(mut rx: Receiver<MetaEvent>, meta: Meta) -> MetaStats {
    let mut stats = MetaStats::default();

    while let Some(event) = rx.recv().await {
        let result = match &event {
            MetaEvent::SessionOpened(row) => meta.open_session(row).await,
            MetaEvent::SegmentSealed(row) => meta.record_segment(row).await,
            MetaEvent::SessionClosed { id, close } => meta.close_session(*id, close).await,
        };

        match result {
            Ok(()) => stats.applied += 1,
            Err(e) => {
                stats.failed += 1;
                tracing::error!(
                    kind = event.kind(),
                    error = %e,
                    "metadata write failed; capture is unaffected"
                );
            }
        }
    }

    if stats.failed > 0 {
        tracing::error!(
            applied = stats.applied,
            failed = stats.failed,
            "metadata index is incomplete and should be rebuilt from raw capture"
        );
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::instrument::Exchange;
    use quant_core::time::Ts;

    #[test]
    fn event_kinds_are_distinct_so_a_failure_says_what_was_lost() {
        let opened = MetaEvent::SessionOpened(
            SessionRow::new([0; 16], Exchange::Binance, "BTCUSDT", Ts::from_secs(0)).unwrap(),
        );
        let closed = MetaEvent::SessionClosed {
            id: uuid::Uuid::nil(),
            close: SessionClose {
                ended_at: crate::rows::to_offset(Ts::from_secs(1)).unwrap(),
                status: crate::rows::SessionStatus::Closed,
                messages: 0,
                venue_bytes: 0,
                dropped: 0,
                gaps_recorded: 0,
                gaps_abandoned: 0,
                backdated: 0,
                note: None,
            },
        };
        assert_ne!(opened.kind(), closed.kind());
        assert_eq!(opened.kind(), "session_opened");
    }

    #[tokio::test]
    async fn the_channel_is_bounded_so_a_stalled_database_cannot_grow_without_limit() {
        // try_send from the writer thread must fail rather than block or allocate,
        // because blocking there would stall block sealing and drop market data.
        let (tx, _rx) = tokio::sync::mpsc::channel::<MetaEvent>(1);
        let row = SessionRow::new([1; 16], Exchange::Binance, "BTCUSDT", Ts::from_secs(0)).unwrap();
        assert!(tx.try_send(MetaEvent::SessionOpened(row.clone())).is_ok());
        assert!(
            tx.try_send(MetaEvent::SessionOpened(row)).is_err(),
            "a full metadata channel must reject, never block"
        );
    }
}
