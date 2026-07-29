//! What travels from the read task to the writer thread.

use quant_core::time::Ts;
use quant_storage::ControlRecord;

/// One item in flight between ingress and the writer.
///
/// Carries the two things ingress exists to stamp -- `local_recv_ts` and
/// `ingest_seq` -- so that neither can be recomputed downstream. In particular
/// the writer never asks the clock what time it is: doing so would replace "when
/// we saw the bytes" with "when we got around to writing them", which is a
/// latency measurement of our own disk masquerading as a measurement of the
/// venue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureRecord {
    /// Venue bytes, verbatim and uninterpreted.
    Venue {
        local_recv_ts: Ts,
        ingest_seq: u64,
        payload: Vec<u8>,
    },
    /// A record we authored, such as a gap.
    Control {
        local_recv_ts: Ts,
        ingest_seq: u64,
        record: ControlRecord,
    },
}

impl CaptureRecord {
    #[must_use]
    pub const fn ingest_seq(&self) -> u64 {
        match self {
            Self::Venue { ingest_seq, .. } | Self::Control { ingest_seq, .. } => *ingest_seq,
        }
    }

    #[must_use]
    pub const fn local_recv_ts(&self) -> Ts {
        match self {
            Self::Venue { local_recv_ts, .. } | Self::Control { local_recv_ts, .. } => {
                *local_recv_ts
            }
        }
    }

    #[must_use]
    pub const fn is_control(&self) -> bool {
        matches!(self, Self::Control { .. })
    }
}
