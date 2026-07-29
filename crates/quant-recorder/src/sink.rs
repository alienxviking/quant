//! The non-blocking seam between ingress and the writer.

use core::fmt;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};

use crate::record::CaptureRecord;

/// Default channel depth, in messages.
///
/// The number is chosen from what it has to absorb, not from a round figure. It
/// needs to cover the longest plausible *writer* stall -- a page-cache flush, a
/// disk hiccup, an antivirus scan of the capture directory -- which is seconds,
/// not minutes. At Binance's depth-plus-trades rate for one symbol (order of
/// 100 messages/sec) 4096 is roughly 40 seconds of slack.
///
/// It is deliberately not larger. A very deep channel does not prevent overload,
/// it delays the evidence: sustained overload should surface as a
/// `LocalOverflow` gap within a minute, while someone is still watching, rather
/// than as gigabytes of resident memory and a drop an hour later.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 4096;

/// Why a record could not be handed over.
#[derive(Debug)]
pub enum SinkError {
    /// No capacity. The record comes back so the caller can decide: a venue
    /// message is dropped and accounted for, a gap record is held and retried.
    ///
    /// Returning it rather than swallowing it is what lets [`crate::Ingress`]
    /// treat those two cases differently without cloning payloads.
    Full(CaptureRecord),
    /// The writer is gone. Fatal: continuing would discard capture silently.
    Disconnected,
}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("capture channel is full"),
            Self::Disconnected => f.write_str("capture writer is gone"),
        }
    }
}

impl std::error::Error for SinkError {}

/// Somewhere to put a record that **must never block**.
///
/// A trait rather than a concrete channel so that the overload behaviour in
/// [`crate::Ingress`] can be tested against a sink whose capacity is set to
/// exactly one, with no threads and no timing involved. That is the whole reason
/// the hard part of this crate is deterministic to test.
pub trait RecordSink {
    /// Hand over a record, or give it back.
    ///
    /// Implementations must return promptly in all cases. Blocking here
    /// reintroduces the coupling the read/write split exists to break.
    fn try_send(&mut self, record: CaptureRecord) -> Result<(), SinkError>;
}

impl RecordSink for SyncSender<CaptureRecord> {
    fn try_send(&mut self, record: CaptureRecord) -> Result<(), SinkError> {
        match SyncSender::try_send(self, record) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(record)) => Err(SinkError::Full(record)),
            Err(TrySendError::Disconnected(_)) => Err(SinkError::Disconnected),
        }
    }
}

/// A bounded capture channel.
///
/// Thin wrapper over [`sync_channel`] so callers do not have to remember which
/// of std's channel constructors is the bounded one -- `channel` is unbounded
/// and would silently undo the entire backpressure design.
#[must_use]
pub fn channel(capacity: usize) -> (SyncSender<CaptureRecord>, Receiver<CaptureRecord>) {
    sync_channel(capacity)
}

#[cfg(test)]
pub(crate) mod test_sink {
    use super::{CaptureRecord, RecordSink, SinkError};

    /// A sink with an adjustable capacity and no threads.
    ///
    /// Lets a test say "the channel is full now" and "it has room again now" at
    /// exact points in a message sequence, which is not something a real channel
    /// and a real writer can be asked to do reproducibly.
    #[derive(Debug)]
    pub struct TestSink {
        pub accepted: Vec<CaptureRecord>,
        pub capacity: usize,
        pub disconnected: bool,
    }

    impl TestSink {
        pub fn with_capacity(capacity: usize) -> Self {
            Self {
                accepted: Vec::new(),
                capacity,
                disconnected: false,
            }
        }

        /// Pretend the writer drained everything: room for `capacity` more.
        pub fn drain(&mut self) -> Vec<CaptureRecord> {
            core::mem::take(&mut self.accepted)
        }
    }

    impl RecordSink for TestSink {
        fn try_send(&mut self, record: CaptureRecord) -> Result<(), SinkError> {
            if self.disconnected {
                return Err(SinkError::Disconnected);
            }
            if self.accepted.len() >= self.capacity {
                return Err(SinkError::Full(record));
            }
            self.accepted.push(record);
            Ok(())
        }
    }
}
