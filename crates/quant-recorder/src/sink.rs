//! The non-blocking seam between ingress and the writer.

use core::fmt;
use core::time::Duration;
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;

use crate::metrics::Metrics;
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

/// The sending half of a capture channel, counting what is in flight.
///
/// # Why the counter lives here rather than in `Ingress`
///
/// Because this is where the queue is. `std::sync::mpsc` exposes no length --
/// which is the one thing it does not give us and the reason this wrapper exists
/// at all -- so depth has to be tracked by whoever pushes and pops. Putting it on
/// the channel means every user of the channel is counted, including any future
/// producer that is not `Ingress`.
#[derive(Debug, Clone)]
pub struct CaptureSender {
    inner: SyncSender<CaptureRecord>,
    metrics: Arc<Metrics>,
}

impl RecordSink for CaptureSender {
    fn try_send(&mut self, record: CaptureRecord) -> Result<(), SinkError> {
        // Counted *before* the send, and undone if it fails. The other order is a
        // race: the instant a record is in the channel the writer thread can take
        // it out and decrement, which on an unsigned counter that has not been
        // incremented yet wraps to `usize::MAX`.
        self.metrics.queue_pushed();
        let result = RecordSink::try_send(&mut self.inner, record);
        if result.is_err() {
            self.metrics.queue_push_failed();
        }
        result
    }
}

/// The receiving half, decrementing what the sender counted up.
///
/// Wraps only the one operation the writer loop uses. `recv_timeout` is not
/// incidental: it is why this design uses a std channel at all, because it is what
/// lets a quiet instrument still get its pending block sealed on a timer.
#[derive(Debug)]
pub struct CaptureReceiver {
    inner: Receiver<CaptureRecord>,
    metrics: Arc<Metrics>,
}

impl CaptureReceiver {
    pub fn recv_timeout(&self, timeout: Duration) -> Result<CaptureRecord, RecvTimeoutError> {
        let result = self.inner.recv_timeout(timeout);
        if result.is_ok() {
            self.metrics.queue_popped();
        }
        result
    }
}

/// A bounded capture channel.
///
/// Thin wrapper over [`sync_channel`] so callers do not have to remember which
/// of std's channel constructors is the bounded one -- `channel` is unbounded
/// and would silently undo the entire backpressure design.
///
/// `metrics` carries the queue-depth counter, which both halves need: the
/// producer runs on the async side and the consumer on the writer thread, so this
/// is the one counter that genuinely crosses a thread boundary.
#[must_use]
pub fn channel(capacity: usize, metrics: Arc<Metrics>) -> (CaptureSender, CaptureReceiver) {
    let (tx, rx) = sync_channel(capacity);
    (
        CaptureSender {
            inner: tx,
            metrics: Arc::clone(&metrics),
        },
        CaptureReceiver { inner: rx, metrics },
    )
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
