//! Feeding two consumers from one ingress.
//!
//! M1 had exactly one consumer of a capture record: the writer. M5 needs two —
//! the writer, and a live trading engine — and they have to see the *same*
//! record, with the same `local_recv_ts` and the same `ingest_seq`. Not two
//! subscriptions to the venue, and not a tail of the file being written.
//!
//! # Why sameness is the whole point
//!
//! `docs/engine-contract.md` §9 requires a paper run's P&L to match a backtest
//! over the data captured during the same window, exactly rather than within a
//! tolerance. That is only achievable if the two paths consume identical events.
//! Two WebSocket subscriptions would deliver the same messages at different
//! instants and stamp them differently; a file tail would lag by up to a block.
//! A tee at ingress makes them the same bytes with the same stamps, so any
//! divergence downstream is a bug rather than a timing artefact.
//!
//! # Why the capture wins
//!
//! [`TeeSink`] has a primary and a secondary, and the asymmetry is deliberate.
//! Market data is irreplaceable; a paper fill is not. If the engine cannot keep
//! up, its copy is dropped and the capture is untouched — and a failure of the
//! *capture* is still the only failure that stops ingress, exactly as in M1.
//!
//! The engine is not told directly that it missed something, because it does not
//! need to be: a dropped record leaves a hole in `ingest_seq`, and the live
//! source discovers it the same way `quant-verify` discovers a recorder drop and
//! the same way `quant-normalize`'s replay does. One mechanism, three places.

use crate::record::CaptureRecord;
use crate::sink::{RecordSink, SinkError};

/// Sends every record to two sinks, favouring the first.
#[derive(Debug, Clone)]
pub struct TeeSink<P, S> {
    primary: P,
    secondary: S,
    secondary_dropped: u64,
    secondary_gone: bool,
}

impl<P: RecordSink, S: RecordSink> TeeSink<P, S> {
    /// `primary` is the sink whose failure is a real failure.
    pub const fn new(primary: P, secondary: S) -> Self {
        Self {
            primary,
            secondary,
            secondary_dropped: 0,
            secondary_gone: false,
        }
    }

    /// Records the secondary consumer did not get.
    ///
    /// Each one is a hole in the secondary's view of `ingest_seq`, which is what
    /// the live source turns into a gap. Reported so an operator can see the
    /// engine falling behind *before* the strategy stops trading because its
    /// book keeps being invalidated.
    #[must_use]
    pub const fn secondary_dropped(&self) -> u64 {
        self.secondary_dropped
    }

    /// Whether the secondary consumer has hung up for good.
    ///
    /// Not fatal. A paper engine that dies must not take the capture with it —
    /// the capture is the artifact that cannot be recreated, and a run that kept
    /// recording after the strategy crashed is strictly better than one that
    /// stopped both.
    #[must_use]
    pub const fn secondary_gone(&self) -> bool {
        self.secondary_gone
    }

    pub const fn primary(&self) -> &P {
        &self.primary
    }

    pub const fn secondary(&self) -> &S {
        &self.secondary
    }
}

impl<P: RecordSink, S: RecordSink> RecordSink for TeeSink<P, S> {
    fn try_send(&mut self, record: CaptureRecord) -> Result<(), SinkError> {
        // Cloned before the primary send, because a failing primary gives the
        // record back and we must return *that* one -- `Ingress` is entitled to
        // look at what it could not send.
        let copy = record.clone();
        // The primary first, and its failure is the failure. Nothing goes to the
        // secondary in that case: the capture does not have the record, so a
        // hole exists on both sides and the two views stay consistent.
        self.primary.try_send(record)?;

        if self.secondary_gone {
            self.secondary_dropped += 1;
            return Ok(());
        }
        match self.secondary.try_send(copy) {
            Ok(()) => {}
            Err(SinkError::Full(_)) => self.secondary_dropped += 1,
            Err(SinkError::Disconnected) => {
                // Latched, so a dead engine costs one failed send rather than
                // one per message for the rest of a two-week run.
                self.secondary_gone = true;
                self.secondary_dropped += 1;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::test_sink::TestSink;
    use quant_core::time::Ts;

    fn record(seq: u64) -> CaptureRecord {
        CaptureRecord::Venue {
            local_recv_ts: Ts::from_nanos(i64::try_from(seq).expect("small")),
            ingest_seq: seq,
            payload: vec![120_u8; 8],
        }
    }

    #[test]
    fn both_sinks_get_the_identical_record() {
        // The property the whole design exists for: same bytes, same stamp, same
        // sequence number, so a divergence downstream is a bug and not timing.
        let mut tee = TeeSink::new(TestSink::with_capacity(4), TestSink::with_capacity(4));
        for seq in 1..=3 {
            tee.try_send(record(seq)).expect("both have room");
        }
        let primary = tee.primary().accepted.clone();
        let secondary = tee.secondary().accepted.clone();
        assert_eq!(primary, secondary);
        assert_eq!(primary.len(), 3);
        assert_eq!(tee.secondary_dropped(), 0);
    }

    #[test]
    fn a_full_secondary_does_not_harm_the_capture() {
        // Market data is irreplaceable; a paper fill is not.
        let mut tee = TeeSink::new(TestSink::with_capacity(8), TestSink::with_capacity(1));
        for seq in 1..=4 {
            tee.try_send(record(seq))
                .expect("the capture always has room");
        }
        assert_eq!(tee.primary().accepted.len(), 4, "every record captured");
        assert_eq!(tee.secondary().accepted.len(), 1, "the engine fell behind");
        assert_eq!(tee.secondary_dropped(), 3);
    }

    #[test]
    fn a_full_primary_is_a_real_failure_and_the_secondary_gets_nothing() {
        // The capture did not get it, so a hole exists on both sides -- which
        // keeps the two views consistent rather than leaving the engine with a
        // record the archive will never have.
        let mut tee = TeeSink::new(TestSink::with_capacity(1), TestSink::with_capacity(8));
        tee.try_send(record(1)).expect("room for one");
        let err = tee.try_send(record(2)).expect_err("the capture is full");
        assert!(matches!(err, SinkError::Full(_)));
        assert_eq!(tee.secondary().accepted.len(), 1, "not two");
    }

    #[test]
    fn a_failing_primary_hands_the_record_back() {
        let mut tee = TeeSink::new(TestSink::with_capacity(1), TestSink::with_capacity(8));
        tee.try_send(record(1)).expect("room for one");
        match tee.try_send(record(7)).expect_err("full") {
            SinkError::Full(returned) => assert_eq!(returned, record(7)),
            SinkError::Disconnected => panic!("expected Full"),
        }
    }

    /// A sink that has always hung up.
    #[derive(Debug)]
    struct Hungup;

    impl RecordSink for Hungup {
        fn try_send(&mut self, _record: CaptureRecord) -> Result<(), SinkError> {
            Err(SinkError::Disconnected)
        }
    }

    #[test]
    fn a_dead_secondary_is_latched_rather_than_retried_forever() {
        // A paper engine that dies must not take the capture with it, and must
        // not cost a failed send per message for the rest of a two-week run.
        let mut tee = TeeSink::new(TestSink::with_capacity(8), Hungup);
        for seq in 1..=3 {
            tee.try_send(record(seq)).expect("the capture is fine");
        }
        assert!(tee.secondary_gone());
        assert_eq!(tee.secondary_dropped(), 3);
        assert_eq!(tee.primary().accepted.len(), 3, "recording carries on");
    }
}
