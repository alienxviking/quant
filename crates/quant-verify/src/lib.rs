//! The offline verifier: what makes M1's acceptance criteria a command.
//!
//! # Why this exists at all
//!
//! `docs/data-contract.md` §7 says the recorder is done when, among other things,
//! *"replaying every recorded file shows `ingest_seq` contiguous within each
//! session, with every discontinuity explained by a recorded `Gap`"*. Until
//! something checks that, it is a sentence in a document. A recorder that believes
//! it is writing complete data and is not looks exactly like one that is -- the
//! whole reason the raw tier records gaps, sequence numbers and a trailer is so
//! that belief can be tested against the bytes.
//!
//! So this is not a debugging aid. It is the thing that decides whether a week of
//! capture may be trusted, and it is meant to run from cron *during* the seven-day
//! acceptance run rather than after it.
//!
//! # The one design idea
//!
//! **Look for a discontinuity, then ask whether the file already explains it.**
//! Never infer an explanation. Every check in [`session`] has that shape, and the
//! explanations are exactly the records the recorder was built to write:
//!
//! - a hole in `ingest_seq` is explained by the gap record immediately after it;
//! - a break in the venue's depth update-id chain is explained by any gap record
//!   between the two deltas;
//! - a delta with no book behind it is explained by a snapshot, or by a
//!   `SnapshotFailed` record saying the recorder tried and could not;
//! - a file with no trailer is explained by being the last one, i.e. still open.
//!
//! An unexplained discontinuity is an error. That asymmetry is the point: it is
//! never the verifier's job to decide that some missing data was probably fine.
//!
//! # Why it is venue-aware, and only just
//!
//! Three of the four checks need nothing but the container format. The update-id
//! chain needs to know that Binance numbers depth messages with `U` and `u` and
//! that contiguity means `U == previous u + 1` -- so that knowledge stays in
//! `quant-binance`, and this crate asks it.
//!
//! There is deliberately **no venue-abstraction trait** yet. One would be designed
//! from a single implementation, which is how a trait ends up encoding one venue's
//! assumptions and calling them universal; extracting it from two real
//! implementations when a second venue arrives produces a better one. In the
//! meantime a capture from a venue this build does not understand is *reported as
//! unchecked* rather than quietly passed, which is the part that actually matters.
//!
//! # Reading a report
//!
//! Errors mean the capture cannot be trusted for what it claims to cover, and fail
//! the run. Warnings are true, worth knowing, and not a reason to distrust the
//! data -- a torn tail on a file still being written is the archetype. Findings are
//! capped per code per session and the remainder counted, because a systematic
//! defect over hundreds of millions of frames would otherwise bury every other
//! finding in the report. See [`finding`].

pub mod discover;
pub mod finding;
pub mod reconcile;
pub mod session;

pub use discover::{discover, Segment, Session};
pub use finding::{code, Finding, Report, Severity, Totals, Where, MAX_PER_CODE};
pub use reconcile::{reconcile, Verified};
pub use session::{verify, SegmentOutcome};
