//! The metadata tier: what we captured, when, and how it went.
//!
//! # The doctrine this crate is built around
//!
//! `docs/data-contract.md` makes the raw tier the source of truth and Postgres
//! "metadata only". Taken seriously, that has a consequence sharper than it first
//! sounds:
//!
//! **A database outage must not stop the recorder, and must not lose data.**
//!
//! Market data is the one thing in this platform that cannot be regenerated. A
//! metadata row can be. So every write in this crate is best-effort: failures are
//! logged and counted, never propagated into the capture path. A recorder started
//! with no database configured is a fully functional recorder that simply has no
//! index of itself.
//!
//! The corresponding obligation is that everything stored here must be
//! **reconstructible from the raw tier**. Each capture file carries its own
//! header (exchange, symbol, session) and trailer (frames, blocks, last sequence),
//! so the M1.d verifier can rebuild these tables by walking the data directory.
//! Anything we stored that could *not* be rebuilt would quietly make Postgres the
//! source of truth for it -- which is precisely the inversion the tiering exists
//! to prevent. The only exceptions are operational annotations: why a session
//! ended, and what the ingress counters saw.
//!
//! # Why not inline on the writer thread
//!
//! The obvious implementation writes a row from [`SegmentStore::sealed`], on the
//! writer thread. It is wrong for the same reason the recorder does not write
//! files from the socket task: a hung database connection would stall block
//! sealing, which backs the capture channel up, which drops market data. A
//! secondary concern must not be able to damage the primary one.
//!
//! So sealed segments are reported over a bounded [`tokio::sync::mpsc`] channel
//! (non-blocking [`try_send`] from the writer thread, `recv().await` in a task)
//! and a full channel drops the *metadata*, loudly. Losing an index row is
//! recoverable; losing a tick is not.
//!
//! # Layering
//!
//! ```text
//!   quant-meta        knows about the recorder and the database
//!       │
//!       ▼
//!   quant-recorder    knows about storage; nothing about databases
//!       │
//!       ▼
//!   quant-storage     knows about bytes
//! ```
//!
//! The arrows only point down. `quant-recorder` reports sealed segments through a
//! plain observer callback ([`ObservedStore`]), so it never learns that Postgres
//! exists.
//!
//! [`SegmentStore::sealed`]: quant_recorder::SegmentStore::sealed
//! [`ObservedStore`]: quant_recorder::ObservedStore
//! [`try_send`]: tokio::sync::mpsc::Sender::try_send

pub mod rows;
pub mod schema;
pub mod store;
pub mod task;

pub use rows::{SegmentRow, SessionClose, SessionRow, SessionStatus};
pub use schema::{migrate, Migration, MIGRATIONS};
pub use store::{connect, Meta, MetaError};
pub use task::{run_metadata, MetaEvent, MetaStats, DEFAULT_METADATA_CAPACITY};
