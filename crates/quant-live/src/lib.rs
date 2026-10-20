//! Trading at a live venue.
//!
//! Two pieces, on purpose (`docs/live-run.md` §5, *`LiveVenue`, decided before
//! the code*). A **merge machine**, [`Merge`]: no socket, no clock, no
//! runtime, only a function from what was heard to what the engine is told,
//! and a readiness answer. Every decision that touches money is in it, so a
//! test can drive it through every interleaving in milliseconds. And a
//! **shell**, `LiveVenue`, which owns the machine, implements
//! `ExecutionVenue`, and moves inputs in and requests out without ever
//! waiting. It is the split `quant-binance::orders` and
//! `quant-binance::user_data` already make, pure logic with the transport
//! elsewhere, one level up.
//!
//! This crate holds the machine. The shell, the request worker that sends
//! orders one at a time, and the timer behind [`Input::Unheard`] are the next
//! pull request; the binary that runs them is item 8.
//!
//! # Why a merge at all
//!
//! Two asynchronous accounts of one order reach this process, and either can
//! arrive first. The REST answer to placing it (`quant-binance::orders`) says
//! whether the venue took the request; the account stream
//! (`quant-binance::account`, `quant-binance::user_data`) says what happened to
//! the order. Neither is complete, either can be lost, and the order they
//! arrive in is a fact about two sockets and nothing about the venue. A live
//! run cannot be debugged at the moment they disagree, so the rules that
//! merge them are written down before the code (`docs/live-run.md` §5, §2 of
//! that subsection) and pinned by a test that runs every interleaving of
//! them (`quant-explain`'s `live_merge` tests).

#[cfg(any(test, feature = "fixtures"))]
pub mod fixtures;
pub mod merge;

pub use merge::{Check, Heard, Input, Merge, MergeStats, Queue, Unsent};
