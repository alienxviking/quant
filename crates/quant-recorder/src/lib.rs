//! Recorder plumbing: ingress stamping, sequencing, and overload policy.
//!
//! # The shape, and why it is two halves
//!
//! ```text
//!   socket                    bounded channel                    disk
//!  ┌────────────────────┐    ┌───────────────┐    ┌──────────────────────┐
//!  │ read task          │    │               │    │ writer thread        │
//!  │  stamp recv_ts     │───▶│  capacity N   │───▶│  frame + zstd + IO   │
//!  │  assign ingest_seq │    │               │    │  flush on a timer    │
//!  │  never blocks      │    └───────────────┘    └──────────────────────┘
//!  └────────────────────┘        [`Ingress`]              [`run_writer`]
//! ```
//!
//! The tempting design is one loop that reads the socket and writes the file.
//! It is wrong, and not subtly: framing and zstd-compressing a block is
//! *blocking* work of unbounded duration -- a slow disk, a page-cache flush, an
//! antivirus scan -- and any time spent in it is time the socket is not being
//! drained. Stop draining a TCP socket and the receive window closes; keep it
//! closed and Binance disconnects us for being a slow consumer. So the venue
//! punishes us for our disk being busy, which is an absurd coupling to accept.
//!
//! Splitting them means the read side does the minimum possible -- stamp, count,
//! enqueue -- and can always keep up.
//!
//! # Why the channel is bounded
//!
//! An unbounded channel does not remove the overload, it converts a bounded,
//! visible, recordable problem into an unbounded invisible one: memory grows
//! until the OOM killer arrives, typically an hour later, taking the whole
//! capture with it. And nothing in the data would have said why.
//!
//! Bounded, the same overload produces a message we *drop on purpose*, a
//! [`GapCause::LocalOverflow`] record saying so, and a hole in `ingest_seq`
//! proving exactly how many. That is a capacity problem we can see and size
//! against.
//!
//! ## What a full channel does *not* do
//!
//! It does not block the read task. Blocking on send would reintroduce the exact
//! coupling the split exists to break -- slow disk stalls the socket -- only now
//! with the added dishonesty that the data would show no gap at all, because we
//! would have "successfully" recorded everything, just late, with `local_recv_ts`
//! values reflecting our own stall rather than the venue's timing.
//!
//! Dropping is the honest choice. Recording the drop is what makes it honest.
//!
//! # Why holes in `ingest_seq` carry the count
//!
//! A sequence number is assigned to every message that arrives, whether or not
//! it survives the channel. So a drop leaves a hole, and the hole's width *is*
//! the number of messages lost -- which is why the gap record does not carry a
//! count field and does not need one. One record marks the event; the sequence
//! arithmetic supplies the magnitude.
//!
//! That is also why consecutive drops coalesce into a single pending gap record
//! rather than one record per lost message: under real overload that would be
//! thousands of records competing for the very channel space we do not have.
//!
//! # Why gap records are never dropped
//!
//! Venue messages are droppable because we can describe their absence. A gap
//! record is the description, so dropping it would lose the only evidence that
//! anything happened.
//!
//! When the channel is full there is nowhere to put one, so it is held in
//! [`Ingress`] and re-offered on every subsequent opportunity -- see
//! [`Ingress::pump`]. This terminates in practice for a reason worth stating:
//! the situations that generate gap records are situations in which the inflow
//! has stopped. A disconnect means no more messages are arriving, so the writer
//! drains and room appears within milliseconds. An overload gap flushes as soon
//! as the burst subsides. The reconnect path is already sleeping on backoff,
//! which is exactly where `pump` belongs.
//!
//! # Why this crate has no async runtime
//!
//! Nothing here needs one. [`Ingress`] is a state machine over
//! "message arrived" and "gap occurred"; [`run_writer`] is a blocking consumer,
//! because framing and file writes genuinely are blocking and wrapping them in
//! `async` would buy nothing but a thread-pool hop.
//!
//! [`std::sync::mpsc::sync_channel`] is used rather than an async channel
//! because it provides precisely the two operations the design calls for:
//! non-blocking [`try_send`] on the producer, and [`recv_timeout`] on the
//! consumer so a quiet instrument still gets its block sealed on a timer. A
//! tokio channel would force either an async writer -- pointless for blocking
//! I/O -- or `blocking_recv`, which has no timeout and so no timed flush.
//!
//! The venue adapter on the other side of [`Ingress`] *is* async, and that seam
//! is deliberate: async where there is waiting on the network to overlap,
//! threads where there is CPU and blocking I/O to get out of the way.
//!
//! [`GapCause::LocalOverflow`]: quant_core::GapCause::LocalOverflow
//! [`try_send`]: std::sync::mpsc::SyncSender::try_send
//! [`recv_timeout`]: std::sync::mpsc::Receiver::recv_timeout

pub mod ingress;
pub mod record;
pub mod sink;
pub mod writer;

pub use ingress::{Accepted, GapOutcome, Ingress, IngressStats, MAX_PENDING_GAPS};
pub use record::CaptureRecord;
pub use sink::{channel, RecordSink, SinkError, DEFAULT_CHANNEL_CAPACITY};
pub use writer::{run_writer, WriterOutcome, DEFAULT_FLUSH_INTERVAL};
