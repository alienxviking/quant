//! The request worker: one REST request at a time, first in first out.
//!
//! `docs/live-run.md` §5, *`LiveVenue`, decided before the code*, §1, is the
//! design; this is its async half. The worker owns the [`TradeClient`], reads
//! requests off the `orders` queue the venue alone sends on, sends each through
//! [`TradeClient::place_order`] or [`TradeClient::cancel_order`], and puts what
//! came back on the `inbox`, stamped with when it arrived. It also keeps the
//! owner's decision 6 timer: [`ORDER_TIMEOUT`] after each `Placed` it says the
//! order went unheard, and the machine decides whether that means anything.
//!
//! # Why one at a time
//!
//! A cancel sent beside the order it names can reach the venue first, come
//! back `-2011` *"Unknown order sent."*, and leave the order resting with
//! nobody cancelling it. One request in flight, in the order the engine asked,
//! makes "a cancel never overtakes its order" a property of the loop rather
//! than of timing. The cost is head-of-line blocking behind a request that
//! times out, at most [`ORDER_TIMEOUT`], which shows as `QueueFull` and never
//! as silence.
//!
//! # What it never does
//!
//! - **Drop an answer.** The inbox is bounded and the worker awaits room in
//!   it: a full inbox makes it wait for the engine's next poll, never lose an
//!   answer. A lost `Unknown` would be an order nobody doubts.
//! - **Panic on a channel.** A failed send means the venue is gone, and the
//!   worker stops sending anything more and returns. Under `panic = "abort"`
//!   an `expect` there would take the process down before the account capture
//!   was sealed, losing the one record of what the venue said about the run's
//!   last orders.
//! - **Send after `stop_sending`.** A tokio receiver still yields what is
//!   buffered after every sender is gone, so the venue sets a stop flag first,
//!   and every request read after it is answered `NotSent { "stopping" }`
//!   without leaving this process.
//!
//! # Stamps
//!
//! An order goes out with the stamp the venue gave it at `submit`: the
//! engine's `now`, less the measured offset, so that the venue refuses an
//! order that waited too long. A cancel is stamped here, as it is sent, from
//! the injected clock less the same offset: wanting an order gone does not go
//! stale. Every answer is stamped from the same clock when its request's future
//! resolves, which is the clock that stamps `local_recv_ts` (§4).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use quant_binance::orders::{CancelOrder, CancelReply, NewOrder, Placement};
use quant_binance::trade::{TradeClient, ORDER_TIMEOUT};
use quant_core::execution::ClientOrderId;
use quant_core::time::{Clock, Ts};
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Why a request that was queued never left: the venue had stopped sending.
pub(crate) const STOPPING: &str = "stopping";

/// One request, as the venue queued it.
#[derive(Debug)]
pub(crate) enum Request {
    /// An order, stamped for the venue when the strategy submitted it.
    Order {
        id: ClientOrderId,
        order: NewOrder,
        stamped_at_ms: i64,
    },
    /// A cancel, stamped by the worker when it sends it.
    Cancel {
        id: ClientOrderId,
        cancel: CancelOrder,
    },
}

impl Request {
    /// The order it is about.
    pub(crate) const fn id(&self) -> ClientOrderId {
        match self {
            Self::Order { id, .. } | Self::Cancel { id, .. } => *id,
        }
    }

    /// The answer to a request read after the venue stopped sending.
    fn stopping(self, at: Ts) -> Answer {
        match self {
            Self::Order { id, .. } => Answer::Placement {
                id,
                placement: Placement::NotSent {
                    why: STOPPING.to_owned(),
                },
                at,
            },
            Self::Cancel { id, .. } => Answer::CancelReply {
                id,
                reply: CancelReply::NotSent {
                    why: STOPPING.to_owned(),
                },
                at,
            },
        }
    }
}

/// What the worker puts on the inbox, stamped with when it was heard.
#[derive(Debug)]
pub(crate) enum Answer {
    /// The answer to an order's request.
    Placement {
        id: ClientOrderId,
        placement: Placement,
        at: Ts,
    },
    /// The answer to a cancel's request.
    CancelReply {
        id: ClientOrderId,
        reply: CancelReply,
        at: Ts,
    },
    /// [`ORDER_TIMEOUT`] since the order's `Placed`: decision 6's timer. Not
    /// an answer to a request, so it does not count against `outstanding`.
    Unheard { id: ClientOrderId, at: Ts },
}

impl Answer {
    pub(crate) const fn at(&self) -> Ts {
        match self {
            Self::Placement { at, .. }
            | Self::CancelReply { at, .. }
            | Self::Unheard { at, .. } => *at,
        }
    }
}

/// What the worker has done, readable from any thread while it runs.
///
/// Atomic for M1.e's reason, not for contention: the worker is the only
/// writer, and the counts must be readable while it is borrowed by its
/// runtime for the length of the run.
#[derive(Debug, Default)]
pub struct WorkerStats {
    sent: AtomicU64,
    answered: AtomicU64,
    stopped: AtomicU64,
    unheard: AtomicU64,
}

impl WorkerStats {
    /// Requests sent to the venue.
    #[must_use]
    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    /// Answers put on the inbox, a stopped request's included.
    #[must_use]
    pub fn answered(&self) -> u64 {
        self.answered.load(Ordering::Relaxed)
    }

    /// Requests answered `NotSent { "stopping" }` without being sent.
    #[must_use]
    pub fn stopped(&self) -> u64 {
        self.stopped.load(Ordering::Relaxed)
    }

    /// Decision 6's `Unheard` inputs put on the inbox.
    #[must_use]
    pub fn unheard(&self) -> u64 {
        self.unheard.load(Ordering::Relaxed)
    }
}

/// Why [`Worker::run`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerEnd {
    /// The venue dropped the queue, and every request in it was answered:
    /// the ordinary end, step 6 of the shutdown.
    QueueClosed,
    /// The venue's inbox is gone, so nobody could hear another answer.
    VenueGone,
}

/// The request worker. See the module docs.
///
/// Made by [`crate::wire`] beside the venue it answers; the binary spawns
/// [`Worker::run`] on its runtime, and nothing else drives it.
#[derive(Debug)]
pub struct Worker {
    pub(crate) client: TradeClient,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) offset_ms: i64,
    pub(crate) orders: mpsc::Receiver<Request>,
    pub(crate) inbox: mpsc::Sender<Answer>,
    pub(crate) stop: Arc<AtomicBool>,
    pub(crate) stats: Arc<WorkerStats>,
    pub(crate) unheard_after: Duration,
}

impl Worker {
    /// This worker, with decision 6's timer firing `after` each `Placed`
    /// instead of [`ORDER_TIMEOUT`]: for a test that needs it to fire, and
    /// only there. Behind `fixtures`, which nothing that ships turns on.
    #[cfg(any(test, feature = "fixtures"))]
    #[must_use]
    pub const fn unheard_after(mut self, after: Duration) -> Self {
        self.unheard_after = after;
        self
    }

    /// What the worker has done, shared with whoever holds this.
    #[must_use]
    pub fn stats(&self) -> Arc<WorkerStats> {
        Arc::clone(&self.stats)
    }

    /// Send every request the venue queues, one at a time and in order, and
    /// answer each on the inbox, until the venue drops the queue or its inbox.
    pub async fn run(mut self) -> WorkerEnd {
        // Every timer is `unheard_after` from when it was set, so they come
        // due in the order they were set, and a queue holds them in order.
        let mut timers: VecDeque<(Instant, ClientOrderId)> = VecDeque::new();
        loop {
            // Idle: the next request, or the next timer, whichever comes.
            let request = loop {
                tokio::select! {
                    biased;
                    () = due(timers.front().map(|&(at, _)| at)) => {
                        if !self.unheard(&mut timers).await {
                            return WorkerEnd::VenueGone;
                        }
                    }
                    request = self.orders.recv() => match request {
                        Some(request) => break request,
                        None => return WorkerEnd::QueueClosed,
                    },
                }
            };

            // Read after the request, so that one queued before the venue
            // stopped and read after is never sent.
            let answer = if self.stop.load(Ordering::SeqCst) {
                self.stats.stopped.fetch_add(1, Ordering::Relaxed);
                request.stopping(self.clock.now())
            } else {
                self.stats.sent.fetch_add(1, Ordering::Relaxed);
                let sending = send(&self.client, self.clock.as_ref(), self.offset_ms, request);
                tokio::pin!(sending);
                // In flight: the answer, and any timer that comes due first,
                // which goes out on the inbox ahead of it.
                loop {
                    tokio::select! {
                        biased;
                        () = due(timers.front().map(|&(at, _)| at)) => {
                            if !self.unheard(&mut timers).await {
                                return WorkerEnd::VenueGone;
                            }
                        }
                        answer = &mut sending => break answer,
                    }
                }
            };

            let placed = match &answer {
                Answer::Placement {
                    id,
                    placement: Placement::Placed { .. },
                    ..
                } => Some(*id),
                Answer::Placement { .. } | Answer::CancelReply { .. } | Answer::Unheard { .. } => {
                    None
                }
            };
            // Awaited, never dropped: a full inbox waits for the engine's next
            // poll. A closed one means nobody is left to hear it.
            if self.inbox.send(answer).await.is_err() {
                return WorkerEnd::VenueGone;
            }
            self.stats.answered.fetch_add(1, Ordering::Relaxed);
            if let Some(id) = placed {
                timers.push_back((Instant::now() + self.unheard_after, id));
            }
        }
    }

    /// The timer at the front came due: say so on the inbox. False when the
    /// venue is gone.
    async fn unheard(&self, timers: &mut VecDeque<(Instant, ClientOrderId)>) -> bool {
        let Some((_, id)) = timers.pop_front() else {
            return true;
        };
        let answer = Answer::Unheard {
            id,
            at: self.clock.now(),
        };
        if self.inbox.send(answer).await.is_err() {
            return false;
        }
        self.stats.unheard.fetch_add(1, Ordering::Relaxed);
        true
    }
}

/// When `at` comes, or never when there is no timer.
async fn due(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Send one request and stamp its answer when it came back.
async fn send(client: &TradeClient, clock: &dyn Clock, offset_ms: i64, request: Request) -> Answer {
    match request {
        Request::Order {
            id,
            order,
            stamped_at_ms,
        } => {
            let placement = client.place_order(&order, stamped_at_ms).await;
            Answer::Placement {
                id,
                placement,
                at: clock.now(),
            }
        }
        Request::Cancel { id, cancel } => {
            // Stamped at sending, from the injected clock, on the venue's.
            let stamped_at_ms = clock.now().as_millis().saturating_sub(offset_ms);
            let reply = client.cancel_order(&cancel, stamped_at_ms).await;
            Answer::CancelReply {
                id,
                reply,
                at: clock.now(),
            }
        }
    }
}

/// The timer's length in production: decision 6.
pub(crate) const UNHEARD_AFTER: Duration = ORDER_TIMEOUT;
