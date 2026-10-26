//! The account replay check: does what the account stream said, as the raw
//! tier holds it, reproduce every line the venue produced in a live run's
//! journal?
//!
//! Item 8's acceptance (`docs/live-run.md` §4) and §6 duty 7, read precisely:
//! REST answers are not in the raw tier, so replaying the recorded account
//! frames reproduces only the entries the stream produced, and the REST
//! answers come back off their own journal lines. The merge machine being
//! pure is what makes the two together replayable at all. So, for each
//! session of a live journal:
//!
//! 1. the session's REST answers and submissions are read back off its lines
//!    as the machine's inputs: `submitted` as `Submitted`, `placed`,
//!    `rejected`, `never_landed` and a REST cause's `doubt raised` as the
//!    placement they record, `cancel_refused` as the cancel's reply, and a
//!    `fills_missed` raise as the owner's `Unheard` timer, which a hole's own
//!    raise makes a no-op;
//! 2. the account capture the session wrote is read back through the real
//!    `AccountFeed`, so that a gap and a hole reach the machine as they
//!    reached the live one, up to the session's last instant, its `stopped`
//!    line, after which nothing the stream said was heard;
//! 3. the two are merged by stamp as the live venue merges them, each source
//!    kept in its own order, a REST answer first on a tie and a submission
//!    after the records it was decided on;
//! 4. a fresh machine hears them, and what it says goes to the journal
//!    writer, through the engine's own rule for an order it is not working,
//!    into memory;
//! 5. those lines and the journal's own, each kind the venue produces, are
//!    compared as multisets with every field, stamps included.
//!
//! A line the journal holds that the replay does not make is *missing*: the
//! record claims something the capture does not support. A line the replay
//! makes that the journal does not hold is *extra*: the capture holds
//! something the record did not take. Either is a disagreement.
//!
//! # What it cannot see
//!
//! An acknowledged cancel writes no line, so it is not replayed: a cancel the
//! venue acknowledged for an order REST refused, the one contradiction only it
//! makes, is beyond this check. And the order across the two sources is
//! arrival order only while each poll's records arrived before what the
//! engine did after it; a record still crossing the tee when the engine
//! polled is heard a poll later live and in stamp order here. Across a lost
//! account stream that can move whether an order submitted in that instant
//! was in doubt for it, and this check would say so as a disagreement rather
//! than agree by accident.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use quant_binance::account::{AccountFeed, AccountRecord, Polled};
use quant_binance::orders::{
    CancelReply, FilterName, Placement, Refusal, RefusalCause, VenueError, WhyUnknown,
};
use quant_binance::user_data::{RunTag, Translator};
use quant_core::event::Side;
use quant_core::execution::{ClientOrderId, RejectReason};
use quant_core::fixed::Notional;
use quant_core::instrument::{Exchange, FilterBreach, InstrumentDef, InstrumentId, InstrumentKind};
use quant_core::report::{CancelRefusal, Cited, DoubtCause, DoubtChange, Landing, VenueReport};
use quant_core::time::Ts;
use quant_engine::journal::JournalEntry;
use quant_engine::{Booked, Portfolio, RunObserver};
use quant_live::merge::{Input, Merge};
use quant_recorder::{account_catalog, CaptureRecord, LastDropped};
use quant_storage::{FrameKind, RawReader};

use crate::record::{JournalWriter, Shared};

/// What a replay found.
#[derive(Debug, Default)]
pub struct Replay {
    /// Sessions of the journal under a run tag.
    pub sessions: usize,
    /// Account capture sessions found, and how many were matched to a
    /// journal session.
    pub captures: usize,
    pub matched: usize,
    /// Account frames and blinds the machine heard, up to each session's end.
    pub records: u64,
    /// Lines of the kinds the venue produces, in the journal.
    pub compared: usize,
    /// Lines the journal holds that the replay does not make.
    pub missing: Vec<JournalEntry>,
    /// Lines the replay makes that the journal does not hold.
    pub extra: Vec<JournalEntry>,
    /// Files in the account tree that are not its captures.
    pub strays: usize,
}

/// The check's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Agrees,
    Disagrees,
    /// Nothing was compared, and why.
    NothingToCompare(String),
}

impl Replay {
    #[must_use]
    pub fn verdict(&self) -> Verdict {
        if !self.missing.is_empty() || !self.extra.is_empty() {
            return Verdict::Disagrees;
        }
        if self.records == 0 && self.compared == 0 {
            return Verdict::NothingToCompare(
                "no account record was replayed and the journal holds no line the venue \
                 produced"
                    .to_owned(),
            );
        }
        Verdict::Agrees
    }
}

/// Why a replay could not be made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayError {
    /// Not a live run's journal, or one that names too little to replay:
    /// nothing to compare.
    NotLive(String),
    /// The account capture could not be read: the record is damaged.
    Unreadable(String),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLive(why) => f.write_str(why),
            Self::Unreadable(why) => write!(f, "the account capture could not be read: {why}"),
        }
    }
}

impl std::error::Error for ReplayError {}

/// Replay the account capture under `root` against `entries`, a live run's
/// journal on `symbol` (or the symbol its order lines name).
///
/// # Errors
///
/// [`ReplayError`].
pub fn replay(
    entries: &[JournalEntry],
    root: &Path,
    symbol: Option<&str>,
) -> Result<Replay, ReplayError> {
    let not_live = |why: &str| ReplayError::NotLive(why.to_owned());
    let run = quant_engine::journal::run_tag(entries)
        .ok_or_else(|| not_live("the journal has no run tag: it is not a live run's"))?;
    let run = RunTag::new(run).map_err(|e| ReplayError::NotLive(e.to_string()))?;
    let symbol = symbol
        .map(str::to_owned)
        .or_else(|| symbol_of(entries))
        .ok_or_else(|| not_live("the journal names no symbol: pass --symbol"))?;
    let (base, quote) = assets_of(entries)
        .ok_or_else(|| not_live("the journal has no balances line to name its two assets"))?;
    let definition = InstrumentDef {
        exchange: Exchange::Binance,
        symbol: symbol.clone(),
        base,
        quote,
        kind: InstrumentKind::Spot,
        // The translator reads the symbol and the asset names, and none of
        // the venue's rules.
        tick_size: quant_core::fixed::Px::ZERO,
        lot_size: quant_core::fixed::Qty::ZERO,
        min_notional: Notional::ZERO,
    };

    let mut out = Replay::default();
    let catalog = account_catalog(root);
    if let Some((path, e)) = catalog.unreadable.first() {
        return Err(ReplayError::Unreadable(format!("{}: {e}", path.display())));
    }
    out.strays = catalog.strays.len();
    out.captures = catalog.sessions.len();
    let mut captures = Vec::new();
    for session in &catalog.sessions {
        let mut records = Vec::new();
        for entry in &session.segments {
            records.extend(read_records(&entry.path)?);
        }
        if let Some(first) = records.first() {
            captures.push((first.local_recv_ts(), records));
        }
    }

    let sessions = split(entries);
    for (i, session) in sessions.iter().enumerate() {
        let started_at = session[0].at();
        let next_at = sessions.get(i + 1).map(|next| next[0].at());
        let mut records = Vec::new();
        for (first, held) in &captures {
            if *first >= started_at && next_at.is_none_or(|next| *first < next) {
                out.matched += 1;
                records.extend(held.iter().cloned());
            }
        }
        out.sessions += 1;
        let made = replay_session(session, records, &run, &definition, &mut out.records)?;
        let journalled: Vec<JournalEntry> = session
            .iter()
            .filter(|e| venue_produced(e))
            .cloned()
            .collect();
        out.compared += journalled.len();
        let (missing, extra) = differ(&journalled, &made);
        out.missing.extend(missing);
        out.extra.extend(extra);
    }
    Ok(out)
}

/// The journal's sessions: each from a `started` line to the next.
fn split(entries: &[JournalEntry]) -> Vec<&[JournalEntry]> {
    let starts: Vec<usize> = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| matches!(e, JournalEntry::Started { .. }))
        .map(|(i, _)| i)
        .collect();
    starts
        .iter()
        .enumerate()
        .map(|(n, &start)| {
            let end = starts.get(n + 1).copied().unwrap_or(entries.len());
            &entries[start..end]
        })
        .collect()
}

/// The symbol the journal's first decision names.
fn symbol_of(entries: &[JournalEntry]) -> Option<String> {
    entries.iter().find_map(|e| {
        if let JournalEntry::Submitted { instrument, .. }
        | JournalEntry::Refused { instrument, .. }
        | JournalEntry::Filled { instrument, .. } = e
        {
            Some(instrument.symbol.clone())
        } else {
            None
        }
    })
}

/// The two assets the journal's first `balances` line names.
fn assets_of(entries: &[JournalEntry]) -> Option<(String, String)> {
    entries.iter().find_map(|e| {
        if let JournalEntry::Balances {
            base_asset,
            quote_asset,
            ..
        } = e
        {
            Some((base_asset.clone(), quote_asset.clone()))
        } else {
            None
        }
    })
}

/// One capture file's records, as ingress handed them to the writer.
fn read_records(path: &Path) -> Result<Vec<CaptureRecord>, ReplayError> {
    let unreadable =
        |e: &dyn std::fmt::Display| ReplayError::Unreadable(format!("{}: {e}", path.display()));
    let file = std::fs::File::open(path).map_err(|e| unreadable(&e))?;
    let mut reader = RawReader::open(std::io::BufReader::new(file)).map_err(|e| unreadable(&e))?;
    // A torn tail is a crash's ordinary signature, and everything before it
    // is intact: it is read, as the verifier reads it.
    let (frames, _truncation) = reader.read_all().map_err(|e| unreadable(&e))?;
    let mut records = Vec::with_capacity(frames.len());
    for frame in frames {
        let record = match frame.kind {
            FrameKind::Control => CaptureRecord::Control {
                local_recv_ts: frame.local_recv_ts,
                ingest_seq: frame.ingest_seq,
                record: frame
                    .control()
                    .map_err(|e| unreadable(&e))?
                    .ok_or_else(|| unreadable(&"a control frame that is not one"))?,
            },
            FrameKind::AccountPayload => CaptureRecord::Account {
                local_recv_ts: frame.local_recv_ts,
                ingest_seq: frame.ingest_seq,
                payload: frame.payload,
            },
            FrameKind::VenuePayload | FrameKind::VenueSnapshot => {
                return Err(unreadable(&"a market frame in the account tree"));
            }
        };
        records.push(record);
    }
    Ok(records)
}

/// A machine input read back off a journal line, with when it was heard and
/// how it ties with a record heard at the same instant.
struct FromLine {
    at: Ts,
    /// A REST answer goes before a record of the same stamp, as the live
    /// venue merges them; a submission after, since the engine decided it on
    /// whatever had been heard by then.
    answer: bool,
    input: Input,
    /// For a submission, the order's side, which the fill line names.
    side: Option<Side>,
}

/// The session's lines as the machine's REST-side inputs, in the order the
/// engine wrote them.
#[allow(clippy::too_many_lines, reason = "one arm per line, every line named")]
fn inputs_of(session: &[JournalEntry]) -> Vec<FromLine> {
    let answer = |at: Ts, input: Input| FromLine {
        at,
        answer: true,
        input,
        side: None,
    };
    let mut inputs = Vec::new();
    for entry in session {
        // Exhaustive with no `_` arm, as every reader of the journal is: a
        // line added later does not build here until somebody has decided
        // whether it is a machine's input.
        #[allow(clippy::match_same_arms)]
        let input = match entry {
            JournalEntry::Submitted {
                at,
                client_order_id,
                side,
                qty,
                ..
            } => Some(FromLine {
                at: *at,
                answer: false,
                input: Input::Submitted {
                    id: *client_order_id,
                    qty: *qty,
                },
                side: Some(*side),
            }),
            JournalEntry::Placed {
                at,
                client_order_id,
                venue_order_id,
                transact_ms,
            } => Some(answer(
                *at,
                Input::Placement {
                    id: *client_order_id,
                    placement: Placement::Placed {
                        order_id: venue_order_id.clone(),
                        client_order_id: String::new(),
                        transact_time_ms: *transact_ms,
                    },
                },
            )),
            JournalEntry::Rejected {
                at,
                client_order_id,
                reason,
                cited: Some(cited),
                breach,
            } => refusal(*reason, cited, *breach).map(|refusal| {
                answer(
                    *at,
                    Input::Placement {
                        id: *client_order_id,
                        placement: Placement::Refused(refusal),
                    },
                )
            }),
            // A rejection with nothing cited is a simulator's, never a REST
            // answer: not an input, and left for the comparison to report.
            JournalEntry::Rejected { cited: None, .. } => None,
            JournalEntry::NeverLanded {
                at,
                client_order_id,
                landing,
            } => match landing {
                Landing::NotSent { why } => Some(answer(
                    *at,
                    Input::Placement {
                        id: *client_order_id,
                        placement: Placement::NotSent {
                            why: why.as_str().to_owned(),
                        },
                    },
                )),
                // Slice (d)'s lookup, which nothing runs yet.
                Landing::NotFound { .. } => None,
            },
            JournalEntry::Doubt {
                at,
                client_order_id,
                change,
            } => match change {
                // The owner's timer, which the live worker sets at a
                // placement. A raise a hole made is the same raise, so
                // hearing the timer as well makes no second line.
                DoubtChange::Raised {
                    cause: DoubtCause::FillsMissed,
                    said: _,
                } => Some(answer(
                    *at,
                    Input::Unheard {
                        id: *client_order_id,
                    },
                )),
                DoubtChange::Raised { cause, said } => {
                    why_unknown(*cause, said.as_ref()).map(|why| {
                        answer(
                            *at,
                            Input::Placement {
                                id: *client_order_id,
                                placement: Placement::Unknown { why },
                            },
                        )
                    })
                }
                // Only the stream clears a doubt: the replay's to make.
                DoubtChange::Cleared { .. } => None,
            },
            JournalEntry::CancelRefused {
                at,
                client_order_id,
                why,
            } => cancel_reply(why).map(|reply| {
                answer(
                    *at,
                    Input::CancelReply {
                        id: *client_order_id,
                        reply,
                    },
                )
            }),
            // What the stream produced, which the replay must make rather
            // than be told.
            JournalEntry::Accepted { .. }
            | JournalEntry::Filled { .. }
            | JournalEntry::Cancelled { .. }
            | JournalEntry::Orphaned { .. }
            | JournalEntry::Stray { .. }
            | JournalEntry::Foreign { .. }
            | JournalEntry::Account { .. }
            | JournalEntry::Anomaly { .. } => None,
            // A cancel asked for: the shell's queue, not the machine's. Its
            // answer is the `cancel_refused` line, or the stream's end.
            JournalEntry::CancelRequested { .. } => None,
            // The engine's, the strategy's, the market's and the run's own.
            JournalEntry::Started { .. }
            | JournalEntry::Checkpoint { .. }
            | JournalEntry::Refused { .. }
            | JournalEntry::Tripped { .. }
            | JournalEntry::Blind { .. }
            | JournalEntry::Note { .. }
            | JournalEntry::Stopped { .. }
            | JournalEntry::Sighted { .. }
            | JournalEntry::Balances { .. } => None,
        };
        inputs.extend(input);
    }
    inputs
}

/// The refusal a `rejected` line records, as the order classifier would
/// have made it: one whose cause gives the line's reason and the filter it
/// names. Any cause that does is the same answer to the machine.
fn refusal(reason: RejectReason, cited: &Cited, breach: Option<FilterBreach>) -> Option<Refusal> {
    let cause = match (breach, reason) {
        (Some(FilterBreach::TickSize), _) => RefusalCause::Filter(FilterName::PriceFilter),
        (Some(FilterBreach::LotSize), _) => RefusalCause::Filter(FilterName::LotSize),
        (Some(FilterBreach::MinNotional), _) => RefusalCause::Filter(FilterName::Notional),
        (None, RejectReason::Malformed) => RefusalCause::Malformed,
        (None, RejectReason::InsufficientFunds) => RefusalCause::InsufficientBalance,
        (None, RejectReason::Venue) => RefusalCause::Declined,
        // No REST answer gives these: a line holding one was not a live
        // venue's, and is left for the comparison to report.
        (Some(FilterBreach::NotAnOrder), _)
        | (None, RejectReason::RiskLimit | RejectReason::NoMarket) => return None,
    };
    Some(Refusal {
        http_status: cited.http_status.unwrap_or(400),
        code: cited.code,
        msg: cited.msg.as_str().to_owned(),
        cause,
    })
}

/// The unknown REST answer a `doubt raised` line records, for a cause REST
/// raises: one whose cause and words are the line's.
fn why_unknown(cause: DoubtCause, said: Option<&Cited>) -> Option<WhyUnknown> {
    let status = said.and_then(|s| s.http_status).unwrap_or(0);
    let words = |s: &Cited| (s.code, s.msg.as_str().to_owned());
    Some(match cause {
        DoubtCause::ServerError => WhyUnknown::ServerError {
            http_status: status,
            said: said.map(|s| VenueError {
                code: s.code,
                msg: s.msg.as_str().to_owned(),
            }),
        },
        DoubtCause::Unsettled => {
            let (code, msg) = words(said?);
            WhyUnknown::Unsettled {
                http_status: status,
                code,
                msg,
            }
        }
        DoubtCause::IdInUse => {
            let (code, msg) = words(said?);
            WhyUnknown::IdInUse {
                id: String::new(),
                http_status: status,
                code,
                msg,
            }
        }
        DoubtCause::Unreadable => WhyUnknown::UnreadableBody {
            http_status: status,
            detail: String::new(),
        },
        DoubtCause::OtherOrder => WhyUnknown::OtherOrder {
            sent: String::new(),
            echoed: String::new(),
        },
        DoubtCause::UnexpectedStatus => WhyUnknown::UnexpectedStatus {
            http_status: status,
        },
        DoubtCause::NoAnswer => WhyUnknown::TimedOut,
        DoubtCause::Interrupted => WhyUnknown::Interrupted(String::new()),
        // Raised by the stream or by slice (d), not by a REST answer.
        DoubtCause::AccountBlind | DoubtCause::FillsMissed | DoubtCause::Restart => return None,
    })
}

/// The cancel reply a `cancel_refused` line records.
fn cancel_reply(why: &CancelRefusal) -> Option<CancelReply> {
    Some(match why {
        CancelRefusal::NotOpen(cited) => CancelReply::NotOpen {
            http_status: cited.http_status.unwrap_or(400),
            code: cited.code,
            msg: cited.msg.as_str().to_owned(),
        },
        CancelRefusal::Refused(cited) => {
            CancelReply::Refused(refusal(RejectReason::Venue, cited, None)?)
        }
        CancelRefusal::Unknown { cause, said } => CancelReply::Unknown {
            why: why_unknown(*cause, said.as_ref())?,
        },
        CancelRefusal::NotSent { why } => CancelReply::NotSent {
            why: why.as_str().to_owned(),
        },
    })
}

/// The session's last instant: its `stopped` line's, or for a session that
/// never stopped its last line's that is not stamped by the wall clock.
fn end_of(session: &[JournalEntry]) -> Option<Ts> {
    session
        .iter()
        .rev()
        .find_map(|e| {
            if let JournalEntry::Stopped { at, .. } = e {
                Some(*at)
            } else {
                None
            }
        })
        .or_else(|| {
            session
                .iter()
                .filter(|e| {
                    !matches!(
                        e,
                        JournalEntry::Started { .. } | JournalEntry::Balances { .. }
                    )
                })
                .map(JournalEntry::at)
                .max()
        })
}

/// One session, replayed: the lines its machine makes.
fn replay_session(
    session: &[JournalEntry],
    records: Vec<CaptureRecord>,
    run: &RunTag,
    definition: &InstrumentDef,
    heard: &mut u64,
) -> Result<Vec<JournalEntry>, ReplayError> {
    let translator = Translator::new(run.clone(), definition)
        .map_err(|e| ReplayError::NotLive(e.to_string()))?;
    let instrument = InstrumentId::from_index(0);
    let mut merge = Merge::new(instrument, translator);

    // The capture through the real feed, so that its gaps and holes reach
    // the machine as they reached the live one.
    let end = end_of(session);
    let (tx, rx) = std::sync::mpsc::sync_channel(records.len().max(1));
    for record in records {
        let _ = tx.send(record);
    }
    drop(tx);
    let mut feed = AccountFeed::new(rx, Arc::new(LastDropped::default()));
    let mut heard_records = Vec::new();
    while let Polled::Record(record) = feed.poll() {
        let at = match &record {
            AccountRecord::Frame { local_recv_ts, .. }
            | AccountRecord::Blind { local_recv_ts, .. } => *local_recv_ts,
        };
        if end.is_some_and(|end| at > end) {
            continue;
        }
        heard_records.push((at, record));
    }
    *heard += heard_records.len() as u64;

    let made: Arc<Mutex<Vec<JournalEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let sink: Shared = Arc::clone(&made) as Shared;
    let mut writer = JournalWriter::to(sink, definition.symbol.clone(), 0);
    let mut engine = Mirror::default();

    let mut from_lines = inputs_of(session).into_iter().peekable();
    let mut from_records = heard_records.into_iter().peekable();
    let mut out = Vec::new();
    loop {
        let line_first = match (from_lines.peek(), from_records.peek()) {
            (Some(line), Some((at, _))) => line.at < *at || (line.at == *at && line.answer),
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        out.clear();
        if line_first {
            let Some(line) = from_lines.next() else { break };
            if let (Input::Submitted { id, .. }, Some(side)) = (&line.input, line.side) {
                engine.working.insert(*id, side);
            }
            merge.step(line.at, line.input, &mut out);
        } else {
            let Some((at, record)) = from_records.next() else {
                break;
            };
            match record {
                AccountRecord::Frame { payload, .. } => merge.frame(at, &payload, &mut out),
                AccountRecord::Blind { cause, .. } => {
                    merge.step(at, Input::Blind(cause), &mut out);
                }
            }
        }
        for report in out.drain(..) {
            engine.hear(&report, &mut writer);
        }
    }
    drop(writer);
    let made = made
        .lock()
        .map_or_else(|e| e.into_inner().clone(), |m| m.clone());
    Ok(made)
}

/// The engine's rule for what it hears, and nothing else: an order report
/// about an order it is working goes to the record's hook for it, and one
/// about any other is withheld, a fill as an orphan and the rest as a stray
/// (`docs/engine-contract.md` §11). Booking and the strategy are not the
/// record's, and are not here.
#[derive(Default)]
struct Mirror {
    working: BTreeMap<ClientOrderId, Side>,
    portfolio: Option<Portfolio>,
}

impl Mirror {
    fn hear(&mut self, report: &VenueReport, writer: &mut JournalWriter) {
        let report = match report {
            VenueReport::Notice(notice) => {
                writer.on_notice(notice, notice.ts());
                return;
            }
            VenueReport::Order(report) => report,
        };
        let id = report.client_order_id();
        let Some(&side) = self.working.get(&id) else {
            match report {
                quant_core::report::OrderReport::Filled { fill, ids, ts, .. } => {
                    writer.on_orphaned(id, fill, ids.as_ref(), *ts);
                }
                quant_core::report::OrderReport::Accepted { ts, .. }
                | quant_core::report::OrderReport::Rejected { ts, .. }
                | quant_core::report::OrderReport::Cancelled { ts, .. } => {
                    writer.on_stray(report, *ts);
                }
            }
            return;
        };
        let portfolio = self
            .portfolio
            .get_or_insert_with(|| Portfolio::new(Notional::ZERO));
        match report {
            quant_core::report::OrderReport::Filled {
                fill,
                remaining,
                ids,
                ts,
                ..
            } => writer.on_fill(
                &Booked {
                    client_order_id: id,
                    instrument: InstrumentId::from_index(0),
                    side,
                    fill,
                    remaining: *remaining,
                    ids: ids.as_ref(),
                    at: *ts,
                },
                portfolio,
            ),
            quant_core::report::OrderReport::Accepted {
                venue_order_id, ts, ..
            } => writer.on_accepted(id, venue_order_id.as_ref(), *ts),
            quant_core::report::OrderReport::Rejected {
                reason, why, ts, ..
            } => {
                writer.on_rejected(id, *reason, why.as_ref(), *ts);
            }
            quant_core::report::OrderReport::Cancelled {
                remaining,
                ended,
                ts,
                ..
            } => writer.on_cancelled(id, *remaining, ended.as_ref(), *ts),
        }
        if report.event().is_terminal() {
            self.working.remove(&id);
        }
    }
}

/// Whether a line is one the venue produces: what the replay must make.
///
/// Exhaustive with no `_` arm, so a line added to the journal does not build
/// here until somebody has decided whether a venue produces it.
#[allow(clippy::match_same_arms)]
#[must_use]
pub fn venue_produced(entry: &JournalEntry) -> bool {
    match entry {
        JournalEntry::Accepted { .. }
        | JournalEntry::Rejected { .. }
        | JournalEntry::Filled { .. }
        | JournalEntry::Cancelled { .. }
        | JournalEntry::Orphaned { .. }
        | JournalEntry::Stray { .. }
        | JournalEntry::Placed { .. }
        | JournalEntry::NeverLanded { .. }
        | JournalEntry::Doubt { .. }
        | JournalEntry::CancelRefused { .. }
        | JournalEntry::Foreign { .. }
        | JournalEntry::Account { .. }
        | JournalEntry::Anomaly { .. } => true,
        // The engine's, the strategy's, the market's and the run's own.
        JournalEntry::Started { .. }
        | JournalEntry::Checkpoint { .. }
        | JournalEntry::Submitted { .. }
        | JournalEntry::Refused { .. }
        | JournalEntry::CancelRequested { .. }
        | JournalEntry::Tripped { .. }
        | JournalEntry::Blind { .. }
        | JournalEntry::Note { .. }
        | JournalEntry::Stopped { .. }
        | JournalEntry::Sighted { .. } => false,
        // The venue's claim, but asked for by REST at the start and stop and
        // never heard on the stream.
        JournalEntry::Balances { .. } => false,
    }
}

/// What `want` holds that `got` does not, and the reverse, as multisets of
/// whole lines.
fn differ(want: &[JournalEntry], got: &[JournalEntry]) -> (Vec<JournalEntry>, Vec<JournalEntry>) {
    let key = |e: &JournalEntry| serde_json::to_string(e).unwrap_or_default();
    let mut counts: BTreeMap<String, (i64, JournalEntry)> = BTreeMap::new();
    for e in want {
        counts.entry(key(e)).or_insert((0, e.clone())).0 += 1;
    }
    for e in got.iter().filter(|e| venue_produced(e)) {
        counts.entry(key(e)).or_insert((0, e.clone())).0 -= 1;
    }
    let (mut missing, mut extra) = (Vec::new(), Vec::new());
    for (n, entry) in counts.into_values() {
        for _ in 0..n.max(0) {
            missing.push(entry.clone());
        }
        for _ in 0..(-n).max(0) {
            extra.push(entry.clone());
        }
    }
    (missing, extra)
}

#[cfg(test)]
mod tests;
