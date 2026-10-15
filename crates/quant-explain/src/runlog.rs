//! Reading a run log, and refusing to be reassuring about it.
//!
//! Two checks, and they answer different questions. [`check`] asks *is this
//! record complete* — does it account for every decision the engine made.
//! [`diff`] asks *do two runs agree* — did the live session and a replay over
//! the same data decide the same things. Neither implies the other: a complete
//! record of a run that diverged is still complete, and two files can agree
//! about everything they contain while both omit the same line.
//!
//! The same separation `quant-verify` and `quant-normalize` keep, for the same
//! reason: a tool that conflated them would have to call one of the two a
//! failure.
//!
//! # Why a hole in the id sequence is evidence
//!
//! `Ledger::mint` hands out `ClientOrderId`s densely from 1, and every mint is
//! followed by exactly one of three outcomes — a refusal at the seam, a refusal
//! at the risk layer, or a submission. All three are written down. So the ids
//! appearing in `submitted` and `refused` lines must be exactly `1..=n`, and a
//! **hole is a decision that happened and was not recorded**.
//!
//! That is the whole reason the id is minted before the risk check rather than
//! after: the omission leaves evidence. A record that could only be checked
//! against something outside itself would be a record you have to trust.
//!
//! Its honest limit, stated rather than discovered later: entries that consume
//! no id — `accepted`, `cancelled`, `blind`, `note` — are **not** covered.
//! Losing one of those leaves no hole. They are covered only by the weaker
//! lifecycle and strategy claims, and the strategy claim is weaker still
//! because both sides of its fold are written by the same strategy.
//!
//! # What a venue said that the record cannot account for
//!
//! From schema 3 a live run writes down what its venue says beside its own
//! decisions, and four kinds of line are things the run's record does not
//! account for, each a finding:
//!
//! - an `orphaned` fill, which no order of this run explains: money moved,
//!   and neither the portfolio nor its checkpoints saw it;
//! - a `stray` report about an order the engine was not working;
//! - a `foreign` order from another run, or under our prefix and readable as
//!   nothing we send, one finding per order however many lines it has;
//! - a `doubt` raised and never cleared, about an order never seen to end.
//!
//! And a line torn by an interrupted write that a restart then wrote past is
//! a finding, read by [`check_file`]: an entry was being written there and is
//! lost, and nothing else in the file can say what it was. `reconcile`
//! agrees across one, because the restarted engine resumed from the same
//! short file, and the hole check need not see it either: a lost submission
//! with the highest id leaves no hole, because the restart mints that id
//! again.
//!
//! A `foreign` order that is not ours at all, a person's or another
//! program's, is **counted rather than flagged**: sharing an account is a
//! fact about the account, not a gap in this run's record, and a check that
//! failed every run on an account somebody else also trades would teach
//! everyone to ignore it. Criterion A accounts for those trades against the
//! venue's balances, which is where they belong.
//!
//! Every finding fails the check, as every finding before them did. The
//! question is whether the record is complete, and each of these is a place
//! where it is not: what was decided, what happened to it, or what it cost.

use std::collections::BTreeMap;

use quant_core::execution::VenueOrderId;
use quant_core::report::{DoubtCause, DoubtChange, ForeignOutcome, Provenance, VenueText};
use quant_core::time::Ts;
use quant_engine::journal::{JournalEntry, Recovered, StrayKind};

/// The first journal schema whose sessions write `sighted` lines.
const SIGHTED_SINCE: u32 = 3;

/// What a run log failed to account for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    /// A decision was made and no line was written for it.
    MissingDecision { client_order_id: u64 },
    /// The notes between two claims do not add up to the claim's own delta.
    ClaimDoesNotFold {
        kind: String,
        claimed: i64,
        counted: i64,
    },
    /// A claim went backwards, which no running total may do.
    ClaimWentBackwards { kind: String, from: u64, to: u64 },
    /// Two decisions claim the same id.
    ///
    /// A journal written before `minting_from` was wired: a restarted session
    /// reminted from 1, so the file holds two different orders under one id. The
    /// hole check cannot see it — nothing is *missing* — which is precisely why
    /// it needs saying separately. A checker that assumed its own fix had always
    /// been in place would read such a file as COMPLETE.
    DuplicateDecision { client_order_id: u64, times: usize },
    /// A fill for an order this engine was not working: an `orphaned` line.
    UnattributedFill { client_order_id: u64, at: Ts },
    /// A report other than a fill about an order the engine was not working.
    Stray {
        client_order_id: u64,
        kind: StrayKind,
        at: Ts,
    },
    /// An order another run sent, or one under our prefix that parses as
    /// nothing we send. One finding per order, however many lines it has.
    ForeignOrder {
        client_id: String,
        venue_order_id: String,
        /// The other run's tag, or `None` when the id would not parse.
        run: Option<String>,
        lines: usize,
        trades: usize,
    },
    /// A doubt raised about an order and never cleared, about an order never
    /// seen to end.
    LeftInDoubt {
        client_order_id: u64,
        since: Ts,
        cause: DoubtCause,
    },
    /// A line an interrupted write tore and a restart then wrote past, so
    /// the entry on it is lost. Numbered from 1, as an editor numbers them.
    TornBeforeRestart { line: usize },
}

impl Finding {
    /// The word an operator reads first on the finding's line.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::MissingDecision { .. }
            | Self::ClaimDoesNotFold { .. }
            | Self::ClaimWentBackwards { .. }
            | Self::DuplicateDecision { .. } => "missing",
            Self::UnattributedFill { .. } => "orphan",
            Self::Stray { .. } => "stray",
            Self::ForeignOrder { .. } => "foreign",
            Self::LeftInDoubt { .. } => "in doubt",
            Self::TornBeforeRestart { .. } => "torn",
        }
    }
}

impl core::fmt::Display for Finding {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnattributedFill {
                client_order_id,
                at,
            } => write!(
                f,
                "a fill at {} for order {client_order_id}, which this run was not working, \
                 so no decision of its explains it and its portfolio never booked it",
                at.to_rfc3339()
            ),
            Self::Stray {
                client_order_id,
                kind,
                at,
            } => write!(
                f,
                "a {kind:?} report at {} about order {client_order_id}, which this run was \
                 not working",
                at.to_rfc3339()
            ),
            Self::ForeignOrder {
                client_id,
                venue_order_id,
                run,
                lines,
                trades,
            } => {
                match run {
                    Some(run) => write!(f, "run {run}'s order")?,
                    None => write!(
                        f,
                        "an order under our prefix that parses as nothing we send,"
                    )?,
                }
                write!(
                    f,
                    " {client_id:?} (venue order {venue_order_id}): {lines} line(s), \
                     {trades} trade(s) on this run's account"
                )
            }
            Self::TornBeforeRestart { line } => write!(
                f,
                "line {line} was torn by an interrupted write and a restart wrote past \
                 it, so the entry on it is lost and nothing here can say what it was"
            ),
            Self::LeftInDoubt {
                client_order_id,
                since,
                cause,
            } => write!(
                f,
                "order {client_order_id} in doubt since {} ({cause:?}), never cleared and \
                 never seen to end",
                since.to_rfc3339()
            ),
            Self::MissingDecision { client_order_id } => write!(
                f,
                "client_order_id {client_order_id} was minted and never written down"
            ),
            Self::ClaimDoesNotFold {
                kind,
                claimed,
                counted,
            } => write!(
                f,
                "the strategy claimed {claimed} more '{kind}' notes between two claims; \
                 the file holds {counted}"
            ),
            Self::ClaimWentBackwards { kind, from, to } => {
                write!(f, "a running total for '{kind}' fell from {from} to {to}")
            }
            Self::DuplicateDecision {
                client_order_id,
                times,
            } => write!(
                f,
                "client_order_id {client_order_id} names {times} different decisions, \
                 so the ids in this file are ambiguous rather than dense"
            ),
        }
    }
}

/// Everything `check` can say about one file.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Report {
    pub entries: usize,
    /// Decisions seen: one per `submitted` or `refused` line.
    pub decisions: usize,
    pub claims: usize,
    /// Orders on the account that this system did not place at all: counted,
    /// not flagged. See the module docs.
    pub not_ours: usize,
    pub findings: Vec<Finding>,
}

impl Report {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.findings.is_empty()
    }
}

/// [`check`], and the lines `read` found torn before a restart.
///
/// What a caller holding the file rather than its entries runs, which is
/// every caller but a test: a torn line is a fact about the file, and the
/// entries alone cannot say where one was.
#[must_use]
pub fn check_file(recovered: &Recovered) -> Report {
    let mut report = check(&recovered.entries);
    report.findings.extend(
        recovered
            .torn_before_restart
            .iter()
            .map(|&line| Finding::TornBeforeRestart { line }),
    );
    report
}

/// Does this file account for every decision it implies?
#[must_use]
pub fn check(entries: &[JournalEntry]) -> Report {
    let mut report = Report {
        entries: entries.len(),
        ..Report::default()
    };

    // P1: the ids must be dense from 1. Collected from the two outcomes a mint
    // can have -- a submission or a refusal. `accepted`, `filled` and the rest
    // reuse an id a `submitted` line already claimed, so counting them would
    // make a hole look filled by its own consequences.
    let mut seen = Vec::new();
    for entry in entries {
        match entry {
            JournalEntry::Submitted {
                client_order_id, ..
            }
            | JournalEntry::Refused {
                client_order_id, ..
            } => seen.push(client_order_id.0),
            _ => {}
        }
    }
    report.decisions = seen.len();
    seen.sort_unstable();
    // Before the dedup, because the dedup is what would hide this.
    let mut run = 1_usize;
    for i in 1..seen.len() {
        if seen[i] == seen[i - 1] {
            run += 1;
        } else {
            if run > 1 {
                report.findings.push(Finding::DuplicateDecision {
                    client_order_id: seen[i - 1],
                    times: run,
                });
            }
            run = 1;
        }
    }
    if run > 1 {
        if let Some(&last) = seen.last() {
            report.findings.push(Finding::DuplicateDecision {
                client_order_id: last,
                times: run,
            });
        }
    }
    seen.dedup();
    if let Some(&highest) = seen.last() {
        let present: std::collections::BTreeSet<u64> = seen.iter().copied().collect();
        for id in 1..=highest {
            if !present.contains(&id) {
                report.findings.push(Finding::MissingDecision {
                    client_order_id: id,
                });
            }
        }
    }

    report
        .findings
        .extend(fold_claims(entries, &mut report.claims));
    report
        .findings
        .extend(venue_findings(entries, &mut report.not_ours));
    report
}

/// Orders this run did not send, one entry per order however many lines.
#[derive(Default)]
struct Foreigners {
    /// Another run's, or unparseable: findings, in the order first heard of.
    orders: Vec<Finding>,
    /// Not placed by this system at all: counted, by client and venue id.
    strangers: Vec<(String, String)>,
}

impl Foreigners {
    fn hear(
        &mut self,
        client_id: &VenueText,
        provenance: &Provenance,
        venue_order_id: &VenueOrderId,
        what: &ForeignOutcome,
    ) {
        let traded = usize::from(matches!(what, ForeignOutcome::Traded { .. }));
        let run = match provenance {
            Provenance::OtherRun(tag) => Some(tag.as_str().to_owned()),
            Provenance::Unparseable => None,
            // A person's order, or another program's: the account is shared,
            // which is not a gap in this run's record.
            Provenance::NotOurs => {
                let key = (client_id.as_str().to_owned(), venue_order_id.0.clone());
                if !self.strangers.contains(&key) {
                    self.strangers.push(key);
                }
                return;
            }
        };
        let same = self.orders.iter_mut().find(|f| {
            matches!(f, Finding::ForeignOrder { client_id: c, venue_order_id: v, .. }
                if c == client_id.as_str() && *v == venue_order_id.0)
        });
        if let Some(Finding::ForeignOrder { lines, trades, .. }) = same {
            *lines += 1;
            *trades += traded;
        } else {
            self.orders.push(Finding::ForeignOrder {
                client_id: client_id.as_str().to_owned(),
                venue_order_id: venue_order_id.0.clone(),
                run,
                lines: 1,
                trades: traded,
            });
        }
    }
}

/// What the venue said that this run's record cannot account for.
///
/// Exhaustive over every entry, with no `_` arm, so a line added to the
/// journal does not build here until somebody has said whether it is one.
fn venue_findings(entries: &[JournalEntry], not_ours: &mut usize) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut foreign = Foreigners::default();
    // Orders in doubt now, and since when.
    let mut doubts: BTreeMap<u64, (Ts, DoubtCause)> = BTreeMap::new();

    // Several arms do the same thing for different reasons, and are kept
    // apart so each reason stays beside the lines it is about.
    for entry in entries {
        #[allow(clippy::match_same_arms)]
        match entry {
            JournalEntry::Orphaned {
                at,
                client_order_id,
                ..
            } => findings.push(Finding::UnattributedFill {
                client_order_id: client_order_id.0,
                at: *at,
            }),
            JournalEntry::Stray {
                at,
                client_order_id,
                kind,
                ..
            } => findings.push(Finding::Stray {
                client_order_id: client_order_id.0,
                kind: *kind,
                at: *at,
            }),
            JournalEntry::Foreign {
                client_id,
                provenance,
                venue_order_id,
                what,
                ..
            } => foreign.hear(client_id, provenance, venue_order_id, what),
            JournalEntry::Doubt {
                at,
                client_order_id,
                change,
            } => match change {
                DoubtChange::Raised { cause, .. } => {
                    // The first raise is when the doubt began; a second
                    // before any clearing is the same doubt, still open.
                    doubts.entry(client_order_id.0).or_insert((*at, *cause));
                }
                DoubtChange::Cleared { .. } => {
                    doubts.remove(&client_order_id.0);
                }
            },
            // An order that ended is no longer in doubt: whatever was unknown
            // about it, its end is known. A fill ends it only when nothing is
            // left, and a fill that does not say what is left ends nothing.
            JournalEntry::Filled {
                client_order_id,
                remaining,
                ..
            } => {
                if remaining.is_some_and(|r| r.raw() == 0) {
                    doubts.remove(&client_order_id.0);
                }
            }
            JournalEntry::Rejected {
                client_order_id, ..
            }
            | JournalEntry::Cancelled {
                client_order_id, ..
            }
            | JournalEntry::NeverLanded {
                client_order_id, ..
            } => {
                doubts.remove(&client_order_id.0);
            }
            // Not an end: an acceptance, a cancel asked for or refused, a
            // placement acknowledged, say nothing about how an order ends.
            JournalEntry::Accepted { .. }
            | JournalEntry::CancelRequested { .. }
            | JournalEntry::CancelRefused { .. }
            | JournalEntry::Placed { .. } => {}
            // Unreadable or contradictory reports: the venue raises a doubt
            // beside each, and that doubt is what is checked.
            JournalEntry::Anomaly { .. } => {}
            // The account stream lost or regained: the venue raises a doubt
            // on every working order when it is lost, and those are checked.
            JournalEntry::Account { .. } => {}
            // The record's own lines, and the market's: nothing a venue said.
            // A restart does not settle a doubt, so `started` leaves them.
            JournalEntry::Started { .. }
            | JournalEntry::Checkpoint { .. }
            | JournalEntry::Submitted { .. }
            | JournalEntry::Refused { .. }
            | JournalEntry::Tripped { .. }
            | JournalEntry::Blind { .. }
            | JournalEntry::Note { .. }
            | JournalEntry::Stopped { .. }
            | JournalEntry::Sighted { .. }
            | JournalEntry::Balances { .. } => {}
        }
    }
    *not_ours = foreign.strangers.len();
    findings.extend(foreign.orders);
    findings.extend(doubts.into_iter().map(|(client_order_id, (since, cause))| {
        Finding::LeftInDoubt {
            client_order_id,
            since,
            cause,
        }
    }));
    findings
}

/// The strategy's own column, folded against its own claims.
///
/// Generic over the strategy: a claim states a `tally` keyed by note kind, and
/// the notes carry that same `kind`. Nothing here knows what a crossing is.
fn fold_claims(entries: &[JournalEntry], claims: &mut usize) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut previous: Option<BTreeMap<String, u64>> = None;
    let mut counted: BTreeMap<String, i64> = BTreeMap::new();

    for entry in entries {
        let JournalEntry::Note { kind, detail, .. } = entry else {
            continue;
        };
        if kind != "claim" {
            *counted.entry(kind.clone()).or_insert(0) += 1;
            continue;
        }
        *claims += 1;
        let tally = tally_of(detail);
        if let Some(before) = previous {
            // Every kind either side has an opinion about, so a kind that
            // appears in one and not the other is still compared.
            let kinds: std::collections::BTreeSet<&String> =
                before.keys().chain(tally.keys()).collect();
            for kind in kinds {
                let was = before.get(kind).copied().unwrap_or(0);
                let now = tally.get(kind).copied().unwrap_or(0);
                if now < was {
                    findings.push(Finding::ClaimWentBackwards {
                        kind: kind.clone(),
                        from: was,
                        to: now,
                    });
                    continue;
                }
                let claimed = i64::try_from(now - was).unwrap_or(i64::MAX);
                let saw = counted.get(kind).copied().unwrap_or(0);
                if claimed != saw {
                    findings.push(Finding::ClaimDoesNotFold {
                        kind: kind.clone(),
                        claimed,
                        counted: saw,
                    });
                }
            }
        }
        previous = Some(tally);
        counted.clear();
    }
    findings
}

fn tally_of(detail: &serde_json::Value) -> BTreeMap<String, u64> {
    detail
        .get("tally")
        .and_then(serde_json::Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(k, v)| v.as_u64().map(|n| (k.clone(), n)))
                .collect()
        })
        .unwrap_or_default()
}

/// Why two run logs could not be compared, or where they first parted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Divergence {
    /// They agree, decision for decision.
    Agree { decisions: usize },
    /// The first place they differ, with both sides.
    Differs {
        at_decision: usize,
        live: String,
        replay: String,
    },
    /// One side ran out first.
    LengthDiffers { live: usize, replay: usize },
    /// There was nothing to compare. **Not** agreement.
    NothingToCompare,
    /// The live side restarted, which forfeits an exact match before any
    /// comparison is attempted.
    LiveRestarted { started: usize },
}

/// Did two runs decide the same things?
///
/// Compares the **decision** entries only — submissions and refusals, in order.
/// Fills are consequences; a venue that filled at a different instant is a
/// different question, and one `reconcile` already asks. The claim here is
/// narrower and stronger: given the same market, the strategy and the risk
/// layer reached the same conclusions in the same order.
///
/// # Why it refuses rather than passing
///
/// Two failure modes return non-zero instead of agreement, and both are the
/// same mistake in different clothes: **"nobody disagreed" is not "two answers
/// matched"**. That is `reconcile`'s exit 2, which M5.c arrived at after
/// shipping a check that could not fail.
///
/// An empty comparison is the obvious one. A restart is the subtle one, and it
/// is a *state* problem rather than a reader problem: `JournalEntry` has no
/// strategy entry and `Engine::resuming` replaces only the portfolio, so a
/// restarted session comes back with the right cash and empty `MaCrossover`
/// windows, a zeroed sampler and a zeroed daily risk tally. The replay runs all
/// of that continuously across the same boundary. Different crossings, different
/// decisions, and a diff that would fail for a reason unrelated to the question
/// being asked. Detected from the file — two `started` entries — and refused up
/// front rather than reported as a divergence at some arbitrary line.
#[must_use]
pub fn diff(live: &[JournalEntry], replay: &[JournalEntry]) -> Divergence {
    let started = live
        .iter()
        .filter(|e| matches!(e, JournalEntry::Started { .. }))
        .count();
    if started > 1 {
        return Divergence::LiveRestarted { started };
    }

    let left = decisions(live);
    let right = decisions(replay);
    if left.is_empty() && right.is_empty() {
        return Divergence::NothingToCompare;
    }
    for (i, (a, b)) in left.iter().zip(right.iter()).enumerate() {
        if a != b {
            return Divergence::Differs {
                at_decision: i + 1,
                live: a.clone(),
                replay: b.clone(),
            };
        }
    }
    if left.len() != right.len() {
        return Divergence::LengthDiffers {
            live: left.len(),
            replay: right.len(),
        };
    }
    Divergence::Agree {
        decisions: left.len(),
    }
}

/// One line per decision, in the form the diff compares.
///
/// **`at` is part of the comparison, and leaving it out made the check
/// vacuous.** The first version of this excluded it, reasoning that a live
/// session and a replay stamp the same decision identically only while the tee
/// held, and that a reader should not assume what M5's criterion proves.
///
/// That is backwards twice over. The ids are dense from 1 and a long/flat
/// crossover alternates buy and sell at a fixed size, so without the timestamp
/// every decision line reads `submitted #n Buy 0.001` — two runs that decided
/// completely different things at completely different moments produce
/// character-identical sequences. Run against the real fortnight it compared
/// 786 decisions from two genuinely different strategies and found every one of
/// them equal, reporting only that one side had more. A check that cannot tell
/// those apart is not measuring anything.
///
/// And the assumption it was avoiding is the thing worth testing. The tee gives
/// both paths identical `local_recv_ts`; if a live decision and its replay
/// disagree about when they happened, that *is* the divergence this tool exists
/// to find. Excluding the field discarded the only evidence.
fn decisions(entries: &[JournalEntry]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|entry| match entry {
            JournalEntry::Submitted {
                at,
                client_order_id,
                side,
                qty,
                ..
            } => Some(format!(
                "{} submitted #{} {side:?} {qty}",
                at.as_nanos(),
                client_order_id.0
            )),
            JournalEntry::Refused {
                at,
                client_order_id,
                reason,
                bound,
                by,
                ..
            } => Some(format!(
                "{} refused #{} {reason:?} {bound:?} by {by:?}",
                at.as_nanos(),
                client_order_id.0
            )),
            _ => None,
        })
        .collect()
}

/// What the engine and the strategy decided around an instant.
///
/// The reader that drew this milestone's boundary becoming the first consumer of
/// the thing that crosses it. M7 established that a decision is not recoverable
/// from raw by any reader; now that decisions are written down, `explain --at`
/// is where they get read.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Decisions {
    /// Decision lines inside the window, newest last.
    pub within: Vec<String>,
    /// The most recent blindness at or before the instant, and whether the
    /// stream had been seen again since.
    pub blind: Option<(i64, String, bool)>,
    /// Strategy notes inside the window, counted by kind.
    pub notes: BTreeMap<String, usize>,
}

/// Read the decisions in `(at - span, at]`.
///
/// # Why blindness is looked for *outside* the window
///
/// Against a simulated venue the engine's clock is the event stream, so an
/// outage produces no entries at all while it lasts — the `blind` line sits at
/// the moment the stream stopped, which for M5's twelve-hour outage is eleven
/// hours before the hour someone asks about. A window query finds nothing and
/// an operator reads nothing as "quiet". The answer to *why did it not trade
/// between 03:00 and 04:00* lives before 03:00, so the last blindness at or
/// before the instant is carried forward and the search for the recovery that
/// ends it runs to the instant.
///
/// Same shape as `market_at`'s day rule: ask the narrow question first, widen
/// only when the narrow answer is the uninformative one.
///
/// # What "seen since" rests on
///
/// **From schema 3 it is read**: the engine writes a `sighted` line when a
/// book goes from meaning nothing to meaning something, and only that ends a
/// blindness. Since M8.a the engine wakes on a quiet market to hear the venue,
/// and a live venue's fill delivered during an outage is journalled at its own
/// later time, with whatever the strategy decides on hearing of it, all after
/// the `blind` line while the market is still dark. Inferred, that outage
/// would read as over.
///
/// **Before schema 3 it is inferred**, because no such line exists: an entry
/// the engine wrote after the `blind` line is taken to mean the market came
/// back. That holds while venue reports cannot arrive without a market event,
/// which is true of `SimulatedVenue`, which reports only from inside
/// `observe`, and so of every journal written before schema 3: backtest and
/// paper. The fortnight's journals are read exactly as they always were.
///
/// Which rule applies is the schema of the session the entry belongs to, read
/// off the `started` line that began it; entries before any `started` line
/// are read the old way.
///
/// # Filtered by stamp, not cut at the first later one
///
/// An entry stamped after the instant is skipped, and reading carries on.
/// This stopped at the first such entry, which is the same thing only in a
/// file whose stamps never fall, and `backtest --journal` writes one whose
/// first line is stamped on the wall clock -- the day the backtest ran, later
/// than every event it replayed. So the loop stopped at line one, and
/// `explain --at` over any backtest journal read nothing at all.
///
/// **A `started` line is read whatever its stamp**, for its schema only. The
/// rule a session is read by is on that line, and the session's entries
/// follow it in the file whatever it is stamped: skip the wall-clock one and
/// a schema-3 backtest is read by the old inference again, quietly. Its stamp
/// still decides whether it ends a blindness. A `started` line after the
/// instant is a restart that had not happened yet, and a session that began
/// later cannot have ended a blindness at the instant -- in a paper journal
/// that restarted, that is the restart's own line, and taking it would
/// report an outage before it as over.
#[must_use]
pub fn decisions_at(entries: &[JournalEntry], at: i64, span: i64) -> Decisions {
    let from = at.saturating_sub(span);
    let mut out = Decisions::default();
    let mut blind: Option<(i64, String)> = None;
    let mut seen_since = false;
    // Whether this session writes `sighted` lines, so that only one of them
    // is evidence the market came back.
    let mut reads_sighted = false;

    for entry in entries {
        let stamp = entry.at().as_nanos();
        if stamp > at {
            // Later than the instant, so not evidence about it; but a
            // session's schema holds for every line after its `started`,
            // whenever that line is stamped.
            if let JournalEntry::Started { schema, .. } = entry {
                reads_sighted = *schema >= SIGHTED_SINCE;
            }
            continue;
        }
        // `Stopped` and the venue's own lines do the same thing, nothing, for
        // different reasons, and the arms are kept apart so each reason stays
        // written down beside the entries it is about: `replay`'s argument
        // for keeping `Orphaned` separate.
        #[allow(clippy::match_same_arms)]
        match entry {
            JournalEntry::Blind { cause, .. } => {
                blind = Some((stamp, format!("{cause:?}")));
                seen_since = false;
            }
            // A restart is a new session; the previous session's blindness is
            // not this one's state.
            JournalEntry::Started { schema, .. } => {
                blind = None;
                seen_since = false;
                reads_sighted = *schema >= SIGHTED_SINCE;
            }
            // The market's return, written down. Schema 3 onward; an older
            // file has none.
            JournalEntry::Sighted { .. } => {
                if blind.is_some() {
                    seen_since = true;
                }
            }
            // `Stopped` proves nothing. It is written unconditionally at
            // shutdown and stamped with `stats.last_ts` — which, for a session
            // that ended while blind, *is* the gap event or, live, a venue
            // report delivered after it. Neither is the market. Counting it
            // would make every run that died in an outage report the stream as
            // seen again, which is the one case where saying so is most wrong.
            JournalEntry::Stopped { .. } => {}
            // In a session that writes `sighted`, none of these is evidence: a
            // live venue's fill, and whatever the strategy decided on it, land
            // while the market is dark. In one that does not, every *other*
            // entry the engine wrote is taken as
            // proof the stream came back. Against a simulated venue the
            // engine's clock only advances on a market event, so a line
            // stamped after the gap was written while handling a later one.
            // That rests on venue reports never arriving without a market
            // event, which a live venue breaks -- see the doc comment above.
            //
            // Every one of them, not only trades. This listed `Submitted |
            // Filled` until the post-milestone audit, which is narrower than
            // the sentence above it in a way that mattered: a run that
            // recovers and then simply does not trade for an hour kept
            // reporting "with nothing seen since", turning a quiet hour back
            // into the dark hour this block exists to tell it apart from.
            //
            // Listed by name rather than caught by `_`, so an entry added
            // later is a compile error here until somebody has decided
            // whether it is evidence about the market.
            JournalEntry::Filled { .. }
            | JournalEntry::Checkpoint { .. }
            | JournalEntry::Tripped { .. }
            | JournalEntry::Submitted { .. }
            | JournalEntry::Refused { .. }
            | JournalEntry::Accepted { .. }
            | JournalEntry::Rejected { .. }
            | JournalEntry::CancelRequested { .. }
            | JournalEntry::Cancelled { .. }
            | JournalEntry::Orphaned { .. }
            | JournalEntry::Note { .. } => {
                if blind.is_some() && !reads_sighted {
                    seen_since = true;
                }
            }
            // What a live venue says on its own, schema 3. No evidence about
            // the market at all: a venue speaks on a wake the market did not
            // cause, which is exactly the wake a dark market produces.
            JournalEntry::Placed { .. }
            | JournalEntry::NeverLanded { .. }
            | JournalEntry::Doubt { .. }
            | JournalEntry::CancelRefused { .. }
            | JournalEntry::Foreign { .. }
            | JournalEntry::Account { .. }
            | JournalEntry::Anomaly { .. }
            | JournalEntry::Stray { .. }
            // The account, asked of the venue on the wall clock: not the
            // market feed.
            | JournalEntry::Balances { .. } => {}
        }
        if stamp <= from {
            continue;
        }
        match entry {
            JournalEntry::Submitted { .. } | JournalEntry::Refused { .. } => {
                out.within.extend(decisions(std::slice::from_ref(entry)));
            }
            JournalEntry::Note { kind, .. } => {
                *out.notes.entry(kind.clone()).or_insert(0) += 1;
            }
            // Not decisions, and not the strategy's own account of one: money,
            // claims, controls, the lifecycle after a decision, and what a
            // venue said. Named, so a new entry has to be placed here.
            JournalEntry::Started { .. }
            | JournalEntry::Filled { .. }
            | JournalEntry::Checkpoint { .. }
            | JournalEntry::Tripped { .. }
            | JournalEntry::Accepted { .. }
            | JournalEntry::Rejected { .. }
            | JournalEntry::CancelRequested { .. }
            | JournalEntry::Cancelled { .. }
            | JournalEntry::Orphaned { .. }
            | JournalEntry::Blind { .. }
            | JournalEntry::Stopped { .. }
            | JournalEntry::Placed { .. }
            | JournalEntry::NeverLanded { .. }
            | JournalEntry::Doubt { .. }
            | JournalEntry::CancelRefused { .. }
            | JournalEntry::Foreign { .. }
            | JournalEntry::Account { .. }
            | JournalEntry::Anomaly { .. }
            | JournalEntry::Stray { .. }
            | JournalEntry::Sighted { .. }
            | JournalEntry::Balances { .. } => {}
        }
    }
    out.blind = blind.map(|(stamp, cause)| (stamp, cause, seen_since));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::event::Side;
    use quant_core::execution::ClientOrderId;
    use quant_core::time::Ts;
    use quant_engine::journal::InstrumentKey;

    fn key() -> InstrumentKey {
        InstrumentKey {
            exchange: quant_core::instrument::Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
        }
    }

    fn submitted(id: u64, at: i64) -> JournalEntry {
        JournalEntry::Submitted {
            at: Ts::from_nanos(at),
            client_order_id: ClientOrderId(id),
            instrument: key(),
            side: Side::Buy,
            qty: "1".parse().expect("qty"),
            limit: None,
            mark: None,
        }
    }

    fn note(kind: &str, at: i64) -> JournalEntry {
        JournalEntry::Note {
            at: Ts::from_nanos(at),
            kind: kind.to_owned(),
            detail: serde_json::json!({ "why": "because" }),
        }
    }

    fn claim(at: i64, tally: &serde_json::Value) -> JournalEntry {
        JournalEntry::Note {
            at: Ts::from_nanos(at),
            kind: "claim".to_owned(),
            detail: serde_json::json!({ "tally": tally }),
        }
    }

    #[test]
    fn a_dense_run_of_ids_is_complete() {
        let entries: Vec<_> = (1_u64..=5)
            .map(|i| submitted(i, i64::try_from(i).expect("small")))
            .collect();
        let report = check(&entries);
        assert!(report.is_complete(), "{:?}", report.findings);
        assert_eq!(report.decisions, 5);
    }

    #[test]
    fn a_hole_in_the_ids_names_the_decision_that_was_not_written_down() {
        // The first criterion, and the reason the id is minted *before* the risk
        // check rather than after: the omission leaves evidence in the file
        // itself, so the record can be checked without anything outside it.
        let entries: Vec<_> = [1, 2, 4, 5]
            .iter()
            .map(|&i| submitted(i, i64::try_from(i).expect("small")))
            .collect();
        let report = check(&entries);
        assert_eq!(
            report.findings,
            vec![Finding::MissingDecision { client_order_id: 3 }]
        );
    }

    #[test]
    fn a_note_lost_between_two_claims_is_caught_by_the_fold() {
        // What P1 structurally cannot see: a note consumes no id, so losing one
        // leaves no hole. The claim fold is the only thing that covers it, and
        // it is weaker on purpose -- both sides are written by the same
        // strategy, so it catches a dropped line and not a wrong belief.
        let good = vec![
            claim(1, &serde_json::json!({ "entry": 0 })),
            note("entry", 2),
            claim(3, &serde_json::json!({ "entry": 1 })),
        ];
        assert!(check(&good).is_complete());

        let lost = vec![
            claim(1, &serde_json::json!({ "entry": 0 })),
            claim(3, &serde_json::json!({ "entry": 1 })),
        ];
        assert_eq!(
            check(&lost).findings,
            vec![Finding::ClaimDoesNotFold {
                kind: "entry".to_owned(),
                claimed: 1,
                counted: 0,
            }]
        );
    }

    #[test]
    fn a_running_total_that_falls_is_a_finding_rather_than_a_negative_delta() {
        // Subtracting unsigned would wrap; reporting it as a huge expected count
        // would bury the actual defect, which is that a total went backwards.
        let entries = vec![
            claim(1, &serde_json::json!({ "entry": 5 })),
            claim(2, &serde_json::json!({ "entry": 3 })),
        ];
        assert_eq!(
            check(&entries).findings,
            vec![Finding::ClaimWentBackwards {
                kind: "entry".to_owned(),
                from: 5,
                to: 3,
            }]
        );
    }

    #[test]
    fn two_identical_runs_agree() {
        let a: Vec<_> = (1_u64..=3)
            .map(|i| submitted(i, i64::try_from(i).expect("small")))
            .collect();
        assert_eq!(diff(&a, &a.clone()), Divergence::Agree { decisions: 3 });
    }

    #[test]
    fn the_diff_localises_to_the_first_decision_that_differs() {
        let a: Vec<_> = (1_u64..=3)
            .map(|i| submitted(i, i64::try_from(i).expect("small")))
            .collect();
        let mut b = a.clone();
        b[1] = submitted(2, 999);
        match diff(&a, &b) {
            Divergence::Differs { at_decision, .. } => assert_eq!(at_decision, 2),
            other => panic!("expected a localised divergence, got {other:?}"),
        }
    }

    #[test]
    fn a_decision_at_a_different_instant_is_a_divergence() {
        // The check that was vacuous before `at` entered the comparison. The ids
        // are dense from 1 and a long/flat crossover alternates buy and sell at
        // one size, so without the timestamp every line reads the same and two
        // entirely different runs compare equal. Measured: 786 decisions from
        // two different strategies, every one "identical".
        let a = vec![submitted(1, 100)];
        let b = vec![submitted(1, 200)];
        assert!(
            matches!(diff(&a, &b), Divergence::Differs { .. }),
            "same id, same side, same size, different moment"
        );
    }

    #[test]
    fn nothing_to_compare_is_not_agreement() {
        // M5.c's lesson, which is why there are three exit codes and not two.
        assert_eq!(diff(&[], &[]), Divergence::NothingToCompare);
    }

    #[test]
    fn a_restarted_live_side_is_refused_before_anything_is_compared() {
        // A state problem, not a reader problem: `Engine::resuming` replaces only
        // the portfolio, so a restarted session returns with empty indicator
        // windows and a zeroed risk tally while the replay runs straight
        // through. The comparison would fail for a reason unrelated to the
        // question, so it is refused rather than reported as a divergence.
        let started = |at: i64| JournalEntry::Started {
            at: Ts::from_nanos(at),
            cash: quant_core::Notional::from_raw(0),
            schema: quant_engine::journal::SCHEMA,
            run: None,
        };
        let live = vec![started(1), submitted(1, 2), started(3), submitted(2, 4)];
        assert_eq!(
            diff(&live, &live.clone()),
            Divergence::LiveRestarted { started: 2 },
            "two identical files, and still refused"
        );
    }

    #[test]
    fn blindness_is_carried_forward_from_before_the_window() {
        // The twelve-hour outage, in miniature, as a paper journal records it.
        // Against a simulated venue the engine's clock is the event stream, so
        // an outage writes nothing while it lasts and the `blind` line sits at
        // the moment the stream stopped -- hours before the hour anyone asks
        // about. A window query alone finds silence and an operator reads
        // silence as quiet.
        let entries = vec![
            submitted(1, 100),
            JournalEntry::Blind {
                at: Ts::from_nanos(200),
                cause: quant_core::event::GapCause::Disconnect,
                last_good_ts: Ts::from_nanos(190),
            },
        ];
        let d = decisions_at(&entries, 10_000, 1_000);
        assert!(d.within.is_empty(), "nothing happened in the window");
        let (at, cause, recovered) = d.blind.expect("the outage is still the answer");
        assert_eq!(at, 200);
        assert_eq!(cause, "Disconnect");
        assert!(!recovered, "nothing was seen after it");
    }

    #[test]
    fn a_gap_the_stream_came_back_from_is_not_reported_as_still_blind() {
        // The other half, and without it every query after the first disconnect
        // of a fortnight would claim the market was dark.
        let entries = vec![
            JournalEntry::Blind {
                at: Ts::from_nanos(200),
                cause: quant_core::event::GapCause::Disconnect,
                last_good_ts: Ts::from_nanos(190),
            },
            submitted(1, 300),
        ];
        let d = decisions_at(&entries, 10_000, 1_000);
        let (_, _, recovered) = d.blind.expect("the gap is still on record");
        assert!(recovered, "a later entry proves the clock advanced again");
    }
    #[test]
    fn a_quiet_hour_after_a_gap_is_not_reported_as_a_dark_one() {
        // The audit's finding. `seen_since` was set only by `Submitted` and
        // `Filled`, so a run that recovered and then did not trade kept
        // reporting "with nothing seen since" -- turning a quiet hour back into
        // the dark hour this whole block exists to distinguish it from. Any
        // later entry proves the clock advanced, and against a simulated venue
        // only the market advances it.
        let entries = vec![
            JournalEntry::Blind {
                at: Ts::from_nanos(200),
                cause: quant_core::event::GapCause::Disconnect,
                last_good_ts: Ts::from_nanos(190),
            },
            // Recovery, evidenced by something that is neither a fill nor an
            // order: the strategy's hourly claim.
            JournalEntry::Note {
                at: Ts::from_nanos(300),
                kind: "claim".to_owned(),
                detail: serde_json::json!({ "tally": {} }),
            },
        ];
        let d = decisions_at(&entries, 10_000, 1_000);
        let (_, _, recovered) = d.blind.expect("the gap is still on record");
        assert!(
            recovered,
            "a claim after the gap proves the stream came back"
        );
    }

    #[test]
    fn a_restart_does_not_inherit_the_previous_session_s_blindness() {
        // A `Started` after a gap is a new session, and the old session's
        // darkness is not this one's state. Without this the first query of
        // every restarted run reports an outage that ended before it began.
        let entries = vec![
            JournalEntry::Blind {
                at: Ts::from_nanos(200),
                cause: quant_core::event::GapCause::Disconnect,
                last_good_ts: Ts::from_nanos(190),
            },
            JournalEntry::Started {
                at: Ts::from_nanos(300),
                cash: quant_core::Notional::from_raw(0),
                schema: quant_engine::journal::SCHEMA,
                run: None,
            },
        ];
        let d = decisions_at(&entries, 10_000, 1_000);
        assert!(d.blind.is_none(), "a new session starts sighted");
    }
    #[test]
    fn a_session_that_ends_while_blind_does_not_claim_the_stream_came_back() {
        // `Stopped` is written unconditionally at shutdown and stamped with the
        // engine's last clock reading -- which during a terminal outage is the
        // gap itself or, live, a venue report delivered after it. Neither is
        // the market. Counting it as proof would make every run that died in
        // an outage report the market as visible again, which is exactly the
        // case where that claim is most misleading.
        let entries = vec![
            JournalEntry::Blind {
                at: Ts::from_nanos(200),
                cause: quant_core::event::GapCause::Disconnect,
                last_good_ts: Ts::from_nanos(190),
            },
            JournalEntry::Stopped {
                at: Ts::from_nanos(200),
                working: Vec::new(),
            },
        ];
        let d = decisions_at(&entries, 10_000, 1_000);
        let (_, _, recovered) = d.blind.expect("the outage is still the answer");
        assert!(!recovered, "shutting down is not seeing the market again");
    }

    #[test]
    fn what_a_venue_says_on_its_own_is_no_evidence_the_market_came_back() {
        // A live venue speaks on wakes the market did not cause, which is
        // exactly what a dark market produces: the account stream coming
        // back, a placement acknowledged, an order in doubt. None of it says
        // anything about the market feed, so none of it ends a blindness.
        use quant_core::report::{AccountChange, Clearance, DoubtChange};
        let blind = JournalEntry::Blind {
            at: Ts::from_nanos(200),
            cause: quant_core::event::GapCause::Disconnect,
            last_good_ts: Ts::from_nanos(190),
        };
        let said = [
            JournalEntry::Account {
                at: Ts::from_nanos(300),
                change: AccountChange::Regained,
            },
            JournalEntry::Placed {
                at: Ts::from_nanos(310),
                client_order_id: ClientOrderId(1),
                venue_order_id: quant_core::execution::VenueOrderId("4293153".to_owned()),
                transact_ms: 1_499_405_658_657,
            },
            JournalEntry::Doubt {
                at: Ts::from_nanos(320),
                client_order_id: ClientOrderId(1),
                change: DoubtChange::Cleared {
                    how: Clearance::Reported,
                },
            },
        ];
        for line in said {
            let d = decisions_at(&[blind.clone(), line.clone()], 10_000, 1_000);
            let (_, _, recovered) = d.blind.expect("the outage is still the answer");
            assert!(!recovered, "{line:?} is not the market");
        }
    }

    fn started(at: i64, schema: u32) -> JournalEntry {
        JournalEntry::Started {
            at: Ts::from_nanos(at),
            cash: quant_core::Notional::from_raw(0),
            schema,
            run: None,
        }
    }

    fn filled_at(at: i64) -> JournalEntry {
        JournalEntry::Filled {
            at: Ts::from_nanos(at),
            client_order_id: ClientOrderId(1),
            instrument: key(),
            side: Side::Buy,
            px: "100".parse().expect("px"),
            qty: "1".parse().expect("qty"),
            fee: "0.1".parse().expect("fee"),
            is_maker: false,
            fee_asset: quant_core::execution::FeeAsset::Quote,
            remaining: Some("0".parse().expect("qty")),
            venue_order_id: None,
            trade_id: None,
        }
    }

    fn disconnect(at: i64) -> JournalEntry {
        JournalEntry::Blind {
            at: Ts::from_nanos(at),
            cause: quant_core::event::GapCause::Disconnect,
            last_good_ts: Ts::from_nanos(at - 10),
        }
    }

    #[test]
    fn from_schema_3_only_a_sighted_line_ends_a_blindness() {
        // J11. A live venue's fill arrives on a wake the market did not cause,
        // so in a session that writes `sighted` it is no evidence the market
        // is back: the inference that served every simulated journal would
        // read a dark hour with a fill in it as over.
        let mut entries = vec![started(100, 3), disconnect(200), filled_at(300)];
        let (_, _, recovered) = decisions_at(&entries, 1_000, 100)
            .blind
            .expect("the outage is on record");
        assert!(!recovered, "a fill is not the market, from schema 3");

        entries.push(JournalEntry::Sighted {
            at: Ts::from_nanos(400),
        });
        let (_, _, recovered) = decisions_at(&entries, 1_000, 100)
            .blind
            .expect("the outage is on record");
        assert!(recovered, "a `sighted` line is");

        // The same lines in a schema-2 session are read the old way, which is
        // right for them: their venue could not speak without the market.
        let older = vec![started(100, 2), disconnect(200), filled_at(300)];
        let (_, _, recovered) = decisions_at(&older, 1_000, 100)
            .blind
            .expect("the outage is on record");
        assert!(recovered, "a schema-2 session keeps the inference");
    }

    #[test]
    fn a_started_line_stamped_after_everything_does_not_hide_the_session() {
        // J12. `backtest --journal` stamps `started` on the wall clock, the
        // day the backtest ran, which is later than every event it replayed.
        // Stopping at the first entry after the instant stopped at line one,
        // and `explain --at` read nothing from any backtest journal.
        let wall_clock = 1_000_000;
        let entries = vec![
            started(wall_clock, 3),
            submitted(1, 100),
            disconnect(200),
            filled_at(300),
            submitted(2, 350),
            submitted(3, 5_000),
        ];
        let d = decisions_at(&entries, 400, 1_000);
        assert_eq!(d.within.len(), 2, "both decisions at or before 400");
        assert!(
            d.within[0].starts_with("100 submitted #1"),
            "{:?}",
            d.within
        );
        assert!(
            d.within[1].starts_with("350 submitted #2"),
            "{:?}",
            d.within
        );
        let (at, _, recovered) = d.blind.expect("the outage is on record");
        assert_eq!(at, 200);
        // The session's schema is read off the wall-clock line though it is
        // later than the instant: schema 3, so the fill and the submission
        // after the gap are not the market, and only `sighted` would be.
        assert!(!recovered, "schema 3 is read off a later-stamped `started`");

        let older = vec![started(wall_clock, 2), disconnect(200), filled_at(300)];
        let (_, _, recovered) = decisions_at(&older, 400, 1_000)
            .blind
            .expect("the outage is on record");
        assert!(
            recovered,
            "and schema 2 by the old rule, from the same line"
        );
    }

    #[test]
    fn a_restart_after_the_instant_does_not_end_a_blindness_before_it() {
        // The other side of reading every `started` line: one stamped after
        // the instant gives its schema and nothing else. A paper journal that
        // restarted holds the restart's line after the outage, and a session
        // that began later cannot have ended a blindness at the instant.
        let entries = vec![
            started(100, 2),
            disconnect(200),
            started(5_000, 2),
            submitted(1, 5_100),
        ];
        let (at, _, recovered) = decisions_at(&entries, 400, 100)
            .blind
            .expect("the outage before the restart is still the answer");
        assert_eq!(at, 200);
        assert!(!recovered, "nothing at or before the instant ended it");
    }

    #[test]
    fn two_decisions_under_one_id_are_named_rather_than_deduplicated() {
        // What the hole check structurally cannot see: nothing is missing, so
        // density holds and the file reads COMPLETE. A journal written before
        // `minting_from` was wired has exactly this shape -- a restarted session
        // reminted from 1 -- and a checker that assumed its own fix had always
        // been in place would vouch for it.
        let entries = vec![submitted(1, 10), submitted(2, 20), submitted(1, 30)];
        let report = check(&entries);
        assert_eq!(
            report.findings,
            vec![Finding::DuplicateDecision {
                client_order_id: 1,
                times: 2
            }]
        );
        assert!(!report.is_complete());
    }

    #[test]
    fn a_line_torn_before_a_restart_fails_the_check_where_nothing_else_can_see_it() {
        // J14. The restarted engine resumed from the same short file, so
        // nothing downstream disagrees: here the lost line is the highest
        // submission, and the restart minted its id again, so the ids are
        // dense and the decisions alone read COMPLETE.
        let recovered = Recovered {
            entries: vec![submitted(1, 10), submitted(2, 20)],
            torn_tail: false,
            torn_before_restart: vec![3],
        };
        assert!(
            check(&recovered.entries).is_complete(),
            "the entries see nothing"
        );
        let report = check_file(&recovered);
        assert_eq!(
            report.findings,
            vec![Finding::TornBeforeRestart { line: 3 }]
        );

        // A torn tail is a journal still being written, not a lost entry.
        let still_writing = Recovered {
            torn_tail: true,
            torn_before_restart: Vec::new(),
            ..recovered
        };
        assert!(check_file(&still_writing).is_complete());
    }

    fn venue_id(id: &str) -> quant_core::execution::VenueOrderId {
        quant_core::execution::VenueOrderId(id.to_owned())
    }

    fn foreign(at: i64, client_id: &str, provenance: Provenance, order: &str) -> JournalEntry {
        JournalEntry::Foreign {
            at: Ts::from_nanos(at),
            client_id: quant_core::report::VenueText::new(client_id),
            provenance,
            venue_order_id: venue_id(order),
            side: Side::Sell,
            what: ForeignOutcome::Accepted,
        }
    }

    fn traded(entry: JournalEntry) -> JournalEntry {
        let JournalEntry::Foreign {
            at,
            client_id,
            provenance,
            venue_order_id,
            side,
            what: _,
        } = entry
        else {
            unreachable!("only a foreign line trades here")
        };
        JournalEntry::Foreign {
            at,
            client_id,
            provenance,
            venue_order_id,
            side,
            what: ForeignOutcome::Traded {
                trade_id: quant_core::execution::VenueTradeId("77".to_owned()),
                px: "100".parse().expect("px"),
                qty: "2".parse().expect("qty"),
                commission: "0.3".parse().expect("fee"),
                commission_asset: quant_core::report::VenueText::new("BNB"),
                remaining: "1".parse().expect("qty"),
            },
        }
    }

    #[test]
    fn what_the_record_cannot_account_for_is_a_finding_and_a_stranger_is_counted() {
        // J13. A fill no order of this run explains, a report about an order
        // it was not working, and another run's order on the same account:
        // each is something this run's record does not account for. A
        // stranger's order is a fact about a shared account, and is counted.
        let other_run = || Provenance::OtherRun(quant_core::report::VenueText::new("r0"));
        let entries = vec![
            submitted(1, 10),
            submitted(2, 20),
            JournalEntry::Orphaned {
                at: Ts::from_nanos(30),
                client_order_id: ClientOrderId(7),
                px: "100".parse().expect("px"),
                qty: "1".parse().expect("qty"),
                fee: "0.1".parse().expect("fee"),
                fee_asset: quant_core::execution::FeeAsset::Quote,
                venue_order_id: None,
                trade_id: None,
            },
            JournalEntry::Stray {
                at: Ts::from_nanos(40),
                client_order_id: ClientOrderId(9),
                kind: StrayKind::Accepted,
                venue_order_id: Some(venue_id("901")),
            },
            // Another run's order 5, heard of twice and traded once: one
            // finding, however many lines.
            foreign(50, "q-r0-5", other_run(), "500"),
            traded(foreign(55, "q-r0-5", other_run(), "500")),
            foreign(60, "q-??", Provenance::Unparseable, "600"),
            // A person's order on the same account, twice: one order counted.
            foreign(70, "web_1", Provenance::NotOurs, "700"),
            traded(foreign(75, "web_1", Provenance::NotOurs, "700")),
            foreign(80, "web_2", Provenance::NotOurs, "800"),
        ];
        let report = check(&entries);
        assert_eq!(
            report.findings,
            vec![
                Finding::UnattributedFill {
                    client_order_id: 7,
                    at: Ts::from_nanos(30),
                },
                Finding::Stray {
                    client_order_id: 9,
                    kind: StrayKind::Accepted,
                    at: Ts::from_nanos(40),
                },
                Finding::ForeignOrder {
                    client_id: "q-r0-5".to_owned(),
                    venue_order_id: "500".to_owned(),
                    run: Some("r0".to_owned()),
                    lines: 2,
                    trades: 1,
                },
                Finding::ForeignOrder {
                    client_id: "q-??".to_owned(),
                    venue_order_id: "600".to_owned(),
                    run: None,
                    lines: 1,
                    trades: 0,
                },
            ]
        );
        assert_eq!(report.not_ours, 2, "two orders, three lines");
        assert!(!report.is_complete());
        assert_eq!(report.decisions, 2, "none of these is a decision");
    }

    fn doubt(at: i64, id: u64, change: DoubtChange) -> JournalEntry {
        JournalEntry::Doubt {
            at: Ts::from_nanos(at),
            client_order_id: ClientOrderId(id),
            change,
        }
    }

    fn raised(at: i64, id: u64) -> JournalEntry {
        doubt(
            at,
            id,
            DoubtChange::Raised {
                cause: DoubtCause::NoAnswer,
                said: None,
            },
        )
    }

    fn filled_leaving(at: i64, id: u64, remaining: &str) -> JournalEntry {
        let JournalEntry::Filled {
            at: _,
            client_order_id: _,
            instrument,
            side,
            px,
            qty,
            fee,
            is_maker,
            fee_asset,
            remaining: _,
            venue_order_id,
            trade_id,
        } = filled_at(at)
        else {
            unreachable!("filled_at builds a fill")
        };
        JournalEntry::Filled {
            at: Ts::from_nanos(at),
            client_order_id: ClientOrderId(id),
            instrument,
            side,
            px,
            qty,
            fee,
            is_maker,
            fee_asset,
            remaining: Some(remaining.parse().expect("qty")),
            venue_order_id,
            trade_id,
        }
    }

    #[test]
    fn a_doubt_is_a_finding_until_it_is_cleared_or_the_order_ends() {
        // J13's other half. Raised and cleared explicitly, so "left in doubt"
        // is a fold of the file rather than an inference from what is
        // missing. An acceptance and a partial fill say nothing about how
        // the order ends, so neither settles it.
        let mut entries = vec![
            submitted(1, 10),
            raised(100, 1),
            JournalEntry::Accepted {
                at: Ts::from_nanos(110),
                client_order_id: ClientOrderId(1),
                venue_order_id: None,
            },
            filled_leaving(120, 1, "0.5"),
        ];
        assert_eq!(
            check(&entries).findings,
            vec![Finding::LeftInDoubt {
                client_order_id: 1,
                since: Ts::from_nanos(100),
                cause: DoubtCause::NoAnswer,
            }]
        );
        entries.push(filled_leaving(130, 1, "0"));
        assert!(
            check(&entries).is_complete(),
            "filled with nothing left: the order ended, so its outcome is known"
        );

        // Every other way out: cleared, cancelled, rejected, never landed.
        let ended = vec![
            submitted(2, 10),
            submitted(3, 11),
            submitted(4, 12),
            submitted(5, 13),
            raised(200, 2),
            doubt(
                210,
                2,
                DoubtChange::Cleared {
                    how: quant_core::report::Clearance::Reported,
                },
            ),
            raised(300, 3),
            JournalEntry::Cancelled {
                at: Ts::from_nanos(310),
                client_order_id: ClientOrderId(3),
                remaining: "1".parse().expect("qty"),
                venue_order_id: None,
                ended: None,
            },
            raised(400, 4),
            JournalEntry::Rejected {
                at: Ts::from_nanos(410),
                client_order_id: ClientOrderId(4),
                reason: quant_core::execution::RejectReason::Venue,
                cited: None,
                breach: None,
            },
            raised(500, 5),
            JournalEntry::NeverLanded {
                at: Ts::from_nanos(510),
                client_order_id: ClientOrderId(5),
                landing: quant_core::report::Landing::NotSent {
                    why: quant_core::report::VenueText::new("connect refused"),
                },
            },
        ];
        // Dense from 1 is not this test's question.
        let left: Vec<_> = check(&ended)
            .findings
            .into_iter()
            .filter(|f| !matches!(f, Finding::MissingDecision { .. }))
            .collect();
        assert_eq!(left, Vec::new());
    }
}
