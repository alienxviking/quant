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

use std::collections::BTreeMap;

use quant_engine::journal::JournalEntry;

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
}

impl core::fmt::Display for Finding {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
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
    pub findings: Vec<Finding>,
}

impl Report {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.findings.is_empty()
    }
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
/// The engine's clock is the event stream, so an outage produces no entries at
/// all while it lasts — the `blind` line sits at the moment the stream stopped,
/// which for M5's twelve-hour outage is eleven hours before the hour someone
/// asks about. A window query finds nothing and an operator reads nothing as
/// "quiet". The answer to *why did it not trade between 03:00 and 04:00* lives
/// before 03:00, so the last blindness at or before the instant is carried
/// forward and the search for the recovery that ends it runs to the instant.
///
/// Same shape as `market_at`'s day rule: ask the narrow question first, widen
/// only when the narrow answer is the uninformative one.
#[must_use]
pub fn decisions_at(entries: &[JournalEntry], at: i64, span: i64) -> Decisions {
    let from = at.saturating_sub(span);
    let mut out = Decisions::default();
    let mut blind: Option<(i64, String)> = None;
    let mut seen_since = false;

    for entry in entries {
        let stamp = entry.at().as_nanos();
        if stamp > at {
            break;
        }
        match entry {
            JournalEntry::Blind { cause, .. } => {
                blind = Some((stamp, format!("{cause:?}")));
                seen_since = false;
            }
            // Any entry the engine wrote after a gap is proof the stream came
            // back: the clock only advances on an event.
            JournalEntry::Submitted { .. } | JournalEntry::Filled { .. } if blind.is_some() => {
                seen_since = true;
            }
            _ => {}
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
            _ => {}
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
        // The twelve-hour outage, in miniature. The engine's clock is the event
        // stream, so an outage writes nothing while it lasts and the `blind`
        // line sits at the moment the stream stopped -- hours before the hour
        // anyone asks about. A window query alone finds silence and an operator
        // reads silence as quiet.
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
}
