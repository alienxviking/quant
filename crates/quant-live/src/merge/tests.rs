//! The machine's own tests: `docs/live-run.md` §5, *`LiveVenue`, decided
//! before the code*, §5's table, one test per row, each seen red under the
//! break the table names before it was trusted; and the rows of §2 and §3 the
//! table does not reach.
//!
//! Every stream input is a frame through the real classifier and translator,
//! from [`crate::fixtures`], so a test here exercises what a recorded frame
//! would.

use super::*;
use crate::fixtures::{self, btcusdt, ours, run};
use quant_binance::orders::{FilterName, Refusal, RefusalCause, VenueError, WhyUnknown};
use quant_core::execution::RejectReason;
use quant_core::instrument::{FilterBreach, InstrumentDef, InstrumentRegistry};
use quant_core::report::{Cited, EndedBy, Landing, Provenance, RejectedWhy};

/// A machine and a clock that moves ten nanoseconds per input.
struct Bench {
    merge: Merge,
    instrument: InstrumentId,
    other: InstrumentId,
    at: i64,
}

impl Bench {
    /// A machine whose account has not been heard.
    fn down() -> Self {
        let mut registry = InstrumentRegistry::new();
        let instrument = registry.register(btcusdt());
        let other = registry.register(InstrumentDef {
            symbol: "ETHUSDT".to_owned(),
            base: "ETH".to_owned(),
            ..btcusdt()
        });
        let translator = Translator::new(run(), &btcusdt()).expect("both assets named");
        Self {
            merge: Merge::new(instrument, translator),
            instrument,
            other,
            at: 0,
        }
    }

    /// A machine whose account is heard.
    fn heard() -> Self {
        let mut bench = Self::down();
        assert_eq!(lines(&bench.frame(&fixtures::subscribed())), ["regained"]);
        bench
    }

    fn step(&mut self, input: Input) -> Vec<VenueReport> {
        self.at += 10;
        let mut out = Vec::new();
        self.merge.step(Ts::from_nanos(self.at), input, &mut out);
        out
    }

    fn frame(&mut self, payload: &[u8]) -> Vec<VenueReport> {
        self.at += 10;
        let mut out = Vec::new();
        self.merge.frame(Ts::from_nanos(self.at), payload, &mut out);
        out
    }

    fn submit(&mut self, id: u64, qty: &str) {
        let out = self.step(Input::Submitted {
            id: ClientOrderId(id),
            qty: qty.parse().expect("qty"),
        });
        assert!(out.is_empty(), "{out:?}");
    }

    fn placed(&mut self, id: u64, order: u64) -> Vec<VenueReport> {
        self.step(Input::Placement {
            id: ClientOrderId(id),
            placement: Placement::Placed {
                order_id: VenueOrderId(order.to_string()),
                client_order_id: ours(id),
                transact_time_ms: 1_499_405_658_657,
            },
        })
    }

    fn unknown(&mut self, id: u64, why: WhyUnknown) -> Vec<VenueReport> {
        self.step(Input::Placement {
            id: ClientOrderId(id),
            placement: Placement::Unknown { why },
        })
    }

    fn refused(&mut self, id: u64) -> Vec<VenueReport> {
        self.step(Input::Placement {
            id: ClientOrderId(id),
            placement: Placement::Refused(Refusal {
                http_status: 400,
                code: -1013,
                msg: "Filter failure: NOTIONAL".to_owned(),
                cause: RefusalCause::Filter(FilterName::Notional),
            }),
        })
    }

    fn ready(&self) -> Result<(), Unready> {
        self.merge.ready(self.instrument, Queue::Room)
    }
}

/// No line at all.
const NOTHING: [&str; 0] = [];

/// What the engine is told, one short line per report, without stamps.
fn lines(out: &[VenueReport]) -> Vec<String> {
    out.iter().map(line).collect()
}

fn line(report: &VenueReport) -> String {
    match report {
        VenueReport::Order(report) => match report {
            OrderReport::Accepted {
                client_order_id,
                venue_order_id,
                ts: _,
            } => format!(
                "accepted {} {}",
                client_order_id.0,
                venue_order_id.as_ref().map_or("-", |v| v.0.as_str())
            ),
            OrderReport::Rejected {
                client_order_id,
                reason,
                why,
                ts: _,
            } => match why {
                Some(RejectedWhy::NeverLanded(_)) => {
                    format!("never_landed {}", client_order_id.0)
                }
                Some(RejectedWhy::Declined { .. }) | None => {
                    format!("rejected {} {reason:?}", client_order_id.0)
                }
            },
            OrderReport::Filled {
                client_order_id,
                fill,
                remaining,
                ids,
                ts: _,
            } => format!(
                "filled {} {} left {} t={}",
                client_order_id.0,
                fill.qty,
                remaining,
                ids.as_ref().map_or("-", |ids| ids.trade_id.0.as_str())
            ),
            OrderReport::Cancelled {
                client_order_id,
                remaining,
                ended,
                ts: _,
            } => format!(
                "cancelled {} left {} {:?}",
                client_order_id.0,
                remaining,
                ended.as_ref().map(|e| e.how)
            ),
        },
        VenueReport::Notice(notice) => match notice {
            Notice::Placed {
                client_order_id,
                venue_order_id,
                ..
            } => format!("placed {} {}", client_order_id.0, venue_order_id.0),
            Notice::Doubt {
                client_order_id,
                change,
                ts: _,
            } => match change {
                DoubtChange::Raised { cause, said: _ } => {
                    format!("raised {} {cause:?}", client_order_id.0)
                }
                DoubtChange::Cleared { how: _ } => format!("cleared {}", client_order_id.0),
            },
            Notice::CancelRefused {
                client_order_id,
                why,
                ts: _,
            } => {
                let why = match why {
                    CancelRefusal::NotOpen(_) => "not_open",
                    CancelRefusal::Refused(_) => "refused",
                    CancelRefusal::Unknown { .. } => "unknown",
                    CancelRefusal::NotSent { .. } => "not_sent",
                };
                format!("cancel_refused {} {why}", client_order_id.0)
            }
            Notice::Foreign(report) => match &report.what {
                ForeignOutcome::Accepted => "foreign accepted".to_owned(),
                ForeignOutcome::Traded {
                    commission_asset, ..
                } => format!("foreign traded {}", commission_asset.as_str()),
                ForeignOutcome::Ended { .. } => "foreign ended".to_owned(),
            },
            Notice::Account { change, ts: _ } => match change {
                AccountChange::Lost { cause } => format!("lost {cause:?}"),
                AccountChange::Regained => "regained".to_owned(),
            },
            Notice::Anomaly {
                client_order_id,
                what,
                ..
            } => format!(
                "anomaly {} {what:?}",
                client_order_id.map_or("-".to_owned(), |id| id.0.to_string())
            ),
        },
    }
}

#[test]
fn a_duplicate_trade_id_books_once_beside_an_anomaly() {
    // Break: deduplicate by `I` (the translator reading it as the trade id),
    // or not at all. The copy has its own `I`, as a resent frame would, and a
    // `z` that its first booking already accounts for.
    let mut b = Bench::heard();
    b.submit(1, "0.003");
    b.placed(1, 100);
    b.frame(&fixtures::new_order(&ours(1), 100, "0.003"));
    let first = fixtures::trade(&ours(1), 100, "0.003", "0.001", "0.001", 7);
    assert_eq!(lines(&b.frame(&first)), ["filled 1 0.001 left 0.002 t=7"]);
    let copy = fixtures::trade_with(&ours(1), 100, "0.003", "0.001", "0.001", 7, 9_999, "USDT");
    assert_eq!(lines(&b.frame(&copy)), ["anomaly 1 DuplicateTrade"]);
    assert!(!b.merge.diverged());
    assert_eq!(b.ready(), Ok(()));
    // And the order carries on from what was booked once.
    let next = fixtures::trade(&ours(1), 100, "0.003", "0.002", "0.003", 8);
    assert_eq!(lines(&b.frame(&next)), ["filled 1 0.002 left 0 t=8"]);
}

#[test]
fn a_resent_copy_of_an_orders_last_trade_is_a_duplicate_not_a_fill_after_its_end() {
    // Break: judge the end before the trade id.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.frame(&fixtures::new_order(&ours(1), 100, "0.001"));
    b.frame(&fixtures::trade(
        &ours(1),
        100,
        "0.001",
        "0.001",
        "0.001",
        7,
    ));
    let copy = fixtures::trade_with(&ours(1), 100, "0.001", "0.001", "0.001", 7, 4_242, "USDT");
    assert_eq!(lines(&b.frame(&copy)), ["anomaly 1 DuplicateTrade"]);
    assert!(!b.merge.diverged(), "a duplicate latches nothing");
}

#[test]
fn a_second_new_before_any_fill_is_a_contradiction_though_its_z_continues() {
    // Break: judge it by `z` alone, which releases it.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    assert_eq!(
        lines(&b.frame(&fixtures::new_order(&ours(1), 100, "0.001"))),
        ["accepted 1 100"]
    );
    assert_eq!(
        lines(&b.frame(&fixtures::new_order(&ours(1), 100, "0.001"))),
        ["anomaly 1 Contradiction"]
    );
    assert_eq!(b.ready(), Err(Unready::Diverged));
}

#[test]
fn placed_then_a_cancel_acknowledged_for_another_order_id_diverges() {
    // Break: compare the stream's ids only.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.placed(1, 100);
    let ack = b.step(Input::CancelReply {
        id: ClientOrderId(1),
        reply: CancelReply::Acknowledged {
            order_id: VenueOrderId("200".to_owned()),
        },
    });
    assert_eq!(lines(&ack), ["anomaly 1 Contradiction"]);
    assert_eq!(b.ready(), Err(Unready::Diverged));

    // The same id: nothing at all, the stream says how the order ended.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.placed(1, 100);
    let ack = b.step(Input::CancelReply {
        id: ClientOrderId(1),
        reply: CancelReply::Acknowledged {
            order_id: VenueOrderId("100".to_owned()),
        },
    });
    assert!(ack.is_empty(), "{ack:?}");
    assert!(!b.merge.diverged());
}

#[test]
fn a_hole_holds_every_later_report_and_raises_fills_missed_once() {
    // Break: release past the hole.
    let mut b = Bench::heard();
    b.submit(1, "0.003");
    b.placed(1, 100);
    b.frame(&fixtures::new_order(&ours(1), 100, "0.003"));
    // The first trade, 0.001, was never heard: this one says 0.002 in all.
    let past = fixtures::trade(&ours(1), 100, "0.003", "0.001", "0.002", 8);
    assert_eq!(lines(&b.frame(&past)), ["raised 1 FillsMissed"]);
    let last = fixtures::trade(&ours(1), 100, "0.003", "0.001", "0.003", 9);
    assert!(lines(&b.frame(&last)).is_empty(), "held, and raised once");
    let end = fixtures::ended(&ours(1), 100, "0.003", "0.003", EndedBy::Expired);
    assert!(lines(&b.frame(&end)).is_empty(), "an end is held too");
    assert_eq!(b.ready(), Err(Unready::InDoubt));
}

#[test]
fn a_fill_after_an_end_is_an_orphan_an_anomaly_and_diverged_and_a_second_end_only_an_anomaly() {
    // Break: emit the second end.
    let mut b = Bench::heard();
    b.submit(1, "0.002");
    b.frame(&fixtures::new_order(&ours(1), 100, "0.002"));
    assert_eq!(
        lines(&b.frame(&fixtures::ended(
            &ours(1),
            100,
            "0.002",
            "0",
            EndedBy::Cancelled
        ))),
        ["cancelled 1 left 0.002 Some(Cancelled)"]
    );
    let late = fixtures::trade(&ours(1), 100, "0.002", "0.001", "0.001", 7);
    assert_eq!(
        lines(&b.frame(&late)),
        ["anomaly 1 AfterEnd", "filled 1 0.001 left 0.001 t=7"],
        "handed over, for the engine to write down as an orphan"
    );
    assert_eq!(b.ready(), Err(Unready::Diverged));

    let mut b = Bench::heard();
    b.submit(1, "0.002");
    b.frame(&fixtures::new_order(&ours(1), 100, "0.002"));
    b.frame(&fixtures::ended(
        &ours(1),
        100,
        "0.002",
        "0",
        EndedBy::Expired,
    ));
    assert_eq!(
        lines(&b.frame(&fixtures::ended(
            &ours(1),
            100,
            "0.002",
            "0",
            EndedBy::Cancelled
        ))),
        ["anomaly 1 SecondEnd"]
    );
    assert!(!b.merge.diverged(), "a second end only an anomaly");
    // A NEW after the end is `after_end`, and latches nothing either.
    assert_eq!(
        lines(&b.frame(&fixtures::new_order(&ours(1), 100, "0.002"))),
        ["anomaly 1 AfterEnd"]
    );
    assert!(!b.merge.diverged());
}

#[test]
fn rest_refusing_an_order_the_stream_reported_diverges_with_no_second_terminal() {
    // Break: emit the refusal.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    assert_eq!(
        lines(&b.frame(&fixtures::new_order(&ours(1), 100, "0.001"))),
        ["accepted 1 100"]
    );
    assert_eq!(lines(&b.refused(1)), ["anomaly 1 Contradiction"]);
    assert_eq!(b.ready(), Err(Unready::Diverged));
    // Still working: its fill is booked, not orphaned.
    assert_eq!(
        lines(&b.frame(&fixtures::trade(
            &ours(1),
            100,
            "0.001",
            "0.001",
            "0.001",
            7
        ))),
        ["filled 1 0.001 left 0 t=7"]
    );

    let mut b = Bench::heard();
    b.submit(1, "0.001");
    assert_eq!(lines(&b.refused(1)), ["rejected 1 Malformed"]);
    assert_eq!(
        lines(&b.frame(&fixtures::new_order(&ours(1), 100, "0.001"))),
        ["anomaly 1 Contradiction"]
    );
    assert_eq!(
        lines(&b.frame(&fixtures::trade(
            &ours(1),
            100,
            "0.001",
            "0.001",
            "0.001",
            7
        ))),
        ["anomaly 1 Contradiction", "filled 1 0.001 left 0 t=7"],
        "a trade on an order REST ended is handed over as an orphan"
    );
    assert_eq!(b.ready(), Err(Unready::Diverged));

    // The same for a request never sent.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.frame(&fixtures::new_order(&ours(1), 100, "0.001"));
    let never = b.step(Input::Placement {
        id: ClientOrderId(1),
        placement: Placement::NotSent {
            why: "connection refused".to_owned(),
        },
    });
    assert_eq!(lines(&never), ["anomaly 1 Contradiction"]);
}

#[test]
fn placed_and_new_naming_different_order_ids_diverge_in_either_order() {
    // Break: keep the later id.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.placed(1, 100);
    assert_eq!(
        lines(&b.frame(&fixtures::new_order(&ours(1), 200, "0.001"))),
        ["anomaly 1 Contradiction"]
    );
    assert_eq!(b.ready(), Err(Unready::Diverged));

    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.frame(&fixtures::new_order(&ours(1), 200, "0.001"));
    assert_eq!(lines(&b.placed(1, 100)), ["anomaly 1 Contradiction"]);
    assert_eq!(b.ready(), Err(Unready::Diverged));
}

#[test]
fn a_trade_as_an_orders_first_report_brings_an_acceptance_before_it() {
    // Break: omit it.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    assert_eq!(
        lines(&b.frame(&fixtures::trade(
            &ours(1),
            100,
            "0.001",
            "0.001",
            "0.001",
            7
        ))),
        ["accepted 1 100", "filled 1 0.001 left 0 t=7"]
    );
    // An end as the first report brings one too.
    b.submit(2, "0.001");
    assert_eq!(
        lines(&b.frame(&fixtures::ended(
            &ours(2),
            101,
            "0.001",
            "0",
            EndedBy::Expired
        ))),
        ["accepted 2 101", "cancelled 2 left 0.001 Some(Expired)"]
    );
}

#[test]
fn each_unknown_outcome_raises_its_own_cause_with_what_the_venue_said() {
    // Break: map two to one.
    let said = |http_status, code, msg: &str| {
        Some(Cited {
            http_status: Some(http_status),
            code,
            msg: VenueText::new(msg),
        })
    };
    let cases = [
        (
            WhyUnknown::ServerError {
                http_status: 503,
                said: Some(VenueError {
                    code: -1001,
                    msg: "Internal error".to_owned(),
                }),
            },
            DoubtCause::ServerError,
            said(503, -1001, "Internal error"),
        ),
        (
            WhyUnknown::Unsettled {
                http_status: 400,
                code: -1007,
                msg: "Timeout".to_owned(),
            },
            DoubtCause::Unsettled,
            said(400, -1007, "Timeout"),
        ),
        (
            WhyUnknown::IdInUse {
                id: ours(1),
                http_status: 400,
                code: -2010,
                msg: "Duplicate order sent.".to_owned(),
            },
            DoubtCause::IdInUse,
            said(400, -2010, "Duplicate order sent."),
        ),
        (
            WhyUnknown::UnreadableBody {
                http_status: 200,
                detail: "EOF".to_owned(),
            },
            DoubtCause::Unreadable,
            None,
        ),
        (
            WhyUnknown::OtherOrder {
                sent: ours(1),
                echoed: ours(2),
            },
            DoubtCause::OtherOrder,
            None,
        ),
        (
            WhyUnknown::UnexpectedStatus { http_status: 409 },
            DoubtCause::UnexpectedStatus,
            None,
        ),
        (WhyUnknown::TimedOut, DoubtCause::NoAnswer, None),
        (
            WhyUnknown::Interrupted("reset".to_owned()),
            DoubtCause::Interrupted,
            None,
        ),
    ];
    for (why, cause, said) in cases {
        let mut b = Bench::heard();
        b.submit(1, "0.001");
        let out = b.unknown(1, why.clone());
        let VenueReport::Notice(Notice::Doubt {
            client_order_id,
            change,
            ts: _,
        }) = &out[..].first().cloned().expect("a doubt raised")
        else {
            panic!("expected a doubt for {why:?}, got {out:?}");
        };
        assert_eq!(out.len(), 1, "{why:?}");
        assert_eq!(*client_order_id, ClientOrderId(1));
        assert_eq!(*change, DoubtChange::Raised { cause, said }, "{why:?}");
        assert_eq!(
            b.ready(),
            Err(if cause == DoubtCause::IdInUse {
                Unready::Diverged
            } else {
                Unready::InDoubt
            }),
            "{why:?}"
        );
    }
}

#[test]
fn id_in_use_latches_in_every_phase() {
    // Break: latch it only when REST answers first.
    let in_use = || WhyUnknown::IdInUse {
        id: ours(1),
        http_status: 400,
        code: -2010,
        msg: "Duplicate order sent.".to_owned(),
    };
    // Sent: raised, and open.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    assert_eq!(lines(&b.unknown(1, in_use())), ["raised 1 IdInUse"]);
    assert_eq!(b.ready(), Err(Unready::Diverged));
    // Live: raised and cleared at once.
    let mut b = Bench::heard();
    b.submit(1, "0.002");
    b.frame(&fixtures::new_order(&ours(1), 100, "0.002"));
    assert_eq!(
        lines(&b.unknown(1, in_use())),
        ["raised 1 IdInUse", "cleared 1"]
    );
    assert_eq!(b.ready(), Err(Unready::Diverged));
    // Ended by the stream: the same.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.frame(&fixtures::trade(
        &ours(1),
        100,
        "0.001",
        "0.001",
        "0.001",
        7,
    ));
    assert_eq!(
        lines(&b.unknown(1, in_use())),
        ["raised 1 IdInUse", "cleared 1"]
    );
    assert_eq!(b.ready(), Err(Unready::Diverged));
}

#[test]
fn unknown_about_an_order_already_heard_is_raised_and_cleared_at_once_and_otherwise_stays() {
    // Break: raise it without the immediate clearing when the stream came
    // first.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.frame(&fixtures::new_order(&ours(1), 100, "0.001"));
    assert_eq!(
        lines(&b.unknown(1, WhyUnknown::TimedOut)),
        ["raised 1 NoAnswer", "cleared 1"]
    );
    assert_eq!(b.ready(), Ok(()));

    // Mentioned and not heard: raised and open, beside the hold's own.
    let mut b = Bench::heard();
    b.submit(1, "0.002");
    b.frame(&fixtures::trade(
        &ours(1),
        100,
        "0.002",
        "0.001",
        "0.002",
        8,
    ));
    assert_eq!(
        lines(&b.unknown(1, WhyUnknown::TimedOut)),
        ["raised 1 NoAnswer"]
    );
    assert_eq!(b.ready(), Err(Unready::InDoubt));
}

#[test]
fn a_foreign_trade_paid_in_bnb_is_a_foreign_line_and_diverged_and_an_acceptance_only_a_line() {
    // Break: refuse the BNB fee.
    let mut b = Bench::heard();
    assert_eq!(
        lines(&b.frame(&fixtures::new_order("web_1", 900, "1"))),
        ["foreign accepted"]
    );
    assert_eq!(b.ready(), Ok(()));
    let theirs = fixtures::trade_with("web_1", 900, "1", "0.5", "0.5", 77, 78, "BNB");
    assert_eq!(lines(&b.frame(&theirs)), ["foreign traded BNB"]);
    assert_eq!(b.ready(), Err(Unready::Diverged));

    // Another run of ours trading here is foreign too, and it latches.
    let mut b = Bench::heard();
    let earlier = fixtures::trade("q-r0-5", 901, "1", "1", "1", 79);
    let out = b.frame(&earlier);
    assert!(
        matches!(
            &out[..],
            [VenueReport::Notice(Notice::Foreign(report))]
                if report.provenance == Provenance::OtherRun(VenueText::new("r0"))
        ),
        "{out:?}"
    );
    assert_eq!(b.ready(), Err(Unready::Diverged));
}

#[test]
fn after_an_overflow_while_down_any_account_event_is_heard_and_without_one_none_is() {
    // Break: take only the subscription's answer as heard, or take any event.
    let mut b = Bench::down();
    assert_eq!(
        lines(&b.step(Input::Blind(GapCause::LocalOverflow))),
        NOTHING
    );
    assert_eq!(lines(&b.frame(&fixtures::balances())), ["regained"]);
    assert_eq!(b.ready(), Ok(()));

    let mut b = Bench::down();
    assert_eq!(lines(&b.frame(&fixtures::balances())), NOTHING);
    assert_eq!(b.ready(), Err(Unready::StreamDown));
    assert_eq!(b.merge.stats().counted, 1);

    // A disconnect after the overflow: the next connection brings its own
    // answer, so an event no longer stands in for one.
    let mut b = Bench::down();
    b.step(Input::Blind(GapCause::LocalOverflow));
    b.step(Input::Blind(GapCause::Disconnect));
    assert_eq!(lines(&b.frame(&fixtures::balances())), NOTHING);
    assert_eq!(b.ready(), Err(Unready::StreamDown));
}

#[test]
fn a_subscription_answer_while_heard_is_a_loss_nobody_saw() {
    // Break: ignore it as a repeat.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.placed(1, 100);
    b.submit(2, "0.001");
    assert_eq!(
        lines(&b.frame(&fixtures::subscribed())),
        ["lost LocalOverflow", "raised 1 AccountBlind", "regained"],
        "order 2, which REST has not answered, has its cause deferred"
    );
    assert_eq!(b.ready(), Err(Unready::InDoubt));
    assert_eq!(
        lines(&b.placed(2, 101)),
        ["placed 2 101", "raised 2 AccountBlind"]
    );
}

#[test]
fn a_refused_subscription_answer_and_an_unrecognised_frame_never_reach_the_translator() {
    // Break: route by `e` alone, which hands everything else to the
    // translator, where an answer comes back unreadable and puts every
    // working order in doubt for nothing.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.placed(1, 100);
    let at = Ts::from_nanos(1);
    assert_eq!(
        b.merge.route(&fixtures::subscribe_refused(), at),
        Heard::Answer
    );
    assert_eq!(lines(&b.frame(&fixtures::subscribe_refused())), NOTHING);
    assert_eq!(b.merge.stats().answers, 1);
    assert_eq!(b.ready(), Ok(()));
    for frame in [&b"not json"[..], br#"{"event":{"E":1}}"#] {
        assert!(
            matches!(b.merge.route(frame, at), Heard::Unrecognised(_)),
            "{}",
            String::from_utf8_lossy(frame)
        );
    }
    // Unrecognised still could have been a fill: an anomaly naming no order,
    // and every working order in doubt.
    assert_eq!(
        lines(&b.frame(b"not json")),
        ["anomaly - Unreadable", "raised 1 FillsMissed"]
    );
}

#[test]
fn an_unreadable_report_doubts_the_order_it_concerns_or_every_order_when_it_names_none() {
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.placed(1, 100);
    b.submit(2, "0.001");
    b.placed(2, 101);
    b.submit(3, "0.001");
    // Ours, unreadable for its BNB fee: that order alone.
    let bnb = fixtures::trade_with(&ours(1), 100, "0.001", "0.001", "0.001", 7, 8, "BNB");
    assert_eq!(
        lines(&b.frame(&bnb)),
        ["anomaly 1 Unreadable", "raised 1 FillsMissed"]
    );
    // Somebody else's, unreadable: none of ours.
    let mut theirs: serde_json::Value =
        serde_json::from_slice(&fixtures::new_order("web_1", 900, "1")).expect("json");
    theirs["event"]["S"] = serde_json::json!("HOLD");
    let theirs = serde_json::to_vec(&theirs).expect("json");
    assert_eq!(lines(&b.frame(&theirs)), ["anomaly - Unreadable"]);
    // Unreadable before its id: every working order, deferred for order 3,
    // whose REST answer has not come.
    let mut nameless: serde_json::Value =
        serde_json::from_slice(&fixtures::new_order(&ours(2), 101, "0.001")).expect("json");
    nameless["event"]["c"] = serde_json::Value::Null;
    let nameless = serde_json::to_vec(&nameless).expect("json");
    assert_eq!(
        lines(&b.frame(&nameless)),
        ["anomaly - Unreadable", "raised 2 FillsMissed"]
    );
    assert_eq!(
        lines(&b.placed(3, 102)),
        ["placed 3 102", "raised 3 FillsMissed"]
    );
}

#[test]
fn losing_the_stream_puts_working_orders_in_doubt_and_defers_where_rest_has_not_answered() {
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.placed(1, 100);
    b.submit(2, "0.001");
    b.submit(3, "0.001");
    b.refused(3);
    assert_eq!(
        lines(&b.step(Input::Blind(GapCause::Disconnect))),
        ["lost SocketClosed", "raised 1 AccountBlind"],
        "order 2 deferred, order 3 over"
    );
    assert_eq!(b.ready(), Err(Unready::StreamDown));
    assert_eq!(
        lines(&b.step(Input::Blind(GapCause::Disconnect))),
        NOTHING,
        "a blind while down writes nothing"
    );
    assert_eq!(lines(&b.frame(&fixtures::subscribed())), ["regained"]);
    assert_eq!(b.ready(), Err(Unready::InDoubt));
    // A refusal drops the deferred cause: an order that never existed
    // missed nothing.
    assert_eq!(lines(&b.refused(2)), ["rejected 2 Malformed"]);
    // The stream's next report on order 1 clears it.
    assert_eq!(
        lines(&b.frame(&fixtures::new_order(&ours(1), 100, "0.001"))),
        ["cleared 1", "accepted 1 100"]
    );
    assert_eq!(b.ready(), Ok(()));
}

#[test]
fn a_deferred_cause_meeting_a_stream_report_first_is_raised_and_cleared_in_one_step() {
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.step(Input::Blind(GapCause::Disconnect));
    b.frame(&fixtures::subscribed());
    assert_eq!(b.ready(), Err(Unready::InDoubt), "a deferred cause");
    assert_eq!(
        lines(&b.frame(&fixtures::new_order(&ours(1), 100, "0.001"))),
        ["raised 1 AccountBlind", "cleared 1", "accepted 1 100"]
    );
    assert_eq!(b.ready(), Ok(()));
    assert_eq!(lines(&b.placed(1, 100)), ["placed 1 100"]);
}

#[test]
fn the_venue_ending_the_stream_is_a_loss_named_for_how_it_ended() {
    for (frame, cause) in [
        (fixtures::server_shutdown(), "lost ServerShutdown"),
        (fixtures::stream_terminated(), "lost StreamTerminated"),
    ] {
        let mut b = Bench::heard();
        assert_eq!(lines(&b.frame(&frame)), [cause]);
        assert_eq!(b.ready(), Err(Unready::StreamDown));
        assert_eq!(lines(&b.frame(&frame)), NOTHING, "nothing while down");
    }
    // An overflow while heard loses only our copy.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.placed(1, 100);
    assert_eq!(
        lines(&b.step(Input::Blind(GapCause::LocalOverflow))),
        ["lost LocalOverflow", "raised 1 AccountBlind", "regained"]
    );
    // A closed feed is down for good.
    let mut b = Bench::heard();
    assert_eq!(lines(&b.step(Input::Closed)), ["lost SocketClosed"]);
    assert_eq!(lines(&b.frame(&fixtures::subscribed())), NOTHING);
    assert_eq!(b.ready(), Err(Unready::StreamDown));
}

#[test]
fn an_order_placed_and_never_mentioned_goes_into_doubt_when_the_shell_says_so() {
    // The owner's decision 6. Break: raise it whether or not the stream has
    // mentioned the order.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.placed(1, 100);
    assert_eq!(
        lines(&b.step(Input::Unheard {
            id: ClientOrderId(1)
        })),
        ["raised 1 FillsMissed"]
    );
    assert_eq!(b.ready(), Err(Unready::InDoubt));
    assert_eq!(
        lines(&b.frame(&fixtures::new_order(&ours(1), 100, "0.001"))),
        ["cleared 1", "accepted 1 100"]
    );

    let mut b = Bench::heard();
    b.submit(1, "0.001");
    b.placed(1, 100);
    b.frame(&fixtures::new_order(&ours(1), 100, "0.001"));
    assert_eq!(
        lines(&b.step(Input::Unheard {
            id: ClientOrderId(1)
        })),
        NOTHING
    );
    assert_eq!(b.ready(), Ok(()));
}

#[test]
fn what_the_shell_did_not_send_is_never_landed_or_a_refused_cancel() {
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    assert_eq!(
        lines(&b.step(Input::Unsent(Unsent::Order {
            id: ClientOrderId(1),
            why: "would not encode".to_owned(),
        }))),
        ["never_landed 1"]
    );
    assert_eq!(
        lines(&b.step(Input::Unsent(Unsent::Cancel {
            id: ClientOrderId(1),
            why: "the order has ended".to_owned(),
        }))),
        ["cancel_refused 1 not_sent"]
    );
    let refused = b.step(Input::CancelReply {
        id: ClientOrderId(1),
        reply: CancelReply::Unknown {
            why: WhyUnknown::TimedOut,
        },
    });
    assert_eq!(lines(&refused), ["cancel_refused 1 unknown"]);
    assert_eq!(b.ready(), Ok(()), "an unknown cancel puts nothing in doubt");
}

/// The reason and the `why` of the one rejection in `out`.
fn rejection(out: &[VenueReport]) -> (RejectReason, Option<RejectedWhy>) {
    match out {
        [VenueReport::Order(OrderReport::Rejected { reason, why, .. })] => (*reason, why.clone()),
        other => panic!("not one rejection: {other:?}"),
    }
}

#[test]
fn a_rejection_keeps_the_venues_words_the_filter_it_named_and_why_nothing_left() {
    // Criterion B reads `cited` and `breach` off the `rejected` line, and
    // `lines` above flattens both away. Written out here by hand rather than
    // through `orders.rs`'s own mapping, which would make the test agree with
    // whatever the mapping says. Break: hand the engine `why: None`, or a
    // `Cited` of zeros, or `RejectReason::Venue` for the refusal.
    let mut b = Bench::heard();
    b.submit(1, "0.001");
    assert_eq!(
        rejection(&b.refused(1)),
        (
            RejectReason::Malformed,
            Some(RejectedWhy::Declined {
                cited: Cited {
                    http_status: Some(400),
                    code: -1013,
                    msg: VenueText::new("Filter failure: NOTIONAL"),
                },
                breach: Some(FilterBreach::MinNotional),
            })
        )
    );
    b.submit(2, "0.001");
    let out = b.step(Input::Placement {
        id: ClientOrderId(2),
        placement: Placement::NotSent {
            why: "connection refused".to_owned(),
        },
    });
    assert_eq!(
        rejection(&out),
        (
            RejectReason::Venue,
            Some(RejectedWhy::NeverLanded(Landing::NotSent {
                why: VenueText::new("connection refused"),
            }))
        )
    );
    b.submit(3, "0.001");
    let out = b.step(Input::Unsent(Unsent::Order {
        id: ClientOrderId(3),
        why: "would not encode".to_owned(),
    }));
    assert_eq!(
        rejection(&out),
        (
            RejectReason::Venue,
            Some(RejectedWhy::NeverLanded(Landing::NotSent {
                why: VenueText::new("would not encode"),
            }))
        )
    );
}

#[test]
fn readiness_names_the_longest_lasting_reason_first() {
    // One case per pair of the six, each with exactly those two holding.
    // Break: swap any two checks in `ready`.
    let order = [
        Unready::NoFilters,
        Unready::Diverged,
        Unready::Unchecked,
        Unready::StreamDown,
        Unready::InDoubt,
        Unready::QueueFull,
    ];
    let mut pairs = 0;
    for (i, &first) in order.iter().enumerate() {
        for &second in &order[i + 1..] {
            let mut b = Bench::heard();
            let mut instrument = b.instrument;
            let mut queue = Queue::Room;
            for reason in [first, second] {
                match reason {
                    Unready::NoFilters => instrument = b.other,
                    Unready::Diverged => {
                        b.frame(&fixtures::trade("web_1", 900, "1", "1", "1", 77));
                    }
                    Unready::Unchecked => {
                        b.step(Input::Check(Check::Failed));
                    }
                    Unready::StreamDown => {
                        b.frame(&fixtures::server_shutdown());
                    }
                    Unready::InDoubt => {
                        b.submit(9, "0.001");
                        b.unknown(9, WhyUnknown::TimedOut);
                    }
                    Unready::QueueFull => queue = Queue::Full,
                    Unready::Untagged | Unready::NoBaseline => unreachable!("never answered"),
                }
            }
            assert_eq!(
                b.merge.ready(instrument, queue),
                Err(first),
                "{first:?} before {second:?}"
            );
            pairs += 1;
        }
    }
    assert_eq!(pairs, 15);

    // And each alone, so a check that never fires is caught too.
    let mut b = Bench::heard();
    assert_eq!(b.merge.ready(b.other, Queue::Room), Err(Unready::NoFilters));
    assert_eq!(
        b.merge.ready(b.instrument, Queue::Full),
        Err(Unready::QueueFull)
    );
    b.step(Input::Check(Check::Failed));
    assert_eq!(b.ready(), Err(Unready::Unchecked));
    b.step(Input::Check(Check::Passed));
    assert_eq!(b.ready(), Ok(()));
    assert_eq!(Bench::down().ready(), Err(Unready::StreamDown));
}

#[test]
fn every_output_is_stamped_with_its_inputs_stamp() {
    // Break: stamp from anything else, a clock or an earlier input.
    let mut b = Bench::down();
    let mut stamped = 0;
    let mut check = |b: &mut Bench, out: Vec<VenueReport>| {
        let at = Ts::from_nanos(b.at);
        for report in &out {
            assert_eq!(report.ts(), at, "{report:?}");
            stamped += 1;
        }
    };
    let out = b.frame(&fixtures::subscribed());
    check(&mut b, out);
    b.submit(1, "0.002");
    b.submit(2, "0.001");
    let out = b.step(Input::Blind(GapCause::LocalOverflow));
    check(&mut b, out);
    let out = b.placed(1, 100);
    check(&mut b, out);
    let out = b.unknown(2, WhyUnknown::TimedOut);
    check(&mut b, out);
    let out = b.frame(&fixtures::trade(
        &ours(1),
        100,
        "0.002",
        "0.001",
        "0.001",
        7,
    ));
    check(&mut b, out);
    let out = b.frame(&fixtures::ended(
        &ours(1),
        100,
        "0.002",
        "0.001",
        EndedBy::Expired,
    ));
    check(&mut b, out);
    let out = b.frame(&fixtures::trade(
        &ours(1),
        100,
        "0.002",
        "0.001",
        "0.002",
        8,
    ));
    check(&mut b, out);
    let out = b.frame(&fixtures::new_order(&ours(2), 101, "0.001"));
    check(&mut b, out);
    let out = b.step(Input::CancelReply {
        id: ClientOrderId(2),
        reply: CancelReply::NotSent {
            why: "stopping".to_owned(),
        },
    });
    check(&mut b, out);
    let out = b.frame(&fixtures::trade_with(
        "web_1", 900, "1", "1", "1", 77, 78, "BNB",
    ));
    check(&mut b, out);
    let out = b.frame(b"not json");
    check(&mut b, out);
    let out = b.frame(&fixtures::server_shutdown());
    check(&mut b, out);
    // Accepted, filled, cancelled, placed, raised, cleared, lost, regained,
    // anomalies, a foreign line and a refused cancel among them.
    assert!(stamped >= 15, "{stamped}");
}
