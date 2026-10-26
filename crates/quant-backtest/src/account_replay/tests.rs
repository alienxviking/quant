//! The replay's own pieces. The replay itself is checked end to end against a
//! rehearsal's journal and capture in `quant-explain`'s `tests/live_run.rs`.

use quant_binance::orders::{
    CancelReply, FilterName, Refusal, RefusalCause, VenueError, WhyUnknown,
};
use quant_core::execution::ClientOrderId;
use quant_core::report::{AccountCause, AccountChange};
use quant_core::time::Ts;
use quant_engine::journal::JournalEntry;

use super::{cancel_reply, differ, refusal, venue_produced, why_unknown};

#[test]
fn every_unknown_answer_read_back_off_its_line_is_the_same_answer_to_the_machine() {
    // The machine reads an unknown answer's cause and words and nothing
    // else, so a line's `doubt raised` read back must give both exactly, for
    // every variant there is.
    let every = [
        WhyUnknown::ServerError {
            http_status: 503,
            said: Some(VenueError {
                code: -1001,
                msg: "Internal error".to_owned(),
            }),
        },
        WhyUnknown::ServerError {
            http_status: 502,
            said: None,
        },
        WhyUnknown::Unsettled {
            http_status: 400,
            code: -1007,
            msg: "Timeout waiting for response from backend server.".to_owned(),
        },
        WhyUnknown::IdInUse {
            id: "q-r1-7".to_owned(),
            http_status: 400,
            code: -2010,
            msg: "Duplicate order sent.".to_owned(),
        },
        WhyUnknown::UnreadableBody {
            http_status: 200,
            detail: "not the acknowledgement".to_owned(),
        },
        WhyUnknown::OtherOrder {
            sent: "q-r1-7".to_owned(),
            echoed: "q-r1-8".to_owned(),
        },
        WhyUnknown::UnexpectedStatus { http_status: 409 },
        WhyUnknown::TimedOut,
        WhyUnknown::Interrupted("connection reset".to_owned()),
    ];
    for why in every {
        let back = why_unknown(why.cause(), why.said().as_ref()).expect("a REST cause");
        assert_eq!(
            (back.cause(), back.said()),
            (why.cause(), why.said()),
            "{why:?}"
        );
    }
}

#[test]
fn every_refusal_read_back_off_its_line_gives_the_same_rejection() {
    // A rejection line keeps the reason, the venue's words and the filter it
    // named; a refusal read back from it must make that same line.
    let causes = [
        RefusalCause::Filter(FilterName::PriceFilter),
        RefusalCause::Filter(FilterName::LotSize),
        RefusalCause::Filter(FilterName::MinNotional),
        RefusalCause::Filter(FilterName::Notional),
        RefusalCause::Filter(FilterName::MarketLotSize),
        RefusalCause::InsufficientBalance,
        RefusalCause::Declined,
        RefusalCause::Timestamp,
        RefusalCause::Credentials,
        RefusalCause::Malformed,
        RefusalCause::RateLimited,
    ];
    for cause in causes {
        let original = Refusal {
            http_status: 400,
            code: -1013,
            msg: "Filter failure: NOTIONAL".to_owned(),
            cause,
        };
        let back = refusal(cause.reject_reason(), &original.cited(), cause.breach())
            .expect("a REST refusal");
        assert_eq!(
            (
                back.cause.reject_reason(),
                back.cause.breach(),
                back.cited()
            ),
            (cause.reject_reason(), cause.breach(), original.cited()),
            "{cause:?}"
        );
    }
}

#[test]
fn every_refused_cancel_read_back_off_its_line_is_the_same_reply() {
    let replies = [
        CancelReply::NotOpen {
            http_status: 400,
            code: -2011,
            msg: "Unknown order sent.".to_owned(),
        },
        CancelReply::Refused(Refusal {
            http_status: 400,
            code: -2010,
            msg: "refused".to_owned(),
            cause: RefusalCause::Declined,
        }),
        CancelReply::Unknown {
            why: WhyUnknown::TimedOut,
        },
        CancelReply::NotSent {
            why: "stopping".to_owned(),
        },
    ];
    for reply in replies {
        let line = reply.refusal().expect("a refusal");
        let back = cancel_reply(&line).expect("read back");
        assert_eq!(back.refusal(), Some(line), "{reply:?}");
    }
}

#[test]
fn a_line_missing_or_made_twice_is_named_once_per_copy() {
    let accepted = |id: u64| JournalEntry::Accepted {
        at: Ts::from_nanos(i64::try_from(id).expect("small")),
        client_order_id: ClientOrderId(id),
        venue_order_id: None,
    };
    let lost = JournalEntry::Account {
        at: Ts::from_nanos(9),
        change: AccountChange::Lost {
            cause: AccountCause::SocketClosed,
        },
    };
    let want = [accepted(1), accepted(2), lost.clone()];
    let got = [accepted(1), accepted(3), accepted(3), lost];
    let (missing, extra) = differ(&want, &got);
    assert_eq!(missing, [accepted(2)]);
    assert_eq!(extra, [accepted(3), accepted(3)]);
    // A line the journal holds twice and the replay makes once is missing
    // once: two copies are two facts.
    let (missing, extra) = differ(&[accepted(1), accepted(1)], &[accepted(1)]);
    assert_eq!((missing, extra), (vec![accepted(1)], Vec::new()));
    // A line the replay may make and the comparison must not count.
    let (missing, extra) = differ(
        &[],
        &[JournalEntry::Checkpoint {
            at: Ts::from_nanos(1),
            cash: "1".parse().expect("cash"),
            realized: "0".parse().expect("realized"),
            fees: "0".parse().expect("fees"),
            fills: 5,
        }],
    );
    assert_eq!((missing, extra), (Vec::new(), Vec::new()));
}

#[test]
fn the_lines_compared_are_the_venue_s_and_not_the_run_s() {
    assert!(venue_produced(&JournalEntry::Account {
        at: Ts::from_nanos(1),
        change: AccountChange::Regained,
    }));
    assert!(!venue_produced(&JournalEntry::CancelRequested {
        at: Ts::from_nanos(1),
        client_order_id: ClientOrderId(1),
    }));
    assert!(!venue_produced(&JournalEntry::Sighted {
        at: Ts::from_nanos(1)
    }));
}
