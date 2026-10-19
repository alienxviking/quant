//! The account listener's capture, judged by the verifier.
//!
//! `docs/live-run.md` §4 item 6 is accepted when what the listener records
//! verifies clean. This records a session against quant-binance's fake venue
//! that goes through what a live afternoon could: account events, a dropped
//! connection, and a `serverShutdown`. Then it runs the same `discover` and
//! `verify` the `verify` binary runs, whose exit code is the report's
//! `is_clean`.

use std::path::PathBuf;
use std::time::Duration;

use quant_binance::fake_venue::{quick_policy, FakeVenue, Listening, Script};
use quant_verify::{discover, verify, Report};

/// Long enough for anything that should happen; a test that waits this long
/// has failed.
const WAIT: Duration = Duration::from_secs(10);

const EXECUTION: &str = r#"{"subscriptionId":0,"event":{"e":"executionReport","E":1499405658658,"s":"BTCUSDT","c":"q-run-1","x":"NEW"}}"#;
const BALANCE: &str = r#"{"subscriptionId":0,"event":{"e":"balanceUpdate","E":1573200697110,"a":"BTC","d":"100.00000000","T":1573200697068}}"#;

/// A data root of this test's own, removed when it ends.
struct Root(PathBuf);

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listened_account_tree_verifies_clean() {
    let root =
        Root(std::env::temp_dir().join(format!("quant-verify-account-{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    let venue = FakeVenue::start(Script::default()).await;
    let listening = Listening::start(&root.0, venue.url(), venue.credentials(), quick_policy());

    venue
        .until("a subscription", WAIT, |log| log.subscribed == 1)
        .await;
    venue.event(EXECUTION);
    venue.drop_connection();
    venue
        .until("a second subscription", WAIT, |log| log.subscribed == 2)
        .await;
    venue.event(BALANCE);
    venue.server_shutdown();
    venue
        .until("a third subscription", WAIT, |log| log.subscribed == 3)
        .await;
    venue.event(EXECUTION);
    venue
        .until("the last event read", WAIT, |log| {
            log.read_by_listener(EXECUTION)
        })
        .await;
    let heard = listening.stop(WAIT).await;
    assert_eq!(heard.stopped, None);

    let mut report = Report::default();
    let sessions = discover(&root.0, &mut report);
    for session in &sessions {
        verify(session, &mut report);
    }
    assert!(report.is_clean(), "{:#?}", report.findings());
    assert!(report.findings().is_empty(), "{:#?}", report.findings());

    let totals = report.totals;
    let recordable = venue.log().recordable().len() as u64;
    // Three subscription answers, three events and a serverShutdown.
    assert_eq!(recordable, 7);
    assert_eq!(totals.sessions, 1);
    assert_eq!(totals.account_sessions, 1);
    assert_eq!(totals.account_messages, recordable);
    // The session's opening gap and one per connection lost.
    assert_eq!(totals.gaps, 3);
    assert_eq!(totals.account_frames, recordable + 3);
    assert_eq!(totals.missing_messages, 0);
}
