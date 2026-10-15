//! What *we* were doing at an instant, from the journal.
//!
//! The counterpart to the market side. Where that reconstructs the venue from
//! raw, this reconstructs our own position and P&L from the journal — the only
//! durable record of what we actually did, and deliberately a file rather than
//! the metadata tier, because M1.c2 made Postgres best-effort and optional and a
//! position that must survive a restart cannot depend on something optional.
//!
//! # It calls the engine's own accounting, and does not reimplement it
//!
//! `journal::replay` folds the fill lines through the real `Portfolio`, so
//! average cost, realized P&L and fees are computed here by exactly the code
//! that computed them live. The alternative — a `jq` fold, or a second
//! accounting written here — is the thing this project has refused four times,
//! and it would be worse than usual: `Portfolio` does average-cost arithmetic in
//! fixed point, so a reimplementation would agree right up until the position
//! changed sign.

use std::path::{Path, PathBuf};

use quant_core::execution::FeeAsset;
use quant_core::fixed::{Notional, Px};
use quant_core::instrument::{InstrumentId, InstrumentRegistry};
use quant_core::time::Ts;
use quant_engine::journal::{self, JournalEntry};
use quant_engine::Portfolio;

/// Our own state at an instant, as the journal tells it.
#[derive(Debug)]
pub struct OurState {
    /// The journal read.
    pub journal: PathBuf,
    /// Entries at or before the instant. The whole file is read, then truncated
    /// to the prefix that had happened — a journal is hundreds of lines a week,
    /// so seeking would buy nothing and cost the torn-tail tolerance `read`
    /// already has.
    pub entries_considered: usize,
    /// `None` when the journal does not exist or holds nothing yet.
    pub portfolio: Option<Portfolio>,
    /// The most recent fill at or before the instant, and the next one after.
    pub last_fill: Option<FillAt>,
    pub next_fill: Option<FillAt>,
    /// Set when the session had not started, or had already stopped.
    pub outside_session: Option<String>,
    /// A checkpoint at exactly this instant, if there is one — what the engine
    /// *believed* at the time, as opposed to what the file replays to. The pair
    /// is what makes the agreement check independent rather than vacuous.
    pub checkpoint_here: Option<Checkpoint>,
}

/// A fill, and which side of the instant it falls.
#[derive(Debug, Clone)]
pub struct FillAt {
    pub at: Ts,
    pub side: quant_core::event::Side,
    pub px: Px,
    pub qty: quant_core::fixed::Qty,
    /// In whichever asset `fee_asset` names, so the two are one fact.
    pub fee: Notional,
    /// Which asset the venue took `fee` in.
    ///
    /// A base-asset fee is a quantity of the base -- 0.000001 BTC, not 0.000001
    /// USDT. This was dropped on the way out of the journal, so `explain`
    /// printed such a fee bare: a millionth of a dollar to anyone reading it,
    /// understating the cost by a factor of the price.
    pub fee_asset: FeeAsset,
    /// The engine's own `ClientOrderId` for the order this fill belongs to.
    ///
    /// It was a *fill ordinal* until M7.5.a, because `RunObserver::on_fill` was
    /// never handed the real id and `paper.rs` wrote `ClientOrderId(self.fills)`
    /// instead. The engine now passes `execution.client_order_id()` through, so
    /// this is the real thing and pasting it into a `jq` filter over the journal
    /// finds the submission it came from.
    ///
    /// The field kept the old name for one commit after the value changed, and
    /// `explain` kept printing it as `fill #n`. That is worse than either state
    /// on its own: ids skip wherever an order was refused, so a correct number
    /// under the wrong label reads as a journal that lost lines. Found by the
    /// post-M7.5 audit.
    pub client_order_id: u64,
}

/// What the engine claimed to believe, written down at the time.
#[derive(Debug, Clone, Copy)]
pub struct Checkpoint {
    pub at: Ts,
    pub cash: Notional,
    pub realized: Notional,
    pub fees: Notional,
    pub fills: u64,
}

/// Reconstruct our state at `at` from a journal file.
///
/// # Errors
///
/// Propagates an unreadable journal. A *missing* one is not an error — a run
/// that never traded has none, and reporting that as a failure would make the
/// ordinary case look broken.
pub fn ours_at(
    journal_path: &Path,
    instrument: InstrumentId,
    registry: &mut InstrumentRegistry,
    at: Ts,
) -> std::io::Result<OurState> {
    let recovered = journal::read(journal_path)?;
    let mut out = OurState {
        journal: journal_path.to_owned(),
        entries_considered: 0,
        portfolio: None,
        last_fill: None,
        next_fill: None,
        outside_session: None,
        checkpoint_here: None,
    };
    if recovered.entries.is_empty() {
        out.outside_session = Some("the journal is empty or absent".to_owned());
        return Ok(out);
    }

    // `Started.at` was hard-coded to zero until M7.5.a, so a journal could not date
    // its own beginning -- see docs/observability.md. The first *fill* is the
    // earliest instant the journal can speak for, and anything before it is
    // reported as such rather than answered with a zero position that looks
    // observed.
    let first_event = recovered.entries.iter().find_map(entry_ts);
    if let Some(first) = first_event {
        if at < first {
            out.outside_session = Some(format!(
                "before this journal's first entry at {} -- nothing had happened yet",
                first.to_rfc3339()
            ));
        }
    }
    if let Some(JournalEntry::Stopped { at: stopped, .. }) = recovered
        .entries
        .iter()
        .rev()
        .find(|e| matches!(e, JournalEntry::Stopped { .. }))
    {
        if at > *stopped {
            out.outside_session = Some(format!(
                "after the session stopped at {}",
                stopped.to_rfc3339()
            ));
        }
    }

    let prefix: Vec<JournalEntry> = recovered
        .entries
        .iter()
        .filter(|e| entry_ts(e).is_none_or(|ts| ts <= at))
        .cloned()
        .collect();
    out.entries_considered = prefix.len();
    out.portfolio = Some(journal::replay(&prefix, registry));

    for entry in &recovered.entries {
        match entry {
            JournalEntry::Filled {
                at: ts,
                side,
                px,
                qty,
                fee,
                fee_asset,
                client_order_id,
                ..
            } => {
                let fill = FillAt {
                    at: *ts,
                    side: *side,
                    px: *px,
                    qty: *qty,
                    fee: *fee,
                    fee_asset: *fee_asset,
                    client_order_id: client_order_id.0,
                };
                if *ts <= at {
                    out.last_fill = Some(fill);
                } else if out.next_fill.is_none() {
                    out.next_fill = Some(fill);
                }
            }
            JournalEntry::Checkpoint {
                at: ts,
                cash,
                realized,
                fees,
                fills,
            } if *ts == at => {
                out.checkpoint_here = Some(Checkpoint {
                    at: *ts,
                    cash: *cash,
                    realized: *realized,
                    fees: *fees,
                    fills: *fills,
                });
            }
            _ => {}
        }
    }

    let _ = instrument;
    Ok(out)
}

/// The instant an entry describes, where it has a meaningful one.
///
/// `Started` returns `None` rather than its timestamp, and the reason survived
/// M7.5 giving it a real one: filtering on it would drop the starting cash from
/// every prefix whose instant precedes the session's own start — silently
/// resetting the account to nothing, the shape of bug M5's first rehearsal hit
/// when `.resuming()` met an empty recompute. The value is now the wall clock
/// rather than a hard-coded zero, which makes it *readable*; it still must not
/// be *filtered on*.
const fn entry_ts(entry: &JournalEntry) -> Option<Ts> {
    match entry {
        // `Balances` is the wall clock too, when the venue was asked: at the
        // start of a run, before the market has dated anything. Filtered on,
        // a start-of-run line could fall after the first fills and vanish
        // from every prefix that should hold it.
        JournalEntry::Started { .. } | JournalEntry::Balances { .. } => None,
        JournalEntry::Filled { at, .. }
        | JournalEntry::Checkpoint { at, .. }
        | JournalEntry::Tripped { at, .. }
        | JournalEntry::Submitted { at, .. }
        | JournalEntry::Refused { at, .. }
        | JournalEntry::Accepted { at, .. }
        | JournalEntry::Rejected { at, .. }
        | JournalEntry::CancelRequested { at, .. }
        | JournalEntry::Cancelled { at, .. }
        | JournalEntry::Orphaned { at, .. }
        | JournalEntry::Blind { at, .. }
        | JournalEntry::Note { at, .. }
        | JournalEntry::Stopped { at, .. }
        // What a live venue says, schema 3: each stamped when we heard it, on
        // the clock every other entry here is stamped on.
        | JournalEntry::Placed { at, .. }
        | JournalEntry::NeverLanded { at, .. }
        | JournalEntry::Doubt { at, .. }
        | JournalEntry::CancelRefused { at, .. }
        | JournalEntry::Foreign { at, .. }
        | JournalEntry::Account { at, .. }
        | JournalEntry::Anomaly { at, .. }
        | JournalEntry::Stray { at, .. }
        | JournalEntry::Sighted { at } => Some(*at),
    }
}

#[cfg(test)]
mod tests {
    use super::ours_at;
    use quant_core::event::Side;
    use quant_core::execution::{ClientOrderId, FeeAsset};
    use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};
    use quant_core::time::Ts;
    use quant_engine::journal::{InstrumentKey, Journal, JournalEntry, SCHEMA};

    fn filled(at: i64, id: u64, fee: &str, fee_asset: FeeAsset) -> JournalEntry {
        JournalEntry::Filled {
            at: Ts::from_nanos(at),
            client_order_id: ClientOrderId(id),
            instrument: InstrumentKey {
                exchange: Exchange::Binance,
                symbol: "BTCUSDT".to_owned(),
            },
            side: Side::Buy,
            px: "76650".parse().expect("px"),
            qty: "0.001".parse().expect("qty"),
            fee: fee.parse().expect("fee"),
            is_maker: false,
            fee_asset,
            remaining: None,
            venue_order_id: None,
            trade_id: None,
        }
    }

    #[test]
    fn a_balances_line_does_not_date_the_session() {
        // `balances` is stamped on the wall clock when the venue was asked, as
        // `started` is, not on the market's clock every other line uses. Read
        // as the journal's first instant, a start-of-run line stamped a moment
        // after the replayed market would put every query before the session
        // began.
        let path = std::env::temp_dir().join(format!(
            "quant-explain-ours-balances-{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut journal = Journal::open(&path).expect("open");
        for entry in [
            JournalEntry::Started {
                at: Ts::from_nanos(10_000),
                cash: "100".parse().expect("cash"),
                schema: SCHEMA,
                run: Some("r1".to_owned()),
            },
            JournalEntry::Balances {
                at: Ts::from_nanos(10_001),
                base_asset: "BTC".to_owned(),
                base_free: "0.05".parse().expect("qty"),
                base_locked: "0.001".parse().expect("qty"),
                quote_asset: "USDT".to_owned(),
                quote_free: "92.5".parse().expect("notional"),
                quote_locked: "7.25".parse().expect("notional"),
            },
            filled(1_000, 1, "0.07665", FeeAsset::Quote),
        ] {
            journal.append(&entry).expect("append");
        }
        drop(journal);

        let mut registry = InstrumentRegistry::new();
        let instrument = registry.register(InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: "0.01".parse().expect("tick"),
            lot_size: "0.00001".parse().expect("lot"),
            min_notional: "5".parse().expect("notional"),
        });
        let ours = ours_at(&path, instrument, &mut registry, Ts::from_nanos(2_000));
        let _ = std::fs::remove_file(&path);
        let ours = ours.expect("readable");
        assert_eq!(ours.outside_session, None, "the fill dates the session");
        assert!(ours.last_fill.is_some());
    }

    #[test]
    fn a_fill_is_reported_with_the_asset_its_fee_was_taken_in() {
        // A base-asset fee is a quantity of the base: 0.000001 BTC, not
        // 0.000001 USDT. Without the asset, `explain` printed it bare, which
        // reads as a millionth of a dollar and understates the cost by a
        // factor of the price. One fill either side of the instant, one in
        // each asset, so a reader that assumed either one is caught.
        let path = std::env::temp_dir().join(format!(
            "quant-explain-ours-fee-asset-{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut journal = Journal::open(&path).expect("open");
        for entry in [
            JournalEntry::Started {
                at: Ts::from_nanos(0),
                cash: "100".parse().expect("cash"),
                schema: SCHEMA,
                run: None,
            },
            filled(1_000, 1, "0.000001", FeeAsset::Base),
            filled(3_000, 2, "0.07665", FeeAsset::Quote),
        ] {
            journal.append(&entry).expect("append");
        }
        drop(journal);

        let mut registry = InstrumentRegistry::new();
        let instrument = registry.register(InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: "0.01".parse().expect("tick"),
            lot_size: "0.00001".parse().expect("lot"),
            min_notional: "5".parse().expect("notional"),
        });
        let ours = ours_at(&path, instrument, &mut registry, Ts::from_nanos(2_000));
        let _ = std::fs::remove_file(&path);
        let ours = ours.expect("readable");

        let last = ours.last_fill.expect("a fill before the instant");
        assert_eq!(last.fee_asset, FeeAsset::Base, "taken in what was bought");
        let next = ours.next_fill.expect("a fill after the instant");
        assert_eq!(next.fee_asset, FeeAsset::Quote);
    }
}
