//! Record one Binance symbol to a raw capture file.
//!
//! ```text
//! record [SYMBOL] [DATA_ROOT] [SECONDS]
//! record BTCUSDT data        # until ctrl-c
//! record BTCUSDT data 30     # stop after 30 seconds
//! ```
//!
//! Argument parsing is deliberately primitive. Real configuration -- symbol lists,
//! database connection, rotation policy -- arrives with M1.c, and inventing a
//! config format now would mean designing it before knowing what the session
//! lifecycle needs from it.
//!
//! # Shutdown
//!
//! Ctrl-C -- or the optional duration limit -- races the connection loop. When
//! either wins, the connection future is dropped, which releases the borrow on
//! `Ingress`, which lets it be dropped, which closes the channel, which makes the
//! writer thread seal the file with a trailer. So a deliberate shutdown produces a
//! file that says it is complete, and a `SIGKILL` produces one that does not --
//! distinguishable later from the bytes alone, with no external bookkeeping.
//!
//! The duration limit exists because that distinction has to be *testable*.
//! Delivering a real console interrupt to a child process is awkward on Windows,
//! and a clean-shutdown path that is never exercised end to end is a clean-shutdown
//! path that does not work. It is also useful in its own right for bounded capture.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use quant_binance::{connection, ConnectionPolicy, StreamSpec, SPOT_WS};
use quant_core::event::GapCause;
use quant_core::instrument::Exchange;
use quant_core::time::{Clock, SystemClock};
use quant_recorder::{
    channel, format_session_id, run_writer, CaptureSession, FileStore, Ingress,
    DEFAULT_CHANNEL_CAPACITY, DEFAULT_FLUSH_INTERVAL,
};
use quant_storage::WriterOptions;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Installed explicitly rather than left to rustls' feature-based detection.
    // That detection panics on first use if it cannot decide, so a dependency
    // change elsewhere in the tree would surface as a crash at the first TLS
    // handshake -- in production, looking exactly like a venue outage. An error
    // here means something already installed a provider, which is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();

    match record().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "recorder stopped");
            ExitCode::FAILURE
        }
    }
}

async fn record() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let symbol = args.next().unwrap_or_else(|| "BTCUSDT".to_owned());
    let root = PathBuf::from(args.next().unwrap_or_else(|| "data".to_owned()));
    let limit = args
        .next()
        .map(|s| s.parse::<u64>())
        .transpose()?
        .map(Duration::from_secs);

    // The one and only clock. Everything that stamps a timestamp takes this, so
    // that the same code can be driven by a replayed event stream later.
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let started = clock.now();

    let session_id = *uuid::Uuid::new_v4().as_bytes();

    // The session owns file creation and day rolling. No file is opened here: the
    // first record decides which day it belongs to, so a recorder started at
    // 23:59:59.9 does not create a file it will never write to.
    let mut session = CaptureSession::new(
        FileStore::new(&root),
        Exchange::Binance,
        symbol.clone(),
        session_id,
        Arc::clone(&clock),
        WriterOptions::default(),
    );

    let (tx, rx) = channel(DEFAULT_CHANNEL_CAPACITY);
    // A plain thread, not spawn_blocking: this runs for the life of the process,
    // and parking a tokio blocking-pool slot forever is not what that pool is for.
    //
    // The session moves in and comes back out, because sealing the final segment
    // has to happen after the loop returns and must be able to report an error.
    let writer_thread = thread::Builder::new()
        .name(format!("capture-writer-{symbol}"))
        .spawn(move || {
            let outcome = run_writer(&rx, &mut session, DEFAULT_FLUSH_INTERVAL);
            (outcome, session)
        })?;

    let mut ingress = Ingress::new(tx, Arc::clone(&clock));

    // First record in every file. Without it, a capture that resumes after a crash
    // would silently abut the previous one and look like continuous coverage.
    ingress.record_gap(GapCause::RecorderRestart)?;

    let spec = StreamSpec::market_data(&symbol);
    let policy = ConnectionPolicy::default();
    // The session id, not a path: with day rolling a run can produce several
    // files, and the session is what identifies them all. Each is logged as it
    // is sealed.
    info!(
        %symbol,
        root = %root.display(),
        session = %format_session_id(&session_id),
        "recording; ctrl-c to stop"
    );

    let outcome = tokio::select! {
        result = connection::run(&spec, SPOT_WS, &mut ingress, &policy) => {
            // Only returns on a fatal condition -- normal disconnects are gaps.
            result.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
        }
        result = tokio::signal::ctrl_c() => {
            result?;
            info!("shutdown requested");
            Ok(())
        }
        () = until(limit) => {
            info!(?limit, "duration limit reached");
            Ok(())
        }
    };

    let stats = ingress.stats();
    // Closes the channel, which is how the writer knows to seal the trailer.
    drop(ingress);

    let (written, session) = writer_thread
        .join()
        .map_err(|_| "capture writer thread panicked")?;
    let written = written?;

    let backdated = session.backdated_records();
    let rolls = session.rolls();
    // Seals the final segment with its trailer. This is the step that makes a
    // clean shutdown distinguishable from a kill.
    let (segments, store) = session.finish()?;

    for segment in &segments {
        info!(
            path = %segment.target.file(store.root()).display(),
            date = %segment.target.date,
            frames = segment.stats.frames,
            blocks = segment.stats.blocks,
            file_bytes = segment.stats.file_bytes,
            venue_bytes = segment.stats.frame_bytes,
            ingest_seq = ?(segment.first_ingest_seq, segment.last_ingest_seq),
            "segment sealed"
        );
    }

    info!(
        messages = stats.messages,
        venue_bytes = stats.bytes,
        dropped = stats.dropped,
        gaps = stats.gaps_recorded,
        gaps_abandoned = stats.gaps_abandoned,
        segments = segments.len(),
        day_rolls = rolls,
        records = written.records,
        timed_flushes = written.timed_flushes,
        seconds = clock.now().delta_nanos(started) / 1_000_000_000,
        "capture closed"
    );
    if backdated > 0 {
        // The host clock stepped backwards across a midnight boundary. Nothing was
        // lost, but some records are filed under the following day -- and a machine
        // whose clock jumps is a machine whose latency measurements are suspect.
        error!(
            backdated,
            "records arrived stamped before the open segment's day: check the host clock"
        );
    }
    if stats.dropped > 0 {
        // Not an error -- it is recorded honestly and the hole proves how much --
        // but it means the channel or the disk needs sizing, so it must not be
        // buried among the numbers above.
        error!(
            dropped = stats.dropped,
            "messages were dropped: recorder could not keep up"
        );
    }
    // Durability is FileStore's job now: it fsyncs each segment as it is sealed,
    // once per file rather than per block.
    outcome
}

/// Completes after `limit`, or never if there is no limit.
///
/// `pending()` rather than an `Option` branch in the `select!`, so the no-limit
/// case is a branch that simply never fires instead of a disabled arm.
async fn until(limit: Option<Duration>) {
    match limit {
        Some(d) => tokio::time::sleep(d).await,
        None => std::future::pending().await,
    }
}
