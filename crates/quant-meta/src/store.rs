//! The Postgres client and the six statements this tier needs.

use core::fmt;

use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

use crate::rows::{SegmentRow, SessionClose, SessionRow};
use crate::schema;

/// Environment variable the recorder reads to find the database.
///
/// Absent means "no metadata tier", not "fail to start". See the crate docs.
pub const DATABASE_URL_ENV: &str = "QUANT_DATABASE_URL";

/// Something went wrong talking to the metadata database.
#[derive(Debug)]
pub enum MetaError {
    Db(tokio_postgres::Error),
    /// A value could not be represented in its column type.
    OutOfRange(&'static str),
}

impl fmt::Display for MetaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Db(e) => write!(f, "metadata database error: {e}"),
            Self::OutOfRange(what) => write!(f, "{what} is out of range for its column"),
        }
    }
}

impl std::error::Error for MetaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Db(e) => Some(e),
            Self::OutOfRange(_) => None,
        }
    }
}

impl From<tokio_postgres::Error> for MetaError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Db(e)
    }
}

/// Connect, and spawn the task that drives the connection.
///
/// `tokio_postgres` splits the client from the connection: the returned future
/// must be polled for anything to happen, so it is spawned here and its handle
/// returned rather than detached. A caller that drops the handle without aborting
/// simply leaves it running until the client is dropped, which is the behaviour we
/// want at shutdown.
///
/// `NoTls`, which is correct for a database on localhost and not correct for one
/// that is not. Moving this off the local machine means adding a TLS connector
/// here, and it should be a deliberate change rather than a default that quietly
/// sends credentials in the clear.
pub async fn connect(url: &str) -> Result<(Meta, JoinHandle<()>), MetaError> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
    let driver = tokio::spawn(async move {
        if let Err(e) = connection.await {
            // Not fatal to the recorder: capture continues without an index.
            tracing::error!(error = %e, "metadata connection lost");
        }
    });
    Ok((Meta { client }, driver))
}

/// A connection to the metadata tier.
#[derive(Debug)]
pub struct Meta {
    client: Client,
}

impl Meta {
    #[must_use]
    pub const fn new(client: Client) -> Self {
        Self { client }
    }

    /// Bring the schema up to date. Returns how many migrations ran.
    pub async fn migrate(&mut self) -> Result<usize, MetaError> {
        schema::migrate(&mut self.client).await
    }

    /// Record a session as started.
    ///
    /// `ON CONFLICT DO NOTHING` so that a retry after a transient failure is a
    /// no-op rather than an error. The row is written *before* recording begins,
    /// so a killed recorder leaves a `running` row -- which is how it is found.
    pub async fn open_session(&self, row: &SessionRow) -> Result<(), MetaError> {
        self.client
            .execute(
                "INSERT INTO capture_sessions (id, exchange, symbol, started_at, status)
                 VALUES ($1, $2, $3, $4, 'running')
                 ON CONFLICT (id) DO NOTHING",
                &[&row.id, &row.exchange, &row.symbol, &row.started_at],
            )
            .await?;
        Ok(())
    }

    /// Record a sealed capture file.
    ///
    /// An upsert on the natural key `(session_id, capture_date, part)`, which *is*
    /// the identity of a file in the §5 layout. A segment is never legitimately
    /// sealed twice -- `create_new` prevents reusing a target -- so the conflict
    /// path exists purely to make a retry idempotent rather than a duplicate.
    pub async fn record_segment(&self, row: &SegmentRow) -> Result<(), MetaError> {
        self.client
            .execute(
                "INSERT INTO capture_segments (
                     session_id, capture_date, part, path, sealed_at,
                     frames, blocks, file_bytes, frame_bytes,
                     first_ingest_seq, last_ingest_seq
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                 ON CONFLICT (session_id, capture_date, part) DO UPDATE SET
                     path = EXCLUDED.path,
                     sealed_at = EXCLUDED.sealed_at,
                     frames = EXCLUDED.frames,
                     blocks = EXCLUDED.blocks,
                     file_bytes = EXCLUDED.file_bytes,
                     frame_bytes = EXCLUDED.frame_bytes,
                     first_ingest_seq = EXCLUDED.first_ingest_seq,
                     last_ingest_seq = EXCLUDED.last_ingest_seq",
                &[
                    &row.session_id,
                    &row.capture_date,
                    &row.part,
                    &row.path,
                    &row.sealed_at,
                    &row.frames,
                    &row.blocks,
                    &row.file_bytes,
                    &row.frame_bytes,
                    &row.first_ingest_seq,
                    &row.last_ingest_seq,
                ],
            )
            .await?;
        Ok(())
    }

    /// Mark a session finished and record what ingress saw.
    pub async fn close_session(&self, id: Uuid, close: &SessionClose) -> Result<(), MetaError> {
        self.client
            .execute(
                "UPDATE capture_sessions SET
                     ended_at = $2, status = $3, messages = $4, venue_bytes = $5,
                     dropped = $6, gaps_recorded = $7, gaps_abandoned = $8,
                     backdated = $9, note = $10
                 WHERE id = $1",
                &[
                    &id,
                    &close.ended_at,
                    &close.status.as_str(),
                    &close.messages,
                    &close.venue_bytes,
                    &close.dropped,
                    &close.gaps_recorded,
                    &close.gaps_abandoned,
                    &close.backdated,
                    &close.note,
                ],
            )
            .await?;
        Ok(())
    }

    /// Sessions still marked `running`.
    ///
    /// The payoff for keeping this table: after an unclean shutdown these are the
    /// captures that never closed, and cross-checking them against files with no
    /// trailer tells you whether the recorder died or the database write did.
    pub async fn running_sessions(&self) -> Result<Vec<(Uuid, String, String)>, MetaError> {
        Ok(self
            .client
            .query(
                "SELECT id, exchange, symbol FROM capture_sessions
                 WHERE status = 'running' ORDER BY started_at DESC",
                &[],
            )
            .await?
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect())
    }

    /// Segments recorded for a session, in capture order.
    pub async fn segments_for(&self, session: Uuid) -> Result<Vec<(String, i64, i64)>, MetaError> {
        Ok(self
            .client
            .query(
                "SELECT path, frames, file_bytes FROM capture_segments
                 WHERE session_id = $1 ORDER BY capture_date, part",
                &[&session],
            )
            .await?
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect())
    }
}
