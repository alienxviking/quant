//! Versioned schema migrations.
//!
//! Numbered SQL files applied in order, recorded in `schema_migrations`.
//!
//! Deliberately not `CREATE TABLE IF NOT EXISTS` sprinkled at startup. That works
//! exactly once -- the first time the schema needs to *change*, there is no record
//! of what any given database already has, and the only safe options are to
//! inspect it by hand or to drop it. `docs/data-contract.md` §6 requires a written
//! migration path for the raw tier; the metadata tier deserves the same
//! discipline, and at forty lines there is no reason not to.
//!
//! Each migration runs in its own transaction, so a failure leaves the database at
//! a known version rather than half-way through one.

use tokio_postgres::Client;

use crate::store::MetaError;

/// One schema step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Migration {
    pub version: i32,
    pub name: &'static str,
    pub sql: &'static str,
}

/// Every migration this build knows, in order.
///
/// Append only. Editing a migration that has already been applied somewhere makes
/// two databases with the same recorded version structurally different, which is
/// the failure mode versioning exists to prevent.
pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "capture",
    sql: include_str!("../migrations/0001_capture.sql"),
}];

const BOOTSTRAP: &str = "
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    integer     PRIMARY KEY,
    name       text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)";

/// Advisory lock key held while migrating.
///
/// An arbitrary but fixed number; only its stability matters, since every process
/// that migrates this schema must choose the same one.
const MIGRATION_LOCK: i64 = 0x5175_616E_7401;

/// Apply any migrations this database has not seen. Returns how many ran.
///
/// Serialized across processes with a Postgres advisory lock. Without it,
/// concurrent starters interleave between "which versions are applied" and
/// "apply this one", and the loser fails with a duplicate-object error --
/// including on `CREATE TABLE IF NOT EXISTS`, which is not atomic against another
/// transaction doing the same thing.
///
/// That is not a hypothetical: two recorders launched together, or a supervisor
/// restarting several symbols at once, race exactly this way. It showed up first as
/// flaky tests, which is the friendly version of finding out.
///
/// Takes `&mut Client` because each migration needs its own transaction, so a
/// failure leaves the database at a known version rather than part-way through one.
pub async fn migrate(client: &mut Client) -> Result<usize, MetaError> {
    client
        .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK])
        .await?;
    let result = migrate_locked(client).await;
    // Released even when migrating failed. A session-level lock left held would
    // block every subsequent start until this connection closed.
    if let Err(e) = client
        .execute("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK])
        .await
    {
        tracing::error!(error = %e, "failed to release the migration lock");
    }
    result
}

async fn migrate_locked(client: &mut Client) -> Result<usize, MetaError> {
    client.batch_execute(BOOTSTRAP).await?;

    let applied: Vec<i32> = client
        .query("SELECT version FROM schema_migrations", &[])
        .await?
        .iter()
        .map(|row| row.get::<_, i32>(0))
        .collect();

    let mut ran = 0;
    for migration in MIGRATIONS {
        if applied.contains(&migration.version) {
            continue;
        }
        let tx = client.transaction().await?;
        tx.batch_execute(migration.sql).await?;
        tx.execute(
            "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
            &[&migration.version, &migration.name],
        )
        .await?;
        tx.commit().await?;
        tracing::info!(
            version = migration.version,
            name = migration.name,
            "applied migration"
        );
        ran += 1;
    }
    Ok(ran)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_unique_and_ascending() {
        // An out-of-order or duplicated version means two databases can record the
        // same version with different structure.
        for pair in MIGRATIONS.windows(2) {
            assert!(
                pair[1].version > pair[0].version,
                "migration {} does not follow {}",
                pair[1].version,
                pair[0].version
            );
        }
        assert_eq!(MIGRATIONS.first().map(|m| m.version), Some(1));
    }

    #[test]
    fn migration_sql_is_embedded_not_read_at_runtime() {
        // include_str! means a deployed binary cannot disagree with its own
        // schema, and cannot fail to find a file that was not shipped with it.
        assert!(MIGRATIONS[0].sql.contains("CREATE TABLE capture_sessions"));
        assert!(MIGRATIONS[0].sql.contains("CREATE TABLE capture_segments"));
    }
}
