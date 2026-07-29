-- Capture session bookkeeping.
--
-- Everything here is an *index* over the raw tier, not a second copy of it.
-- docs/data-contract.md makes raw the source of truth and Postgres metadata, so
-- every column below is either derivable from the capture files themselves plus
-- the filesystem, or is operational annotation (why a session ended) that no
-- query on market data depends on.
--
-- That is a deliberate constraint, not an accident. It is what allows the
-- recorder to keep recording when this database is unreachable, and it is what
-- lets the M1.d verifier rebuild these tables by walking the data directory. A
-- metadata row we cannot reconstruct would quietly make Postgres the source of
-- truth for something.

CREATE TABLE capture_sessions (
    id              uuid        PRIMARY KEY,
    exchange        text        NOT NULL,
    symbol          text        NOT NULL,

    -- Operational timestamps, microsecond resolution. Deliberately NOT the
    -- nanosecond `Ts` values that events carry: those live in the raw tier and
    -- are the only thing anything may order or dispatch on. Nothing may derive a
    -- trading decision from a column in this table.
    started_at      timestamptz NOT NULL,
    ended_at        timestamptz,

    status          text        NOT NULL,
    -- Ingress accounting for the whole session, written at close. `dropped`
    -- being non-zero is the signal that the channel or the disk needs sizing;
    -- the corresponding holes in ingest_seq are in the files.
    messages        bigint,
    venue_bytes     bigint,
    dropped         bigint,
    gaps_recorded   bigint,
    gaps_abandoned  bigint,
    backdated       bigint,
    note            text,

    CONSTRAINT capture_sessions_status_valid
        CHECK (status IN ('running', 'closed', 'failed')),
    -- A session that claims to be finished must say when. Enforced here rather
    -- than in application code because a half-written close is exactly what a
    -- crash produces.
    CONSTRAINT capture_sessions_ended_with_status
        CHECK ((status = 'running') = (ended_at IS NULL))
);

-- The query the operator actually makes: "what have we captured for this
-- instrument lately".
CREATE INDEX capture_sessions_instrument_time
    ON capture_sessions (exchange, symbol, started_at DESC);

-- Sessions still marked running are either live or were killed. Partial index
-- because that set is tiny and is checked often.
CREATE INDEX capture_sessions_running
    ON capture_sessions (started_at DESC)
    WHERE status = 'running';

CREATE TABLE capture_segments (
    session_id       uuid        NOT NULL
        REFERENCES capture_sessions (id) ON DELETE CASCADE,
    capture_date     date        NOT NULL,
    part             integer     NOT NULL,
    path             text        NOT NULL,
    sealed_at        timestamptz NOT NULL,

    frames           bigint      NOT NULL,
    blocks           bigint      NOT NULL,
    -- Bytes written to disk, and the uncompressed frame bytes they came from.
    -- Both, so the achieved compression ratio is a query rather than a guess.
    file_bytes       bigint      NOT NULL,
    frame_bytes      bigint      NOT NULL,

    -- Null only for a segment sealed without ever being written to, which the
    -- session avoids creating -- kept nullable so the schema cannot lie if it
    -- ever happens.
    first_ingest_seq bigint,
    last_ingest_seq  bigint,

    -- This triple is the identity of a capture file in the §5 layout, so making
    -- it the primary key means the database physically cannot hold two rows for
    -- one file. That is what lets a retry after a transient failure be an upsert
    -- instead of a duplicate.
    PRIMARY KEY (session_id, capture_date, part),

    CONSTRAINT capture_segments_seq_ordered
        CHECK (first_ingest_seq IS NULL OR last_ingest_seq >= first_ingest_seq)
);

-- "Which files cover this day", across sessions -- the verifier's entry point.
CREATE INDEX capture_segments_date ON capture_segments (capture_date);
