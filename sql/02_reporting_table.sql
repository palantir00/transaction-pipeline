-- =====================================================================
-- REPORTING LAYER - daily aggregates produced by Airflow
--
-- Table character: OVERWRITTEN.
-- Re-running the DAG for the same day must produce the same result
-- rather than duplicates. The composite key below makes that possible.
-- =====================================================================

CREATE TABLE IF NOT EXISTS daily_user_reports (
    user_id            TEXT          NOT NULL,

    -- DATE, not TIMESTAMPTZ: the report covers a whole day, not a moment
    report_date        DATE          NOT NULL,

    -- Currency belongs in the key: 100 PLN and 100 EUR are two separate
    -- rows. Summing them into a single number would mean nothing.
    currency           CHAR(3)       NOT NULL,

    total_amount       NUMERIC(18,2) NOT NULL,
    transaction_count  INTEGER       NOT NULL,

    -- Deliberate redundancy: avg_amount = total_amount / transaction_count.
    -- Stored precomputed so that reading the report needs no arithmetic.
    avg_amount         NUMERIC(18,2) NOT NULL,

    -- When this row was computed. Re-running the DAG updates this
    -- while the figures stay the same - useful to see the report is live.
    generated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- COMPOSITE KEY - the heart of idempotency.
    -- Exactly one row per user, per day, per currency, which allows:
    --   INSERT ... ON CONFLICT (user_id, report_date, currency) DO UPDATE
    PRIMARY KEY (user_id, report_date, currency),

    CONSTRAINT chk_count_positive CHECK (transaction_count > 0)
);

-- Why a separate index when report_date is already in the primary key?
-- A composite index works like an alphabetical directory: sorted by
-- user_id first, then report_date. A query such as "give me every report
-- for August 19th" does not know user_id, so it cannot use that index and
-- would scan the whole table. Hence this second index, sorted by date.
CREATE INDEX IF NOT EXISTS idx_daily_user_reports_date
    ON daily_user_reports (report_date);
