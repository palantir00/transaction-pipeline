-- =====================================================================
-- RAW LAYER - transactions exactly as they arrived from the stream
--
-- Table character: APPEND-ONLY.
-- We never UPDATE or DELETE here. This is the record of what actually
-- arrived and when. Cleaning up happens in the reporting layer, so that
-- unexpected data can never block ingestion.
-- =====================================================================

CREATE TABLE IF NOT EXISTS raw_transactions (
    -- Technical key. GENERATED ALWAYS AS IDENTITY is the modern
    -- equivalent of BIGSERIAL; "ALWAYS" prevents inserting an id by hand.
    id              BIGINT        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Business key assigned by the producer.
    -- UNIQUE means the same transaction cannot land twice, even when
    -- Kafka redelivers a message after the consumer restarts
    -- (Kafka guarantees at-least-once delivery, not exactly-once).
    transaction_id  UUID          NOT NULL UNIQUE,

    user_id         TEXT          NOT NULL,

    -- NUMERIC, never FLOAT. Money is counted in decimal, exactly.
    -- (18, 2) = up to 18 significant digits, 2 of them after the point.
    amount          NUMERIC(18,2) NOT NULL,

    -- ISO 4217: PLN, EUR, USD - always exactly 3 characters
    currency        CHAR(3)       NOT NULL,

    -- ISO 3166-1 alpha-2: PL, DE, GB - always exactly 2 characters.
    -- Nullable on purpose: a missing country is no reason to drop a row.
    country         CHAR(2),

    -- EVENT TIME: when the customer actually tapped the card.
    -- The batch layer aggregates by this column.
    occurred_at     TIMESTAMPTZ   NOT NULL,

    -- PROCESSING TIME: when the row reached our database.
    -- Set automatically. Used for diagnostics: how far behind are we,
    -- is the consumer keeping up, why did yesterday's report change.
    ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- Minimal validation - only what makes a row useless anyway.
    -- We deliberately do NOT check currency against an allow-list:
    -- the first transaction in a new currency would stall the stream.
    CONSTRAINT chk_amount_nonzero CHECK (amount <> 0)
);

-- Index for the batch layer's main query:
--   WHERE occurred_at >= :day AND occurred_at < :day + 1
-- Without it Postgres would scan the whole table on every DAG run.
CREATE INDEX IF NOT EXISTS idx_raw_transactions_occurred_at
    ON raw_transactions (occurred_at);

COMMENT ON COLUMN raw_transactions.occurred_at IS
    'Event time - when the transaction happened. The batch layer aggregates by this.';
COMMENT ON COLUMN raw_transactions.ingested_at IS
    'Processing time - when the row reached the database. Used to diagnose lag.';
