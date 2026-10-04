-- PulseStream M3: retry scheduling and the dead-letter state (ADR-008).
--
-- This migration is immutable once published. It upgrades an existing M2
-- database in place: no row is deleted or rewritten except to backfill
-- `available_at`, and every existing PENDING, PROCESSING, and PROCESSED row
-- stays valid and claimable exactly as before.

-- 1. Retry scheduling. A PENDING event may be claimed only once
--    `available_at <= now()`. Existing rows become available at their
--    admission time, so nothing that was claimable under M2 is delayed.
ALTER TABLE events ADD COLUMN available_at TIMESTAMPTZ;
UPDATE events SET available_at = accepted_at;
ALTER TABLE events
    ALTER COLUMN available_at SET NOT NULL,
    -- Same transaction clock as `accepted_at`'s default, so a newly admitted
    -- event has `available_at = accepted_at`.
    ALTER COLUMN available_at SET DEFAULT now();

-- 2. Safe failure metadata: a stable machine-readable code, a bounded
--    message, and when the last failure was recorded. Never stack traces,
--    payload copies, or credentials.
ALTER TABLE events
    ADD COLUMN last_failure_code    TEXT
        CHECK (last_failure_code ~ '^[A-Z][A-Z0-9_]{0,63}$'),
    ADD COLUMN last_failure_message TEXT
        CHECK (char_length(last_failure_message) <= 1024),
    ADD COLUMN last_failed_at       TIMESTAMPTZ,
    ADD COLUMN dead_lettered_at     TIMESTAMPTZ,
    ADD CONSTRAINT events_failure_metadata_consistent CHECK (
        (last_failure_code IS NULL) = (last_failed_at IS NULL)
        AND (last_failure_code IS NULL) = (last_failure_message IS NULL)
    );

-- 3. The status set gains DEAD_LETTERED.
ALTER TABLE events DROP CONSTRAINT events_status_check;
ALTER TABLE events ADD CONSTRAINT events_status_check
    CHECK (status IN ('PENDING', 'PROCESSING', 'PROCESSED', 'DEAD_LETTERED'));

-- 4. Lifecycle fields must agree with the status. The M2 rules are kept and
--    `dead_lettered_at` is tied to DEAD_LETTERED. A dead-lettered event keeps
--    its attempt count and must say why it was dead-lettered.
ALTER TABLE events DROP CONSTRAINT events_lifecycle_consistent;
ALTER TABLE events ADD CONSTRAINT events_lifecycle_consistent CHECK (
    (status = 'PENDING'
        AND processing_owner IS NULL
        AND processing_started_at IS NULL
        AND lease_expires_at IS NULL
        AND processed_at IS NULL
        AND dead_lettered_at IS NULL)
    OR (status = 'PROCESSING'
        AND processing_owner IS NOT NULL
        AND processing_started_at IS NOT NULL
        AND lease_expires_at IS NOT NULL
        AND processed_at IS NULL
        AND dead_lettered_at IS NULL
        AND delivery_attempts >= 1)
    OR (status = 'PROCESSED'
        AND processing_owner IS NULL
        AND lease_expires_at IS NULL
        AND processed_at IS NOT NULL
        AND dead_lettered_at IS NULL
        AND delivery_attempts >= 1)
    OR (status = 'DEAD_LETTERED'
        AND processing_owner IS NULL
        AND lease_expires_at IS NULL
        AND processed_at IS NULL
        AND dead_lettered_at IS NOT NULL
        AND last_failure_code IS NOT NULL
        AND delivery_attempts >= 1)
);

-- 5. Claim path for eligible pending work. DEAD_LETTERED rows are in neither
--    claim index, so the standard claim path never selects them.
CREATE INDEX events_pending_available_idx ON events (available_at) WHERE status = 'PENDING';
