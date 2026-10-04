-- PulseStream M2: durable event store.
--
-- This migration is immutable once published. Schema changes go in new,
-- higher-numbered files.
--
-- All lifecycle timestamps use the database clock (now()), so lease expiry
-- does not depend on the clocks of individual API or worker hosts.

CREATE TABLE events (
    event_id              UUID        PRIMARY KEY,

    -- Immutable event content (guarded by the trigger below).
    source                TEXT        NOT NULL CHECK (char_length(source) BETWEEN 1 AND 100),
    event_type            TEXT        NOT NULL CHECK (char_length(event_type) BETWEEN 1 AND 150),
    payload               JSONB       NOT NULL CHECK (payload <> 'null'::jsonb),
    idempotency_key       TEXT        NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 128),
    -- SHA-256 over the canonical request (see pulsestream_core::idempotency).
    request_fingerprint   BYTEA       NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    accepted_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Mutable processing lifecycle.
    status                TEXT        NOT NULL DEFAULT 'PENDING'
                                      CHECK (status IN ('PENDING', 'PROCESSING', 'PROCESSED')),
    processing_owner      UUID,
    processing_started_at TIMESTAMPTZ,
    lease_expires_at      TIMESTAMPTZ,
    processed_at          TIMESTAMPTZ,
    delivery_attempts     INTEGER     NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),

    -- Producer idempotency is scoped by source and enforced here, not only in
    -- application code, so concurrent admissions cannot create duplicates.
    CONSTRAINT events_source_idempotency_key_key UNIQUE (source, idempotency_key),

    -- Lifecycle fields must agree with the status.
    CONSTRAINT events_lifecycle_consistent CHECK (
        (status = 'PENDING'
            AND processing_owner IS NULL
            AND processing_started_at IS NULL
            AND lease_expires_at IS NULL
            AND processed_at IS NULL)
        OR (status = 'PROCESSING'
            AND processing_owner IS NOT NULL
            AND processing_started_at IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND processed_at IS NULL
            AND delivery_attempts >= 1)
        OR (status = 'PROCESSED'
            AND processing_owner IS NULL
            AND lease_expires_at IS NULL
            AND processed_at IS NOT NULL
            AND delivery_attempts >= 1)
    )
);

-- Claim paths: oldest pending events first, and expired leases.
CREATE INDEX events_pending_idx ON events (accepted_at, event_id) WHERE status = 'PENDING';
CREATE INDEX events_processing_lease_idx ON events (lease_expires_at) WHERE status = 'PROCESSING';

-- Event identity and content never change after admission. Only lifecycle
-- columns may be updated; an idempotency conflict must never rewrite a row.
CREATE FUNCTION events_reject_content_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_id            IS DISTINCT FROM OLD.event_id
    OR NEW.source              IS DISTINCT FROM OLD.source
    OR NEW.event_type          IS DISTINCT FROM OLD.event_type
    OR NEW.payload             IS DISTINCT FROM OLD.payload
    OR NEW.idempotency_key     IS DISTINCT FROM OLD.idempotency_key
    OR NEW.request_fingerprint IS DISTINCT FROM OLD.request_fingerprint
    OR NEW.accepted_at         IS DISTINCT FROM OLD.accepted_at
    THEN
        RAISE EXCEPTION 'event content is immutable (event_id %)', OLD.event_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER events_content_immutable
    BEFORE UPDATE ON events
    FOR EACH ROW EXECUTE FUNCTION events_reject_content_update();
