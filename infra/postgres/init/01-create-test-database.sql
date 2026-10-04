-- Runs only when the PostgreSQL data volume is first initialized.
-- Creates the separate database used by real-PostgreSQL tests. Tests create
-- and drop their own disposable `pulsestream_test_<hex>` databases from it.
CREATE DATABASE pulsestream_test;
