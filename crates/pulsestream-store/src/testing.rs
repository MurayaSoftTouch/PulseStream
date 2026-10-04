//! Test support for real-PostgreSQL tests (the `test-util` feature).
//!
//! # Safety guard
//!
//! Tests never touch `DATABASE_URL`. They read `PULSESTREAM_TEST_DATABASE_URL`,
//! whose database name **must end in `_test`**. Each test creates its own
//! disposable database named `<that name>_<random hex>`, applies migrations to
//! it, and drops it afterwards (`DROP DATABASE ... WITH (FORCE)`). Only
//! databases this module created are ever dropped, and nothing is truncated,
//! so tests are isolated and can run in parallel.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pulsestream_core::config::{DatabaseConfig, DatabaseUrl};
use pulsestream_core::event::EventId;
use sqlx::{AssertSqlSafe, Connection, PgConnection};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use uuid::Uuid;

use crate::Store;

pub const TEST_DATABASE_URL_VAR: &str = "PULSESTREAM_TEST_DATABASE_URL";

/// Splits `postgres://auth@host:port/name?query` into (prefix up to `/`, name, query).
fn split_database_name(url: &str) -> Option<(&str, &str, &str)> {
    let scheme_end = url.find("://")? + 3;
    let path_start = scheme_end + url[scheme_end..].find('/')?;
    let (name, query) = match url[path_start + 1..].find('?') {
        Some(q) => (
            &url[path_start + 1..path_start + 1 + q],
            &url[path_start + 1 + q..],
        ),
        None => (&url[path_start + 1..], ""),
    };
    Some((&url[..path_start + 1], name, query))
}

/// Refuses anything but a plain identifier ending in `_test`.
fn guard_test_database_name(name: &str) -> Result<(), String> {
    let plain = !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if plain && name.ends_with("_test") {
        Ok(())
    } else {
        Err(format!(
            "{TEST_DATABASE_URL_VAR} must name a database ending in `_test` \
             (lowercase letters, digits, underscores); refusing to use {name:?}"
        ))
    }
}

/// A disposable, migrated database for one test.
pub struct TestDatabase {
    pub store: Store,
    url: String,
    admin_url: String,
    name: String,
}

impl TestDatabase {
    /// Creates and migrates a fresh database.
    ///
    /// # Panics
    ///
    /// Panics if `PULSESTREAM_TEST_DATABASE_URL` is unset or fails the safety
    /// guard, or if PostgreSQL is unreachable.
    pub async fn create() -> Self {
        let db = Self::create_empty().await;
        db.store.migrate().await.expect("migrations apply");
        db
    }

    /// Creates a fresh database without applying migrations.
    pub async fn create_empty() -> Self {
        let admin_url = std::env::var(TEST_DATABASE_URL_VAR).unwrap_or_else(|_| {
            panic!("{TEST_DATABASE_URL_VAR} must be set to run PostgreSQL tests")
        });
        let (prefix, base, query) =
            split_database_name(&admin_url).expect("test database URL has a database name");
        guard_test_database_name(base).unwrap_or_else(|msg| panic!("{msg}"));

        let name = format!("{base}_{}", &Uuid::new_v4().simple().to_string()[..12]);
        let url = format!("{prefix}{name}{query}");
        let mut admin = PgConnection::connect(&admin_url)
            .await
            .expect("connect to the test database server");
        // `name` is `<guarded base>_<hex>`: a plain identifier, safe to interpolate.
        sqlx::query(AssertSqlSafe(format!("CREATE DATABASE \"{name}\"")))
            .execute(&mut admin)
            .await
            .expect("create disposable test database");
        admin.close().await.ok();

        let config = DatabaseConfig::with_url(DatabaseUrl::parse(&url).expect("valid URL"));
        let store = Store::connect_lazy(&config, "pulsestream-test").expect("pool");
        Self {
            store,
            url,
            admin_url,
            name,
        }
    }

    /// Connection settings for this database, for building additional,
    /// independent pools (for example, a "restarted" application instance).
    pub fn config(&self) -> DatabaseConfig {
        DatabaseConfig::with_url(DatabaseUrl::parse(&self.url).expect("valid URL"))
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Simulates the passage of a claim's lease without sleeping: moves the
    /// lease expiry of a PROCESSING event into the past.
    pub async fn expire_lease(&self, id: EventId) {
        let updated = sqlx::query(
            "UPDATE events SET lease_expires_at = now() - interval '1 second' \
             WHERE event_id = $1 AND status = 'PROCESSING'",
        )
        .bind(id.as_uuid())
        .execute(self.store.pool())
        .await
        .expect("expire lease");
        assert_eq!(updated.rows_affected(), 1, "event {id} was not PROCESSING");
    }

    /// Raw lifecycle columns, for assertions.
    pub async fn lifecycle(&self, id: EventId) -> Lifecycle {
        let (status, owner, attempts, lease_active): (String, Option<Uuid>, i32, Option<bool>) =
            sqlx::query_as(
                "SELECT status, processing_owner, delivery_attempts, lease_expires_at > now() \
                 FROM events WHERE event_id = $1",
            )
            .bind(id.as_uuid())
            .fetch_one(self.store.pool())
            .await
            .expect("event row exists");
        Lifecycle {
            status,
            owner,
            attempts,
            lease_active,
        }
    }

    /// Row count for one source, to assert "no duplicate rows" without
    /// depending on other rows.
    pub async fn count_for_source(&self, source: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM events WHERE source = $1")
            .bind(source)
            .fetch_one(self.store.pool())
            .await
            .expect("count")
    }

    /// Number of events in each status, in the order PENDING, PROCESSING, PROCESSED.
    pub async fn status_counts(&self) -> (i64, i64, i64) {
        sqlx::query_as(
            "SELECT count(*) FILTER (WHERE status = 'PENDING'), \
                    count(*) FILTER (WHERE status = 'PROCESSING'), \
                    count(*) FILTER (WHERE status = 'PROCESSED') \
             FROM events",
        )
        .fetch_one(self.store.pool())
        .await
        .expect("status counts")
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Drop runs outside async context (and possibly while unwinding), so
        // clean up on a short-lived thread with its own runtime.
        let admin_url = self.admin_url.clone();
        let name = self.name.clone();
        let cleanup = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime");
            runtime.block_on(async move {
                if let Ok(mut admin) = PgConnection::connect(&admin_url).await {
                    // Only the database this guard created is dropped.
                    let drop = format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)");
                    let _ = sqlx::query(AssertSqlSafe(drop)).execute(&mut admin).await;
                    admin.close().await.ok();
                }
            });
        });
        let _ = cleanup.join();
    }
}

/// Lifecycle columns of one event row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lifecycle {
    pub status: String,
    pub owner: Option<Uuid>,
    pub attempts: i32,
    /// `None` when no lease is set.
    pub lease_active: Option<bool>,
}

/// A TCP proxy in front of PostgreSQL that can simulate an outage and a
/// recovery. While down, it closes all proxied connections and refuses new
/// ones.
pub struct OutageProxy {
    addr: SocketAddr,
    up: Arc<AtomicBool>,
    generation: watch::Sender<u64>,
}

impl OutageProxy {
    /// Starts a proxy to the host and port of `database_url`, initially up.
    pub async fn start(database_url: &str) -> Self {
        let (prefix, _, _) = split_database_name(database_url).expect("database URL");
        let authority = prefix
            .split("://")
            .nth(1)
            .expect("scheme")
            .trim_end_matches('/');
        let host_port = authority.rsplit('@').next().expect("host");
        let target = if host_port.contains(':') {
            host_port.to_owned()
        } else {
            format!("{host_port}:5432")
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().expect("proxy addr");
        let up = Arc::new(AtomicBool::new(true));
        let (generation, _) = watch::channel(0u64);
        let accept_up = Arc::clone(&up);
        let accept_generation = generation.clone();
        tokio::spawn(async move {
            while let Ok((mut client, _)) = listener.accept().await {
                if !accept_up.load(Ordering::Acquire) {
                    continue; // dropping `client` closes it immediately
                }
                let target = target.clone();
                let mut outage = accept_generation.subscribe();
                tokio::spawn(async move {
                    let Ok(mut server) = TcpStream::connect(&target).await else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                        _ = outage.changed() => {} // outage: drop both sockets
                    }
                });
            }
        });
        Self {
            addr,
            up,
            generation,
        }
    }

    /// `database_url` rewritten to connect through the proxy.
    pub fn proxied_url(&self, database_url: &str) -> String {
        let (prefix, name, query) = split_database_name(database_url).expect("database URL");
        let scheme_end = prefix.find("://").expect("scheme") + 3;
        let authority = &prefix[scheme_end..prefix.len() - 1];
        let credentials = authority.rsplit_once('@').map(|(c, _)| format!("{c}@"));
        format!(
            "{}{}{}/{name}{query}",
            &prefix[..scheme_end],
            credentials.unwrap_or_default(),
            self.addr
        )
    }

    /// Simulates a database outage: closes existing connections, refuses new ones.
    pub fn go_down(&self) {
        self.up.store(false, Ordering::Release);
        self.generation.send_modify(|g| *g += 1);
    }

    /// Ends the simulated outage.
    pub fn come_up(&self) {
        self.up.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_database_names() {
        assert_eq!(
            split_database_name("postgres://u:p@h:1/pulsestream_test?sslmode=disable"),
            Some((
                "postgres://u:p@h:1/",
                "pulsestream_test",
                "?sslmode=disable"
            ))
        );
        assert_eq!(
            split_database_name("postgres://h/db"),
            Some(("postgres://h/", "db", ""))
        );
    }

    #[test]
    fn guard_accepts_only_test_databases() {
        assert!(guard_test_database_name("pulsestream_test").is_ok());
        for name in [
            "pulsestream",
            "prod",
            "test",
            "pulsestream_test2",
            "x_test\"; DROP",
            "",
        ] {
            assert!(guard_test_database_name(name).is_err(), "{name:?}");
        }
    }
}
