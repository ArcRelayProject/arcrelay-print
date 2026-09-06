use async_trait::async_trait;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, QueryResult,
    Statement, TransactionTrait,
};
use serde::de::DeserializeOwned;

use crate::application::{PrintJobRepository, PrinterShareRepository, QueueBindingRepository};
use crate::domain::{
    ClientJobId, DeviceId, LocalPrinterId, PrintJob, PrintJobId, PrinterShare, PrinterShareId,
    QueueBindingId, RemoteQueueBinding,
};
use crate::{PrintError, Result};

/// SQLite persistence for print configuration and job metadata.
///
/// Aggregates are stored as versionable JSON payloads while their lookup keys
/// and state are first-class indexed columns. Print document bytes are never
/// stored in this database; the spool directory is owned by the document
/// storage adapter.
#[derive(Clone)]
pub struct SqlitePrintStore {
    db: DatabaseConnection,
}

impl SqlitePrintStore {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        let mut options = ConnectOptions::new(url.clone());
        if url.starts_with("sqlite::memory:") {
            options.max_connections(1);
        }
        options.sqlx_logging(false);
        let db = Database::connect(options)
            .await
            .map_err(persistence_error)?;
        let store = Self { db };
        store.migrate().await?;
        Ok(store)
    }

    pub async fn from_connection(db: DatabaseConnection) -> Result<Self> {
        let store = Self { db };
        store.migrate().await?;
        Ok(store)
    }

    pub fn connection(&self) -> &DatabaseConnection {
        &self.db
    }

    pub async fn migrate(&self) -> Result<()> {
        for sql in [
            r#"
            CREATE TABLE IF NOT EXISTS printer_shares (
                id TEXT PRIMARY KEY NOT NULL,
                local_printer_id TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS remote_queue_bindings (
                id TEXT PRIMARY KEY NOT NULL,
                remote_device_id TEXT NOT NULL,
                printer_share_id TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(remote_device_id, printer_share_id)
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS print_jobs (
                id TEXT PRIMARY KEY NOT NULL,
                client_job_id TEXT NOT NULL,
                source_device_id TEXT NOT NULL,
                printer_share_id TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(source_device_id, client_job_id)
            )
            "#,
            r#"
            CREATE INDEX IF NOT EXISTS idx_print_jobs_active
            ON print_jobs(state, updated_at_ms)
            "#,
            r#"
            CREATE INDEX IF NOT EXISTS idx_print_jobs_printer
            ON print_jobs(printer_share_id, updated_at_ms)
            "#,
        ] {
            self.db
                .execute(Statement::from_string(DbBackend::Sqlite, sql))
                .await
                .map_err(persistence_error)?;
        }
        self.migrate_client_job_id_scope().await?;
        Ok(())
    }

    async fn migrate_client_job_id_scope(&self) -> Result<()> {
        let Some(row) = self
            .db
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'print_jobs'",
            ))
            .await
            .map_err(persistence_error)?
        else {
            return Ok(());
        };
        let schema: String = row.try_get("", "sql").map_err(persistence_error)?;
        if !schema.contains("client_job_id TEXT NOT NULL UNIQUE") {
            return Ok(());
        }

        let transaction = self.db.begin().await.map_err(persistence_error)?;
        for sql in [
            "ALTER TABLE print_jobs RENAME TO print_jobs_legacy",
            r#"
            CREATE TABLE print_jobs (
                id TEXT PRIMARY KEY NOT NULL,
                client_job_id TEXT NOT NULL,
                source_device_id TEXT NOT NULL,
                printer_share_id TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(source_device_id, client_job_id)
            )
            "#,
            r#"
            INSERT INTO print_jobs (
                id, client_job_id, source_device_id, printer_share_id,
                state, updated_at_ms, payload_json
            )
            SELECT id, client_job_id, source_device_id, printer_share_id,
                   state, updated_at_ms, payload_json
            FROM print_jobs_legacy
            "#,
            "DROP TABLE print_jobs_legacy",
            "CREATE INDEX idx_print_jobs_active ON print_jobs(state, updated_at_ms)",
            "CREATE INDEX idx_print_jobs_printer ON print_jobs(printer_share_id, updated_at_ms)",
        ] {
            transaction
                .execute(Statement::from_string(DbBackend::Sqlite, sql))
                .await
                .map_err(persistence_error)?;
        }
        transaction.commit().await.map_err(persistence_error)?;
        Ok(())
    }

    async fn query_payload<T: DeserializeOwned>(
        &self,
        sql: &str,
        values: impl IntoIterator<Item = sea_orm::Value>,
    ) -> Result<Option<T>> {
        self.db
            .query_one(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                sql,
                values,
            ))
            .await
            .map_err(persistence_error)?
            .map(decode_payload)
            .transpose()
    }

    async fn query_payloads<T: DeserializeOwned>(
        &self,
        sql: &str,
        values: impl IntoIterator<Item = sea_orm::Value>,
    ) -> Result<Vec<T>> {
        self.db
            .query_all(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                sql,
                values,
            ))
            .await
            .map_err(persistence_error)?
            .into_iter()
            .map(decode_payload)
            .collect()
    }
}

#[async_trait]
impl PrinterShareRepository for SqlitePrintStore {
    async fn find(&self, id: &PrinterShareId) -> Result<Option<PrinterShare>> {
        self.query_payload(
            "SELECT payload_json FROM printer_shares WHERE id = ?",
            [id.to_string().into()],
        )
        .await
    }

    async fn find_by_local_printer(&self, id: &LocalPrinterId) -> Result<Option<PrinterShare>> {
        self.query_payload(
            "SELECT payload_json FROM printer_shares WHERE local_printer_id = ?",
            [id.to_string().into()],
        )
        .await
    }

    async fn list(&self) -> Result<Vec<PrinterShare>> {
        self.query_payloads(
            "SELECT payload_json FROM printer_shares ORDER BY updated_at_ms DESC, id",
            [],
        )
        .await
    }

    async fn save(&self, share: &PrinterShare) -> Result<()> {
        let payload = serde_json::to_string(share)?;
        self.db
            .execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                r#"
                INSERT INTO printer_shares
                    (id, local_printer_id, state, updated_at_ms, payload_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    local_printer_id = excluded.local_printer_id,
                    state = excluded.state,
                    updated_at_ms = excluded.updated_at_ms,
                    payload_json = excluded.payload_json
                "#,
                [
                    share.id.to_string().into(),
                    share.local_printer_id.to_string().into(),
                    enum_token(share.state)?.into(),
                    share.updated_at_ms.into(),
                    payload.into(),
                ],
            ))
            .await
            .map_err(persistence_error)?;
        Ok(())
    }
}

#[async_trait]
impl QueueBindingRepository for SqlitePrintStore {
    async fn find(&self, id: &QueueBindingId) -> Result<Option<RemoteQueueBinding>> {
        self.query_payload(
            "SELECT payload_json FROM remote_queue_bindings WHERE id = ?",
            [id.to_string().into()],
        )
        .await
    }

    async fn find_remote(
        &self,
        remote_device_id: &DeviceId,
        printer_share_id: &PrinterShareId,
    ) -> Result<Option<RemoteQueueBinding>> {
        self.query_payload(
            r#"
            SELECT payload_json FROM remote_queue_bindings
            WHERE remote_device_id = ? AND printer_share_id = ?
            "#,
            [
                remote_device_id.to_string().into(),
                printer_share_id.to_string().into(),
            ],
        )
        .await
    }

    async fn list(&self) -> Result<Vec<RemoteQueueBinding>> {
        self.query_payloads(
            "SELECT payload_json FROM remote_queue_bindings ORDER BY updated_at_ms DESC, id",
            [],
        )
        .await
    }

    async fn save(&self, binding: &RemoteQueueBinding) -> Result<()> {
        let payload = serde_json::to_string(binding)?;
        self.db
            .execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                r#"
                INSERT INTO remote_queue_bindings
                    (id, remote_device_id, printer_share_id, state, updated_at_ms, payload_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    remote_device_id = excluded.remote_device_id,
                    printer_share_id = excluded.printer_share_id,
                    state = excluded.state,
                    updated_at_ms = excluded.updated_at_ms,
                    payload_json = excluded.payload_json
                "#,
                [
                    binding.id.to_string().into(),
                    binding.remote_device_id.to_string().into(),
                    binding.printer_share_id.to_string().into(),
                    enum_token(binding.state)?.into(),
                    binding.updated_at_ms.into(),
                    payload.into(),
                ],
            ))
            .await
            .map_err(persistence_error)?;
        Ok(())
    }

    async fn delete(&self, id: &QueueBindingId) -> Result<()> {
        self.db
            .execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "DELETE FROM remote_queue_bindings WHERE id = ?",
                [id.to_string().into()],
            ))
            .await
            .map_err(persistence_error)?;
        Ok(())
    }
}

#[async_trait]
impl PrintJobRepository for SqlitePrintStore {
    async fn find(&self, id: &PrintJobId) -> Result<Option<PrintJob>> {
        self.query_payload(
            "SELECT payload_json FROM print_jobs WHERE id = ?",
            [id.to_string().into()],
        )
        .await
    }

    async fn find_by_client_job(
        &self,
        source_device_id: &DeviceId,
        id: &ClientJobId,
    ) -> Result<Option<PrintJob>> {
        self.query_payload(
            "SELECT payload_json FROM print_jobs WHERE source_device_id = ? AND client_job_id = ?",
            [source_device_id.to_string().into(), id.to_string().into()],
        )
        .await
    }

    async fn list_active(&self) -> Result<Vec<PrintJob>> {
        self.query_payloads(
            r#"
            SELECT payload_json FROM print_jobs
            WHERE state NOT IN ('completed', 'cancelled', 'failed')
            ORDER BY updated_at_ms, id
            "#,
            [],
        )
        .await
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<PrintJob>> {
        self.query_payloads(
            r#"
            SELECT payload_json FROM print_jobs
            ORDER BY updated_at_ms DESC, id DESC
            LIMIT ?
            "#,
            [(limit.min(100) as i64).into()],
        )
        .await
    }

    async fn save(&self, job: &PrintJob) -> Result<()> {
        let payload = serde_json::to_string(job)?;
        self.db
            .execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                r#"
                INSERT INTO print_jobs
                    (id, client_job_id, source_device_id, printer_share_id,
                     state, updated_at_ms, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    client_job_id = excluded.client_job_id,
                    source_device_id = excluded.source_device_id,
                    printer_share_id = excluded.printer_share_id,
                    state = excluded.state,
                    updated_at_ms = excluded.updated_at_ms,
                    payload_json = excluded.payload_json
                "#,
                [
                    job.id.to_string().into(),
                    job.client_job_id.to_string().into(),
                    job.source_device_id.to_string().into(),
                    job.printer_share_id.to_string().into(),
                    enum_token(job.state)?.into(),
                    job.updated_at_ms.into(),
                    payload.into(),
                ],
            ))
            .await
            .map_err(persistence_error)?;
        Ok(())
    }
}

fn enum_token(value: impl serde::Serialize) -> Result<String> {
    serde_json::to_value(value)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            PrintError::Serialization(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "enum did not serialize to a string",
            )))
        })
}

fn decode_payload<T: DeserializeOwned>(row: QueryResult) -> Result<T> {
    let payload: String = row.try_get("", "payload_json").map_err(persistence_error)?;
    Ok(serde_json::from_str(&payload)?)
}

fn persistence_error(error: impl std::fmt::Display) -> PrintError {
    PrintError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::PrintJobRepository;
    use crate::domain::{
        ColorMode, DuplexMode, Orientation, PageRange, PrintDocument, PrintJob, PrintJobState,
        PrintOptions,
    };

    #[tokio::test]
    async fn job_round_trips_and_active_query_excludes_terminal_jobs() {
        let store = SqlitePrintStore::connect("sqlite::memory:").await.unwrap();
        let mut job = PrintJob::offer(
            ClientJobId::parse("client-1").unwrap(),
            DeviceId::parse("device-1").unwrap(),
            PrinterShareId::parse("share-1").unwrap(),
            PrintDocument {
                name: "test.pdf".into(),
                media_type: "application/pdf".into(),
                size_bytes: 20,
                sha256: [9; 32],
            },
            PrintOptions {
                copies: 1,
                color_mode: ColorMode::Monochrome,
                duplex_mode: DuplexMode::OneSided,
                orientation: Orientation::Portrait,
                media_name: "iso_a4_210x297mm".into(),
                page_ranges: vec![PageRange { first: 1, last: 2 }],
                collate: false,
            },
            1,
        )
        .unwrap();
        PrintJobRepository::save(&store, &job).await.unwrap();
        assert_eq!(store.list_active().await.unwrap().len(), 1);
        assert_eq!(
            PrintJobRepository::find(&store, &job.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PrintJobState::Offered
        );

        job.cancel(2).unwrap();
        PrintJobRepository::save(&store, &job).await.unwrap();
        assert!(store.list_active().await.unwrap().is_empty());
        let recent = store.list_recent(10).await.unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].state, PrintJobState::Cancelled);
    }

    #[tokio::test]
    async fn client_job_id_is_scoped_to_source_device() {
        let store = SqlitePrintStore::connect("sqlite::memory:").await.unwrap();
        let options = PrintOptions {
            copies: 1,
            color_mode: ColorMode::Monochrome,
            duplex_mode: DuplexMode::OneSided,
            orientation: Orientation::Portrait,
            media_name: "iso_a4_210x297mm".into(),
            page_ranges: Vec::new(),
            collate: false,
        };
        let document = PrintDocument {
            name: "test.pdf".into(),
            media_type: "application/pdf".into(),
            size_bytes: 20,
            sha256: [9; 32],
        };
        let client_job_id = ClientJobId::parse("shared-client-id").unwrap();
        let source_a = DeviceId::parse("device-a").unwrap();
        let source_b = DeviceId::parse("device-b").unwrap();
        for source in [&source_a, &source_b] {
            let job = PrintJob::offer(
                client_job_id.clone(),
                source.clone(),
                PrinterShareId::parse("share-1").unwrap(),
                document.clone(),
                options.clone(),
                1,
            )
            .unwrap();
            PrintJobRepository::save(&store, &job).await.unwrap();
        }

        let found_a = store
            .find_by_client_job(&source_a, &client_job_id)
            .await
            .unwrap()
            .unwrap();
        let found_b = store
            .find_by_client_job(&source_b, &client_job_id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(found_a.id, found_b.id);
    }
}
