//! Minimal loopback IPP bridge used to register a remote ArcRelay printer in
//! the local operating system. It intentionally listens on loopback only.

use std::collections::HashMap;
use std::path::{Path as FilePath, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderValue, Response, StatusCode};
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::domain::{
    ClientJobId, ColorMode, DuplexMode, Orientation, PageRange, PrintJobState, PrintOptions,
};
use crate::infrastructure::quic::{PrintQuicService, RemotePrinter};
use crate::{PrintError, Result};

mod request;
mod response;

use request::*;
use response::*;

const PRINT_JOB: u16 = 0x0002;
const VALIDATE_JOB: u16 = 0x0004;
const CANCEL_JOB: u16 = 0x0008;
const GET_JOB_ATTRIBUTES: u16 = 0x0009;
const GET_JOBS: u16 = 0x000a;
const GET_PRINTER_ATTRIBUTES: u16 = 0x000b;
const IPP_OK: u16 = 0x0000;
const IPP_BAD_REQUEST: u16 = 0x0400;
const IPP_NOT_FOUND: u16 = 0x0406;
const IPP_OPERATION_NOT_SUPPORTED: u16 = 0x0501;
const MAX_IPP_REQUEST_BYTES: usize = arcrelay_wire::MAX_PRINT_DOCUMENT_SIZE as usize + 64 * 1024;
const MAX_IPP_ATTRIBUTE_BYTES: usize = 64 * 1024;
pub const IPP_BRIDGE_PORT: u16 = 17_654;

pub struct IppBridge {
    port: u16,
    network: Arc<PrintQuicService>,
    next_job_id: AtomicU32,
    jobs: tokio::sync::RwLock<HashMap<u32, IppJobBinding>>,
    state_path: PathBuf,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct IppJobBinding {
    remote_device_id: String,
    printer_share_id: String,
    remote_job_id: String,
    #[serde(default)]
    remote_device_name: String,
    #[serde(default)]
    printer_name: String,
    #[serde(default)]
    document_name: String,
    #[serde(default)]
    state: Option<PrintJobState>,
    #[serde(default)]
    failure_message: Option<String>,
    #[serde(default)]
    created_at_ms: i64,
    #[serde(default)]
    updated_at_ms: i64,
}

#[derive(Debug, Clone, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct OutgoingPrintJob {
    pub local_job_id: u32,
    pub remote_device_id: String,
    pub remote_device_name: String,
    pub printer_share_id: String,
    pub printer_name: String,
    pub document_name: String,
    pub remote_job_id: String,
    pub state: Option<PrintJobState>,
    pub failure_message: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub source_available: bool,
}

impl IppBridge {
    pub async fn start(
        network: Arc<PrintQuicService>,
        state_path: impl Into<PathBuf>,
    ) -> Result<Arc<Self>> {
        // System print queues outlive the ArcRelay process, so their URI must
        // remain valid across restarts. A random ephemeral port would leave
        // every installed queue pointing at the previous process instance.
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", IPP_BRIDGE_PORT)).await?;
        let port = listener.local_addr()?.port();
        let state_path = state_path.into();
        let jobs = load_job_bindings(&state_path).await;
        let next_job_id = jobs.keys().copied().max().unwrap_or(0).saturating_add(1);
        let bridge = Arc::new(Self {
            port,
            network,
            next_job_id: AtomicU32::new(next_job_id),
            jobs: tokio::sync::RwLock::new(jobs),
            state_path,
        });
        let router = Router::new()
            .route("/printers/{device_id}/{share_id}", post(handle_ipp))
            .route("/jobs/{job_id}", post(handle_ipp_job))
            .with_state(bridge.clone());
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, router).await {
                tracing::error!(%error, "loopback IPP bridge stopped");
            }
        });
        Ok(bridge)
    }

    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    #[must_use]
    pub fn printer_uri(&self, printer: &RemotePrinter) -> String {
        format!(
            "ipp://127.0.0.1:{}/printers/{}/{}",
            self.port, printer.printer.source_device_id, printer.printer.share_id
        )
    }

    async fn find_remote(&self, device_id: &str, share_id: &str) -> Option<RemotePrinter> {
        self.network.snapshot().await.into_iter().find(|remote| {
            remote.printer.source_device_id.as_str() == device_id
                && remote.printer.share_id.as_str() == share_id
        })
    }

    async fn record_job(
        &self,
        local_job_id: u32,
        remote: &RemotePrinter,
        job_id: &str,
        document_name: &str,
    ) {
        let now_ms = unix_timestamp_ms();
        let snapshot = {
            let mut jobs = self.jobs.write().await;
            jobs.insert(
                local_job_id,
                IppJobBinding {
                    remote_device_id: remote.printer.source_device_id.to_string(),
                    printer_share_id: remote.printer.share_id.to_string(),
                    remote_job_id: job_id.to_owned(),
                    remote_device_name: remote.printer.source_device_name.clone(),
                    printer_name: remote.printer.display_name.clone(),
                    document_name: document_name.to_owned(),
                    state: Some(PrintJobState::Queued),
                    failure_message: None,
                    created_at_ms: now_ms,
                    updated_at_ms: now_ms,
                },
            );
            while jobs.len() > 1_024 {
                if let Some(oldest) = jobs.keys().copied().min() {
                    jobs.remove(&oldest);
                } else {
                    break;
                }
            }
            jobs.clone()
        };
        if let Err(error) = persist_job_bindings(&self.state_path, &snapshot).await {
            tracing::warn!(%error, "could not persist IPP job mapping");
        }
    }

    async fn update_job_state(
        &self,
        local_job_id: u32,
        state: PrintJobState,
        failure_message: Option<String>,
    ) {
        let snapshot = {
            let mut jobs = self.jobs.write().await;
            let Some(binding) = jobs.get_mut(&local_job_id) else {
                return;
            };
            binding.state = Some(state);
            binding.failure_message = failure_message;
            binding.updated_at_ms = unix_timestamp_ms();
            jobs.clone()
        };
        if let Err(error) = persist_job_bindings(&self.state_path, &snapshot).await {
            tracing::warn!(%error, "could not persist IPP job status");
        }
    }

    pub async fn list_jobs(&self, refresh_active: bool, limit: usize) -> Vec<OutgoingPrintJob> {
        let remotes = self.network.snapshot().await;
        let mut bindings = self
            .jobs
            .read()
            .await
            .iter()
            .map(|(id, binding)| (*id, binding.clone()))
            .collect::<Vec<_>>();
        bindings.sort_by_key(|(id, binding)| std::cmp::Reverse((binding.created_at_ms, *id)));
        bindings.truncate(limit.min(100));

        let mut result = Vec::with_capacity(bindings.len());
        let mut refresh_attempts = 0_usize;
        for (local_job_id, mut binding) in bindings {
            let remote = remotes.iter().find(|remote| {
                remote.printer.source_device_id.as_str() == binding.remote_device_id
                    && remote.printer.share_id.as_str() == binding.printer_share_id
            });
            let should_refresh =
                refresh_active && binding.state.map(|state| !state.terminal()).unwrap_or(true);
            if should_refresh && refresh_attempts < 6 {
                refresh_attempts += 1;
                if let (Some(remote), Ok(remote_job_id)) = (
                    remote,
                    crate::domain::PrintJobId::parse(binding.remote_job_id.clone()),
                ) {
                    if let Ok(Ok(job)) = tokio::time::timeout(
                        std::time::Duration::from_millis(500),
                        self.network.get_remote_job(remote, &remote_job_id),
                    )
                    .await
                    {
                        binding.state = Some(job.state);
                        binding.failure_message = job.failure_message.clone();
                        binding.updated_at_ms = unix_timestamp_ms();
                        self.update_job_state(local_job_id, job.state, job.failure_message)
                            .await;
                    }
                }
            }
            let remote_device_name = if binding.remote_device_name.is_empty() {
                remote
                    .map(|remote| remote.printer.source_device_name.clone())
                    .unwrap_or_else(|| binding.remote_device_id.clone())
            } else {
                binding.remote_device_name.clone()
            };
            let printer_name = if binding.printer_name.is_empty() {
                remote
                    .map(|remote| remote.printer.display_name.clone())
                    .unwrap_or_else(|| "LAN printer".to_string())
            } else {
                binding.printer_name.clone()
            };
            result.push(OutgoingPrintJob {
                local_job_id,
                remote_device_id: binding.remote_device_id,
                remote_device_name,
                printer_share_id: binding.printer_share_id,
                printer_name,
                document_name: if binding.document_name.is_empty() {
                    "Print job".to_string()
                } else {
                    binding.document_name
                },
                remote_job_id: binding.remote_job_id,
                state: binding.state,
                failure_message: binding.failure_message,
                created_at_ms: binding.created_at_ms,
                updated_at_ms: binding.updated_at_ms,
                source_available: remote.is_some(),
            });
        }
        result
    }

    async fn resolve_binding(&self, local_job_id: u32) -> Option<(IppJobBinding, RemotePrinter)> {
        let binding = self.jobs.read().await.get(&local_job_id).cloned()?;
        let remote = self
            .find_remote(&binding.remote_device_id, &binding.printer_share_id)
            .await?;
        Some((binding, remote))
    }
}

async fn load_job_bindings(path: &FilePath) -> HashMap<u32, IppJobBinding> {
    match tokio::fs::read(path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|error| {
            tracing::warn!(%error, "could not parse IPP job mapping");
            HashMap::new()
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
        Err(error) => {
            tracing::warn!(%error, "could not read IPP job mapping");
            HashMap::new()
        }
    }
}

async fn persist_job_bindings(path: &FilePath, jobs: &HashMap<u32, IppJobBinding>) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let bytes = serde_json::to_vec(jobs)?;
    let temporary = path.with_extension("json.tmp");
    tokio::fs::write(&temporary, bytes).await?;
    if tokio::fs::try_exists(path).await? {
        tokio::fs::remove_file(path).await?;
    }
    tokio::fs::rename(temporary, path).await?;
    Ok(())
}

fn unix_timestamp_ms() -> i64 {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as i64
}

async fn handle_ipp(
    State(bridge): State<Arc<IppBridge>>,
    Path((device_id, share_id)): Path<(String, String)>,
    mut body: Body,
) -> Response<Body> {
    let temporary = match tempfile::NamedTempFile::new() {
        Ok(file) => file,
        Err(error) => {
            tracing::warn!(%error, "could not create loopback IPP spool file");
            return http_error(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    let write_handle = match temporary.reopen() {
        Ok(file) => file,
        Err(error) => {
            tracing::warn!(%error, "could not open loopback IPP spool file");
            return http_error(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    let mut output = tokio::fs::File::from_std(write_handle);
    let mut prefix = Vec::with_capacity(MAX_IPP_ATTRIBUTE_BYTES);
    let mut total = 0_usize;
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                tracing::warn!(%error, "could not read loopback IPP request");
                return http_error(StatusCode::BAD_REQUEST);
            }
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        total = match total.checked_add(data.len()) {
            Some(total) if total <= MAX_IPP_REQUEST_BYTES => total,
            _ => return http_error(StatusCode::PAYLOAD_TOO_LARGE),
        };
        if prefix.len() < MAX_IPP_ATTRIBUTE_BYTES {
            let take = (MAX_IPP_ATTRIBUTE_BYTES - prefix.len()).min(data.len());
            prefix.extend_from_slice(&data[..take]);
        }
        if let Err(error) = output.write_all(&data).await {
            tracing::warn!(%error, "could not spool loopback IPP request");
            return http_error(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
    if let Err(error) = output.flush().await {
        tracing::warn!(%error, "could not flush loopback IPP request");
        return http_error(StatusCode::INTERNAL_SERVER_ERROR);
    }
    drop(output);
    let request = match IppRequest::parse(&prefix, total as u64) {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(%error, "invalid loopback IPP request");
            return ipp_response(&[2, 0], IPP_BAD_REQUEST, 0, Vec::new());
        }
    };
    let Some(remote) = bridge.find_remote(&device_id, &share_id).await else {
        return ipp_response(
            &request.version,
            IPP_NOT_FOUND,
            request.request_id,
            operation_attributes(),
        );
    };
    match request.operation {
        GET_PRINTER_ATTRIBUTES => printer_attributes_response(&bridge, &remote, &request),
        GET_JOB_ATTRIBUTES => {
            job_attributes_response(&bridge, &request, request.integer("job-id")).await
        }
        GET_JOBS => get_jobs_response(&bridge, &remote, &request).await,
        CANCEL_JOB => cancel_job_response(&bridge, &request, request.integer("job-id")).await,
        VALIDATE_JOB => ipp_response(
            &request.version,
            IPP_OK,
            request.request_id,
            operation_attributes(),
        ),
        PRINT_JOB => print_job_response(&bridge, &remote, request, temporary.path()).await,
        _ => ipp_response(
            &request.version,
            IPP_OPERATION_NOT_SUPPORTED,
            request.request_id,
            operation_attributes(),
        ),
    }
}

async fn handle_ipp_job(
    State(bridge): State<Arc<IppBridge>>,
    Path(job_id): Path<u32>,
    mut body: Body,
) -> Response<Body> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(_) => return http_error(StatusCode::BAD_REQUEST),
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if bytes.len().saturating_add(data.len()) > MAX_IPP_ATTRIBUTE_BYTES {
            return http_error(StatusCode::PAYLOAD_TOO_LARGE);
        }
        bytes.extend_from_slice(&data);
    }
    let request = match IppRequest::parse(&bytes, bytes.len() as u64) {
        Ok(request) => request,
        Err(_) => return ipp_response(&[2, 0], IPP_BAD_REQUEST, 0, Vec::new()),
    };
    match request.operation {
        GET_JOB_ATTRIBUTES => job_attributes_response(&bridge, &request, Some(job_id as i32)).await,
        CANCEL_JOB => cancel_job_response(&bridge, &request, Some(job_id as i32)).await,
        _ => ipp_response(
            &request.version,
            IPP_OPERATION_NOT_SUPPORTED,
            request.request_id,
            operation_attributes(),
        ),
    }
}

async fn job_attributes_response(
    bridge: &IppBridge,
    request: &IppRequest,
    local_job_id: Option<i32>,
) -> Response<Body> {
    let Some(local_job_id) = local_job_id.and_then(|value| u32::try_from(value).ok()) else {
        return ipp_response(
            &request.version,
            IPP_BAD_REQUEST,
            request.request_id,
            operation_attributes(),
        );
    };
    let Some((binding, remote)) = bridge.resolve_binding(local_job_id).await else {
        return ipp_response(
            &request.version,
            IPP_NOT_FOUND,
            request.request_id,
            operation_attributes(),
        );
    };
    let Ok(remote_job_id) = crate::domain::PrintJobId::parse(binding.remote_job_id) else {
        return ipp_response(
            &request.version,
            IPP_NOT_FOUND,
            request.request_id,
            operation_attributes(),
        );
    };
    match bridge.network.get_remote_job(&remote, &remote_job_id).await {
        Ok(job) => {
            bridge
                .update_job_state(local_job_id, job.state, job.failure_message.clone())
                .await;
            ipp_response(
                &request.version,
                IPP_OK,
                request.request_id,
                job_attributes(local_job_id, &job),
            )
        }
        Err(error) => {
            tracing::debug!(%error, "could not query remote IPP job");
            ipp_response(
                &request.version,
                0x0500,
                request.request_id,
                operation_attributes(),
            )
        }
    }
}

async fn cancel_job_response(
    bridge: &IppBridge,
    request: &IppRequest,
    local_job_id: Option<i32>,
) -> Response<Body> {
    let Some(local_job_id) = local_job_id.and_then(|value| u32::try_from(value).ok()) else {
        return ipp_response(
            &request.version,
            IPP_BAD_REQUEST,
            request.request_id,
            operation_attributes(),
        );
    };
    let Some((binding, remote)) = bridge.resolve_binding(local_job_id).await else {
        return ipp_response(
            &request.version,
            IPP_NOT_FOUND,
            request.request_id,
            operation_attributes(),
        );
    };
    let Ok(remote_job_id) = crate::domain::PrintJobId::parse(binding.remote_job_id) else {
        return ipp_response(
            &request.version,
            IPP_NOT_FOUND,
            request.request_id,
            operation_attributes(),
        );
    };
    match bridge
        .network
        .cancel_remote_job(&remote, &remote_job_id)
        .await
    {
        Ok(job) => {
            bridge
                .update_job_state(local_job_id, job.state, job.failure_message)
                .await;
            ipp_response(
                &request.version,
                IPP_OK,
                request.request_id,
                operation_attributes(),
            )
        }
        Err(error) => {
            tracing::debug!(%error, "could not cancel remote IPP job");
            ipp_response(
                &request.version,
                0x0500,
                request.request_id,
                operation_attributes(),
            )
        }
    }
}

async fn get_jobs_response(
    bridge: &IppBridge,
    remote: &RemotePrinter,
    request: &IppRequest,
) -> Response<Body> {
    let mut jobs = bridge
        .jobs
        .read()
        .await
        .iter()
        .map(|(id, binding)| (*id, binding.clone()))
        .collect::<Vec<_>>();
    jobs.sort_by_key(|(id, _)| std::cmp::Reverse(*id));
    let mut payload = operation_attributes();
    for (local_id, binding) in jobs.into_iter().take(64) {
        if binding.remote_device_id != remote.printer.source_device_id.as_str()
            || binding.printer_share_id != remote.printer.share_id.as_str()
        {
            continue;
        }
        let Ok(remote_id) = crate::domain::PrintJobId::parse(binding.remote_job_id) else {
            continue;
        };
        if let Ok(job) = bridge.network.get_remote_job(remote, &remote_id).await {
            bridge
                .update_job_state(local_id, job.state, job.failure_message.clone())
                .await;
            payload.extend(job_attributes(local_id, &job));
        }
    }
    ipp_response(&request.version, IPP_OK, request.request_id, payload)
}

fn job_attributes(local_job_id: u32, job: &crate::infrastructure::quic::RemotePrintJob) -> Vec<u8> {
    let mut payload = vec![0x02];
    integer_attribute(&mut payload, "job-id", local_job_id as i32);
    enum_attribute(&mut payload, "job-state", ipp_job_state(job.state));
    attribute(
        &mut payload,
        0x44,
        "job-state-reasons",
        if job.failure_message.is_some() {
            b"job-completed-with-errors"
        } else {
            b"none"
        },
    );
    payload
}

fn ipp_job_state(state: crate::domain::PrintJobState) -> i32 {
    use crate::domain::PrintJobState;
    match state {
        PrintJobState::Offered
        | PrintJobState::Receiving
        | PrintJobState::Validating
        | PrintJobState::Ready
        | PrintJobState::Submitting
        | PrintJobState::Queued => 3,
        PrintJobState::Printing => 5,
        PrintJobState::Held | PrintJobState::Ambiguous => 6,
        PrintJobState::Cancelled => 7,
        PrintJobState::Failed => 8,
        PrintJobState::Completed => 9,
    }
}

fn printer_attributes_response(
    bridge: &IppBridge,
    remote: &RemotePrinter,
    request: &IppRequest,
) -> Response<Body> {
    let mut payload = operation_attributes();
    payload.push(0x04);
    let uri = bridge.printer_uri(remote);
    let more_info_uri = uri
        .strip_prefix("ipp://")
        .map(|value| format!("http://{value}"))
        .unwrap_or_else(|| uri.clone());
    attribute(&mut payload, 0x45, "printer-uri-supported", uri.as_bytes());
    attribute(&mut payload, 0x44, "uri-authentication-supported", b"none");
    attribute(&mut payload, 0x44, "uri-security-supported", b"none");
    attribute(&mut payload, 0x47, "charset-configured", b"utf-8");
    attribute(&mut payload, 0x47, "charset-supported", b"utf-8");
    attribute(&mut payload, 0x44, "compression-supported", b"none");
    attribute(&mut payload, 0x48, "natural-language-configured", b"en");
    attribute(
        &mut payload,
        0x48,
        "generated-natural-language-supported",
        b"en",
    );
    attribute(
        &mut payload,
        0x42,
        "printer-name",
        remote.printer.display_name.as_bytes(),
    );
    attribute(
        &mut payload,
        0x41,
        "printer-info",
        remote.printer.display_name.as_bytes(),
    );
    attribute(&mut payload, 0x41, "printer-location", b"ArcRelay");
    attribute(
        &mut payload,
        0x41,
        "printer-make-and-model",
        remote.printer.display_name.as_bytes(),
    );
    attribute(
        &mut payload,
        0x45,
        "printer-more-info",
        more_info_uri.as_bytes(),
    );
    attribute(
        &mut payload,
        0x45,
        "printer-uuid",
        format!("urn:uuid:{}", remote.printer.share_id).as_bytes(),
    );
    enum_attribute(&mut payload, "printer-state", 3);
    attribute(&mut payload, 0x44, "printer-state-reasons", b"none");
    boolean_attribute(&mut payload, "printer-is-accepting-jobs", true);
    integer_attribute(&mut payload, "queued-job-count", 0);
    integer_attribute(
        &mut payload,
        "printer-up-time",
        (unix_timestamp_ms() / 1_000).clamp(0, i32::MAX as i64) as i32,
    );
    repeated_attribute(
        &mut payload,
        0x44,
        "ipp-versions-supported",
        &[b"1.1", b"2.0"],
    );
    repeated_i32_attribute(
        &mut payload,
        0x23,
        "operations-supported",
        &[
            PRINT_JOB as i32,
            VALIDATE_JOB as i32,
            CANCEL_JOB as i32,
            GET_JOB_ATTRIBUTES as i32,
            GET_JOBS as i32,
            GET_PRINTER_ATTRIBUTES as i32,
        ],
    );
    attribute(
        &mut payload,
        0x49,
        "document-format-supported",
        b"application/pdf",
    );
    attribute(
        &mut payload,
        0x49,
        "document-format-default",
        b"application/pdf",
    );
    attribute(&mut payload, 0x44, "pdl-override-supported", b"attempted");
    repeated_attribute(
        &mut payload,
        0x44,
        "job-creation-attributes-supported",
        &[
            b"copies",
            b"sides",
            b"orientation-requested",
            b"media",
            b"printer-resolution",
            b"job-name",
            b"print-color-mode",
        ],
    );
    boolean_attribute(
        &mut payload,
        "color-supported",
        remote
            .printer
            .capabilities
            .color_modes
            .contains(&ColorMode::Color),
    );
    let sides = remote
        .printer
        .capabilities
        .duplex_modes
        .iter()
        .map(|mode| match mode {
            DuplexMode::OneSided => b"one-sided".as_slice(),
            DuplexMode::TwoSidedLongEdge => b"two-sided-long-edge".as_slice(),
            DuplexMode::TwoSidedShortEdge => b"two-sided-short-edge".as_slice(),
        })
        .collect::<Vec<_>>();
    repeated_attribute(&mut payload, 0x44, "sides-supported", &sides);
    attribute(&mut payload, 0x44, "sides-default", b"one-sided");
    let media = remote
        .printer
        .capabilities
        .media_sizes
        .iter()
        .map(|media| media.name.as_bytes())
        .collect::<Vec<_>>();
    repeated_attribute(&mut payload, 0x44, "media-supported", &media);
    if let Some(first) = media.first() {
        attribute(&mut payload, 0x44, "media-default", first);
    }
    if let Some(first) = remote.printer.capabilities.media_sizes.first() {
        media_col_default_attribute(&mut payload, first.width_microns, first.height_microns);
    }
    range_attribute(
        &mut payload,
        "copies-supported",
        1,
        remote.printer.capabilities.max_copies as i32,
    );
    integer_attribute(&mut payload, "copies-default", 1);
    enum_attribute(&mut payload, "orientation-requested-default", 3);
    repeated_i32_attribute(
        &mut payload,
        0x23,
        "orientation-requested-supported",
        &[3, 4],
    );
    let resolutions = remote
        .printer
        .capabilities
        .resolutions
        .iter()
        .map(|resolution| (resolution.horizontal_dpi, resolution.vertical_dpi))
        .collect::<Vec<_>>();
    if let Some(&(horizontal, vertical)) = resolutions.first() {
        resolution_attribute(
            &mut payload,
            "printer-resolution-default",
            horizontal,
            vertical,
        );
    }
    repeated_resolution_attribute(&mut payload, "printer-resolution-supported", &resolutions);
    let color_modes: &[&[u8]] = if remote
        .printer
        .capabilities
        .color_modes
        .contains(&ColorMode::Color)
    {
        &[b"monochrome", b"color"]
    } else {
        &[b"monochrome"]
    };
    attribute(
        &mut payload,
        0x44,
        "print-color-mode-default",
        color_modes[0],
    );
    repeated_attribute(
        &mut payload,
        0x44,
        "print-color-mode-supported",
        color_modes,
    );
    ipp_response(&request.version, IPP_OK, request.request_id, payload)
}

async fn print_job_response(
    bridge: &IppBridge,
    remote: &RemotePrinter,
    request: IppRequest,
    request_path: &std::path::Path,
) -> Response<Body> {
    if request.document_size == 0
        || !file_range_starts_with(request_path, request.document_offset, b"%PDF-")
            .await
            .unwrap_or(false)
    {
        return ipp_response(
            &request.version,
            IPP_BAD_REQUEST,
            request.request_id,
            operation_attributes(),
        );
    }
    let options = options_from_request(&request);
    let document_name = request
        .text("job-name")
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("ArcRelay Print Job");
    let digest =
        match sha256_file_range(request_path, request.document_offset, request.document_size).await
        {
            Ok(digest) => digest,
            Err(error) => {
                tracing::warn!(%error, "could not hash loopback IPP document");
                return http_error(StatusCode::INTERNAL_SERVER_ERROR);
            }
        };
    let client_job_id = ipp_client_job_id(&request, &digest);
    match bridge
        .network
        .submit_file_with_id(
            remote,
            client_job_id,
            document_name,
            request_path,
            request.document_offset,
            request.document_size,
            digest,
            options,
        )
        .await
    {
        Ok(job_id) => {
            let local_job_id = bridge.next_job_id.fetch_add(1, Ordering::Relaxed);
            bridge
                .record_job(local_job_id, remote, job_id.as_str(), document_name)
                .await;
            let mut payload = operation_attributes();
            payload.push(0x02);
            attribute(
                &mut payload,
                0x45,
                "job-uri",
                format!("ipp://127.0.0.1:{}/jobs/{local_job_id}", bridge.port).as_bytes(),
            );
            integer_attribute(&mut payload, "job-id", local_job_id as i32);
            enum_attribute(&mut payload, "job-state", 5);
            attribute(&mut payload, 0x44, "job-state-reasons", b"none");
            ipp_response(&request.version, IPP_OK, request.request_id, payload)
        }
        Err(error) => {
            tracing::warn!(%error, "forwarding loopback IPP job failed");
            ipp_response(
                &request.version,
                0x0500,
                request.request_id,
                operation_attributes(),
            )
        }
    }
}

fn ipp_client_job_id(request: &IppRequest, digest: &[u8; 32]) -> ClientJobId {
    if let Some(uuid) = request.text("job-uuid") {
        if let Ok(id) = ClientJobId::parse(uuid.to_owned()) {
            return id;
        }
    }
    ClientJobId::parse(format!(
        "ipp-{}-{}",
        request.request_id,
        digest[..8]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
    .expect("bounded IPP client job id")
}

fn options_from_request(request: &IppRequest) -> PrintOptions {
    let copies = request.integer("copies").unwrap_or(1).clamp(1, 100) as u32;
    let color_mode = match request.text("print-color-mode") {
        Some("color") => ColorMode::Color,
        _ => ColorMode::Monochrome,
    };
    let duplex_mode = match request.text("sides") {
        Some("two-sided-long-edge") => DuplexMode::TwoSidedLongEdge,
        Some("two-sided-short-edge") => DuplexMode::TwoSidedShortEdge,
        _ => DuplexMode::OneSided,
    };
    let orientation = match request.integer("orientation-requested") {
        Some(4) => Orientation::Landscape,
        _ => Orientation::Portrait,
    };
    PrintOptions {
        copies,
        color_mode,
        duplex_mode,
        orientation,
        media_name: normalize_media_name(request.text("media").unwrap_or("iso_a4_210x297mm")),
        page_ranges: request
            .ranges("page-ranges")
            .into_iter()
            .filter_map(|(first, last)| {
                (first > 0 && last >= first).then_some(PageRange {
                    first: first as u32,
                    last: last as u32,
                })
            })
            .collect(),
        collate: request.text("sheet-collate") == Some("collated")
            || request.text("multiple-document-handling")
                == Some("separate-documents-collated-copies"),
    }
}

fn normalize_media_name(media: &str) -> String {
    match media.to_ascii_lowercase().as_str() {
        // macOS CUPS uses the PPD page-size keywords in Print-Job requests,
        // while ArcRelay advertises and validates the corresponding PWG/IPP
        // media names.
        "a4" => "iso_a4_210x297mm".into(),
        "letter" | "usletter" => "na_letter_8.5x11in".into(),
        _ => media.to_owned(),
    }
}

#[cfg(test)]
mod tests;
