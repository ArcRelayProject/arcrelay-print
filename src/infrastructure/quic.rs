//! Public LAN printer discovery and the read-only portion of the print QUIC
//! protocol. Job upload streams build on this same authenticated session.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arcrelay_network::{NetworkRuntime, PeerAdvertisement, Session, SessionKind};
use arcrelay_peer::CapabilityId;
use arcrelay_transport::{read_frame, read_stream_kind, write_frame};
use arcrelay_wire::proto;
use arcrelay_wire::{
    MAX_PRINT_CONTROL_FRAME_SIZE, MAX_PRINT_DOCUMENT_CHUNK_SIZE, MAX_PRINT_DOCUMENT_SIZE,
};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{watch, Mutex, OwnedMutexGuard, RwLock, Semaphore};

use crate::application::{PrintJobRepository, PrintRuntime};
use crate::domain::{
    ClientJobId, DeviceId, PrintDocument, PrintJobId, PrintJobState, PrintOptions, PrinterStatus,
    PublishedPrinter,
};
use crate::infrastructure::wire::{
    decode_create_job, decode_printer, encode_document, encode_job, encode_options,
    encode_printer_list,
};
use crate::{PrintError, Result};

mod wire;

use wire::*;

const PRINT_REQUEST_TIMEOUT_MS: u32 = 30_000;
const MAX_PRINT_REQUEST_TIMEOUT_MS: u32 = 120_000;
const PRINT_UPLOAD_TICKET_TTL_MS: i64 = 5 * 60 * 1_000;
const PRINT_UPLOAD_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const PRINT_UPLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const PRINT_UPLOAD_TOTAL_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const PRINT_SESSION_DIRECTION_RETRY_TIMEOUT: Duration = Duration::from_secs(3);
const PRINT_SESSION_DIRECTION_RETRY_DELAY: Duration = Duration::from_millis(50);

static NEXT_PRINT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct PeerRequestGates {
    gates: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl PeerRequestGates {
    async fn lock(&self, peer_id: &str) -> OwnedMutexGuard<()> {
        let gate = {
            let mut gates = self.gates.lock().await;
            gates
                .entry(peer_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        gate.lock_owned().await
    }
}

#[derive(Debug, Clone)]
struct UploadTicket {
    token: Vec<u8>,
    expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct RemotePrinter {
    pub printer: PublishedPrinter,
    pub address: IpAddr,
    pub port: u16,
    pub certificate_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePrintJob {
    pub id: PrintJobId,
    pub state: PrintJobState,
    pub failure_message: Option<String>,
}

pub struct PrintQuicService {
    runtime: Arc<PrintRuntime>,
    network: Arc<NetworkRuntime>,
    remote_printers: RwLock<Vec<RemotePrinter>>,
    snapshots: watch::Sender<Arc<Vec<RemotePrinter>>>,
    control_requests: PeerRequestGates,
    upload_slots: Semaphore,
    upload_tickets: Mutex<HashMap<(String, String), UploadTicket>>,
}

impl PrintQuicService {
    pub async fn start(
        runtime: Arc<PrintRuntime>,
        network: Arc<NetworkRuntime>,
    ) -> Result<Arc<Self>> {
        let (snapshots, _) = watch::channel(Arc::new(Vec::new()));
        let service = Arc::new(Self {
            runtime,
            network,
            remote_printers: RwLock::new(Vec::new()),
            snapshots,
            control_requests: PeerRequestGates::default(),
            upload_slots: Semaphore::new(4),
            upload_tickets: Mutex::new(HashMap::new()),
        });
        service.spawn_server();
        service.spawn_discovery_observer();
        Ok(service)
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<Vec<RemotePrinter>>> {
        self.snapshots.subscribe()
    }

    pub async fn snapshot(&self) -> Vec<RemotePrinter> {
        self.remote_printers.read().await.clone()
    }

    pub async fn refresh(&self) -> Result<Vec<RemotePrinter>> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.query_discovered().await;
        Ok(self.snapshot().await)
    }

    pub async fn submit_document(
        &self,
        remote: &RemotePrinter,
        document_path: &Path,
        options: PrintOptions,
    ) -> Result<PrintJobId> {
        options.validate()?;
        let metadata = tokio::fs::metadata(document_path).await?;
        if metadata.len() > MAX_PRINT_DOCUMENT_SIZE {
            return Err(PrintError::Invalid("print document is too large".into()));
        }
        let digest = sha256_file(document_path, 0, metadata.len()).await?;
        let name = document_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("document.pdf");
        self.submit_file_with_id(
            remote,
            ClientJobId::parse(uuid::Uuid::new_v4().to_string())?,
            name,
            document_path,
            0,
            metadata.len(),
            digest,
            options,
        )
        .await
    }

    pub async fn get_remote_job(
        &self,
        remote: &RemotePrinter,
        job_id: &PrintJobId,
    ) -> Result<RemotePrintJob> {
        self.remote_job_request(
            remote,
            proto::print_client_frame::Body::GetJob(proto::PrintGetJobRequest {
                job_id: job_id.to_string(),
            }),
        )
        .await
    }

    pub async fn cancel_remote_job(
        &self,
        remote: &RemotePrinter,
        job_id: &PrintJobId,
    ) -> Result<RemotePrintJob> {
        self.remote_job_request(
            remote,
            proto::print_client_frame::Body::CancelJob(proto::PrintCancelJobRequest {
                job_id: job_id.to_string(),
            }),
        )
        .await
    }

    async fn remote_job_request(
        &self,
        remote: &RemotePrinter,
        body: proto::print_client_frame::Body,
    ) -> Result<RemotePrintJob> {
        let _request = self
            .control_requests
            .lock(remote.printer.source_device_id.as_str())
            .await;
        let (_session, _connection, mut send, mut receive) = self.connect_control(remote).await?;
        let frame = new_client_frame(body);
        let request_id = frame.request_id;
        write_client_frame(&mut send, frame).await?;
        let result = match read_correlated_server_frame(&mut receive, request_id)
            .await?
            .body
        {
            Some(proto::print_server_frame::Body::JobResult(result)) => result,
            Some(proto::print_server_frame::Body::Status(status)) => {
                require_ok(Some(status))?;
                return Err(PrintError::Backend("print job response is missing".into()));
            }
            _ => return Err(PrintError::Backend("print job response is missing".into())),
        };
        require_ok(result.status.clone())?;
        decode_remote_job(
            result
                .job
                .ok_or_else(|| PrintError::Backend("print job is missing".into()))?,
        )
    }

    pub async fn submit_bytes(
        &self,
        remote: &RemotePrinter,
        document_name: &str,
        bytes: &[u8],
        options: PrintOptions,
    ) -> Result<PrintJobId> {
        self.submit_bytes_with_id(
            remote,
            ClientJobId::parse(uuid::Uuid::new_v4().to_string())?,
            document_name,
            bytes,
            options,
        )
        .await
    }

    pub async fn submit_bytes_with_id(
        &self,
        remote: &RemotePrinter,
        client_job_id: ClientJobId,
        document_name: &str,
        bytes: &[u8],
        options: PrintOptions,
    ) -> Result<PrintJobId> {
        let _request = self
            .control_requests
            .lock(remote.printer.source_device_id.as_str())
            .await;
        options.validate()?;
        let document = PrintDocument {
            name: document_name.to_owned(),
            media_type: "application/pdf".into(),
            size_bytes: bytes.len() as u64,
            sha256: Sha256::digest(bytes).into(),
        };
        document.validate()?;
        let (_session, connection, mut send, mut receive) = self.connect_control(remote).await?;
        let frame = new_client_frame(proto::print_client_frame::Body::CreateJob(
            proto::PrintCreateJobRequest {
                client_job_id: client_job_id.to_string(),
                printer_share_id: remote.printer.share_id.to_string(),
                document: Some(encode_document(&document)),
                options: Some(encode_options(&options)),
            },
        ));
        let request_id = frame.request_id;
        write_client_frame(&mut send, frame).await?;
        let result = match read_correlated_server_frame(&mut receive, request_id)
            .await?
            .body
        {
            Some(proto::print_server_frame::Body::JobResult(result)) => result,
            Some(proto::print_server_frame::Body::Status(status)) => {
                require_ok(Some(status))?;
                return Err(PrintError::Backend("print job response is missing".into()));
            }
            _ => return Err(PrintError::Backend("print job response is missing".into())),
        };
        require_ok(result.status.clone())?;
        let job = result
            .job
            .as_ref()
            .ok_or_else(|| PrintError::Backend("print job id is missing".into()))?;
        let job_id = PrintJobId::parse(job.job_id.clone())?;
        match proto::PrintJobState::try_from(job.state) {
            Ok(proto::PrintJobState::Offered | proto::PrintJobState::Receiving) => {}
            Ok(
                proto::PrintJobState::Queued
                | proto::PrintJobState::Printing
                | proto::PrintJobState::Completed,
            ) => return Ok(job_id),
            Ok(state) => {
                return Err(PrintError::InvalidState(format!(
                    "remote print job cannot be uploaded in state {state:?}"
                )))
            }
            Err(_) => {
                return Err(PrintError::Backend(
                    "remote print job state is invalid".into(),
                ))
            }
        }
        let upload_ticket = require_upload_ticket(&result)?;
        self.upload_document(&connection, &job_id, &document, bytes, &upload_ticket)
            .await?;
        Ok(job_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn submit_file_with_id(
        &self,
        remote: &RemotePrinter,
        client_job_id: ClientJobId,
        document_name: &str,
        document_path: &Path,
        document_offset: u64,
        document_size: u64,
        document_sha256: [u8; 32],
        options: PrintOptions,
    ) -> Result<PrintJobId> {
        let _request = self
            .control_requests
            .lock(remote.printer.source_device_id.as_str())
            .await;
        options.validate()?;
        let document = PrintDocument {
            name: document_name.to_owned(),
            media_type: "application/pdf".into(),
            size_bytes: document_size,
            sha256: document_sha256,
        };
        document.validate()?;
        let (_session, connection, mut send, mut receive) = self.connect_control(remote).await?;
        let frame = new_client_frame(proto::print_client_frame::Body::CreateJob(
            proto::PrintCreateJobRequest {
                client_job_id: client_job_id.to_string(),
                printer_share_id: remote.printer.share_id.to_string(),
                document: Some(encode_document(&document)),
                options: Some(encode_options(&options)),
            },
        ));
        let request_id = frame.request_id;
        write_client_frame(&mut send, frame).await?;
        let result = match read_correlated_server_frame(&mut receive, request_id)
            .await?
            .body
        {
            Some(proto::print_server_frame::Body::JobResult(result)) => result,
            Some(proto::print_server_frame::Body::Status(status)) => {
                require_ok(Some(status))?;
                return Err(PrintError::Backend("print job response is missing".into()));
            }
            _ => return Err(PrintError::Backend("print job response is missing".into())),
        };
        require_ok(result.status.clone())?;
        let job = result
            .job
            .as_ref()
            .ok_or_else(|| PrintError::Backend("print job id is missing".into()))?;
        let job_id = PrintJobId::parse(job.job_id.clone())?;
        match proto::PrintJobState::try_from(job.state) {
            Ok(proto::PrintJobState::Offered | proto::PrintJobState::Receiving) => {
                let upload_ticket = require_upload_ticket(&result)?;
                self.upload_document_file(
                    &connection,
                    &job_id,
                    &document,
                    document_path,
                    document_offset,
                    &upload_ticket,
                )
                .await?;
            }
            Ok(
                proto::PrintJobState::Queued
                | proto::PrintJobState::Printing
                | proto::PrintJobState::Completed,
            ) => {}
            Ok(state) => {
                return Err(PrintError::InvalidState(format!(
                    "remote print job cannot be submitted in state {state:?}"
                )))
            }
            Err(_) => {
                return Err(PrintError::Backend(
                    "remote print job state is invalid".into(),
                ))
            }
        }
        Ok(job_id)
    }

    fn spawn_server(self: &Arc<Self>) {
        let service = self.clone();
        let mut incoming = self.network.subscribe();
        tokio::spawn(async move {
            while let Ok(session) = incoming.recv().await {
                if session.kind() != SessionKind::Print {
                    continue;
                }
                let service = service.clone();
                tokio::spawn(async move {
                    if let Err(error) = service
                        .network
                        .require(&session, CapabilityId::PrintSubmit)
                        .await
                    {
                        session.close("print capability denied");
                        tracing::warn!(%error, "incoming print authorization failed");
                        return;
                    }
                    if let Err(error) = service.handle_connection(session).await {
                        tracing::warn!(%error, "incoming print connection failed");
                    }
                });
            }
        });
    }

    fn spawn_discovery_observer(self: &Arc<Self>) {
        let service = self.clone();
        let mut receiver = self.network.discovery().subscribe();
        tokio::spawn(async move {
            loop {
                service.query_discovered().await;
                if receiver.changed().await.is_err() {
                    break;
                }
                receiver.borrow_and_update();
            }
        });
    }

    async fn query_discovered(&self) {
        let devices = self.network.discovery().snapshot();
        // Installed system queues can query the loopback IPP bridge at any
        // time. Keep the last successful printer metadata during transient
        // discovery or QUIC failures so those queries do not become a fatal
        // IPP not-found response that causes CUPS to stop the queue.
        let mut printers = cached_printers_for_refresh(self.remote_printers.read().await.clone());
        for device in devices.iter() {
            if device.device_id == self.network.device_id() {
                continue;
            }
            match self.query_device(device).await {
                Ok(found) => {
                    replace_device_printers(&mut printers, device.device_id.as_str(), found)
                }
                Err(error) => tracing::debug!(
                    event = "print.discovery.query_failed",
                    %error,
                    "could not query LAN printers; retaining cached metadata"
                ),
            }
        }
        printers.sort_by(|left, right| {
            left.printer
                .source_device_name
                .cmp(&right.printer.source_device_name)
                .then_with(|| left.printer.display_name.cmp(&right.printer.display_name))
        });
        *self.remote_printers.write().await = printers.clone();
        self.snapshots.send_replace(Arc::new(printers));
    }

    async fn query_device(&self, device: &PeerAdvertisement) -> Result<Vec<RemotePrinter>> {
        let address = device
            .addresses
            .first()
            .copied()
            .ok_or_else(|| PrintError::NotFound("print endpoint address".into()))?;
        self.query_device_at(device, address).await
    }

    async fn query_device_at(
        &self,
        device: &PeerAdvertisement,
        address: IpAddr,
    ) -> Result<Vec<RemotePrinter>> {
        let _request = self.control_requests.lock(device.device_id.as_str()).await;
        let (_session, _connection, mut send, mut receive) =
            self.connect_device(&device.device_id).await?;
        let frame = new_client_frame(proto::print_client_frame::Body::ListPrinters(
            proto::PrintListPrintersRequest {},
        ));
        let request_id = frame.request_id;
        write_client_frame(&mut send, frame).await?;
        let list = match read_correlated_server_frame(&mut receive, request_id)
            .await?
            .body
        {
            Some(proto::print_server_frame::Body::PrinterList(list)) => list,
            _ => {
                return Err(PrintError::Backend(
                    "printer list response is missing".into(),
                ))
            }
        };
        require_ok(list.status.clone())?;
        let source_id = DeviceId::parse(list.source_device_id)?;
        list.printers
            .into_iter()
            .map(|printer| {
                Ok(RemotePrinter {
                    printer: decode_printer(&source_id, &list.source_device_name, printer)?,
                    address,
                    port: device.port,
                    certificate_sha256: device
                        .certificate_sha256
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect(),
                })
            })
            .collect()
    }

    async fn connect_control(
        &self,
        remote: &RemotePrinter,
    ) -> Result<(
        Arc<Session>,
        quinn::Connection,
        quinn::SendStream,
        quinn::RecvStream,
    )> {
        let peer_id = arcrelay_peer::DeviceId::parse(remote.printer.source_device_id.to_string())
            .map_err(|_| PrintError::Invalid("invalid remote print device id".into()))?;
        self.connect_device(&peer_id).await
    }

    async fn connect_device(
        &self,
        peer_id: &arcrelay_peer::DeviceId,
    ) -> Result<(
        Arc<Session>,
        quinn::Connection,
        quinn::SendStream,
        quinn::RecvStream,
    )> {
        let local_device_id = self.network.device_id();
        let retry_started = tokio::time::Instant::now();
        loop {
            let session = self
                .network
                .connect_discovered(peer_id, SessionKind::Print)
                .await
                .map_err(|error| PrintError::Backend(error.to_string()))?;
            if session.initiator_id() == &local_device_id {
                let stream = session
                    .open_feature_stream("arcrelay.print", &[])
                    .await
                    .map_err(|error| PrintError::Backend(error.to_string()))?;
                let connection = session.transport_handle();
                return Ok((session, connection, stream.send, stream.receive));
            }

            // Simultaneous discovery can leave the peer-initiated session as
            // the deterministic winner. Its primary stream is owned by the
            // inbound server; opening another stream here would be parsed as
            // a document upload and the control request would time out.
            drop(session);
            if retry_started.elapsed() >= PRINT_SESSION_DIRECTION_RETRY_TIMEOUT {
                return Err(PrintError::Backend(
                    "peer currently owns the print session; retry discovery shortly".into(),
                ));
            }
            tokio::time::sleep(PRINT_SESSION_DIRECTION_RETRY_DELAY).await;
        }
    }

    async fn upload_document(
        &self,
        connection: &quinn::Connection,
        job_id: &PrintJobId,
        document: &PrintDocument,
        bytes: &[u8],
        upload_ticket: &[u8],
    ) -> Result<()> {
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| network_error("open print document stream", error))?;
        send.write_u8(arcrelay_wire::STREAM_KIND_PRINT_DOCUMENT_UPLOAD)
            .await?;
        write_encoded(
            &mut send,
            proto::PrintDocumentOpen {
                job_id: job_id.to_string(),
                size_bytes: document.size_bytes,
                sha256: document.sha256.to_vec(),
                upload_ticket: upload_ticket.to_vec(),
            },
            "write print document metadata",
        )
        .await?;
        for (index, chunk) in bytes.chunks(MAX_PRINT_DOCUMENT_CHUNK_SIZE).enumerate() {
            let frame = proto::PrintDocumentChunk {
                offset: (index * MAX_PRINT_DOCUMENT_CHUNK_SIZE) as u64,
                data: chunk.to_vec().into(),
            }
            .encode_to_vec();
            write_frame(&mut send, &frame, MAX_PRINT_DOCUMENT_CHUNK_SIZE + 128)
                .await
                .map_err(|error| frame_error(error, "write print document chunk"))?;
        }
        send.finish()
            .map_err(|error| network_error("finish print document stream", error))?;
        let response = read_frame(&mut receive, MAX_PRINT_CONTROL_FRAME_SIZE)
            .await
            .map_err(|error| frame_error(error, "read print upload response"))?;
        let result =
            proto::PrintDocumentUploadResult::decode(response.as_slice()).map_err(|error| {
                PrintError::Backend(format!("decode print upload response: {error}"))
            })?;
        require_ok(result.status)
    }

    async fn upload_document_file(
        &self,
        connection: &quinn::Connection,
        job_id: &PrintJobId,
        document: &PrintDocument,
        path: &Path,
        offset: u64,
        upload_ticket: &[u8],
    ) -> Result<()> {
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .map_err(|error| network_error("open print document stream", error))?;
        send.write_u8(arcrelay_wire::STREAM_KIND_PRINT_DOCUMENT_UPLOAD)
            .await?;
        write_encoded(
            &mut send,
            proto::PrintDocumentOpen {
                job_id: job_id.to_string(),
                size_bytes: document.size_bytes,
                sha256: document.sha256.to_vec(),
                upload_ticket: upload_ticket.to_vec(),
            },
            "write print document metadata",
        )
        .await?;
        let mut file = tokio::fs::File::open(path).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut written = 0_u64;
        let mut buffer = vec![0_u8; MAX_PRINT_DOCUMENT_CHUNK_SIZE];
        while written < document.size_bytes {
            let remaining = (document.size_bytes - written) as usize;
            let limit = remaining.min(buffer.len());
            let read = file.read(&mut buffer[..limit]).await?;
            if read == 0 {
                return Err(PrintError::Invalid(
                    "print document ended before its declared size".into(),
                ));
            }
            let frame = proto::PrintDocumentChunk {
                offset: written,
                data: buffer[..read].to_vec().into(),
            }
            .encode_to_vec();
            write_frame(&mut send, &frame, MAX_PRINT_DOCUMENT_CHUNK_SIZE + 128)
                .await
                .map_err(|error| frame_error(error, "write print document chunk"))?;
            written += read as u64;
        }
        send.finish()
            .map_err(|error| network_error("finish print document stream", error))?;
        let response = read_frame(&mut receive, MAX_PRINT_CONTROL_FRAME_SIZE)
            .await
            .map_err(|error| frame_error(error, "read print upload response"))?;
        let result =
            proto::PrintDocumentUploadResult::decode(response.as_slice()).map_err(|error| {
                PrintError::Backend(format!("decode print upload response: {error}"))
            })?;
        require_ok(result.status)
    }

    async fn handle_connection(self: &Arc<Self>, session: Arc<Session>) -> Result<()> {
        let stream = session
            .accept_feature_stream()
            .await
            .map_err(|error| PrintError::Backend(error.to_string()))?;
        if stream.feature_id != "arcrelay.print" || stream.negotiate_minor(1, 0, 0).is_err() {
            return Err(PrintError::Invalid(
                "unexpected print feature stream".into(),
            ));
        }
        let mut send = stream.send;
        let mut receive = stream.receive;
        let connection = session.transport_handle();
        let source_device_id = DeviceId::parse(session.peer().device_id.to_string())?;
        let upload_source_device_id = source_device_id.clone();
        let upload_service = self.clone();
        let upload_connection = connection.clone();
        tokio::spawn(async move {
            while let Ok((send, receive)) = upload_connection.accept_bi().await {
                let service = upload_service.clone();
                let source_device_id = upload_source_device_id.clone();
                tokio::spawn(async move {
                    match tokio::time::timeout(
                        PRINT_UPLOAD_TOTAL_TIMEOUT,
                        service.handle_document_stream(source_device_id, send, receive),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            tracing::warn!(%error, "print document upload failed")
                        }
                        Err(_) => tracing::warn!("print document upload exceeded total timeout"),
                    }
                });
            }
        });

        loop {
            let frame = match read_client_frame(&mut receive).await {
                Ok(frame) => frame,
                Err(PrintError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(())
                }
                Err(error) => return Err(error),
            };
            let request_id = frame.request_id;
            if request_id == 0
                || frame.timeout_ms == 0
                || frame.timeout_ms > MAX_PRINT_REQUEST_TIMEOUT_MS
            {
                write_server_frame(
                    &mut send,
                    proto::PrintServerFrame {
                        request_id,
                        body: Some(proto::print_server_frame::Body::Status(error_status(
                            proto::ErrorCode::InvalidArgument,
                            "print request_id must be non-zero and timeout_ms must be in range",
                        ))),
                    },
                )
                .await?;
                continue;
            }
            let response =
                match tokio::time::timeout(Duration::from_millis(frame.timeout_ms as u64), async {
                    Ok::<_, PrintError>(match frame.body {
                        Some(proto::print_client_frame::Body::ListPrinters(_)) => {
                            let device_id = DeviceId::parse(self.network.device_id().to_string())?;
                            let device_name = self.network.metadata().name;
                            let printers = self
                                .runtime
                                .shares
                                .list_published(&device_id, &device_name)
                                .await?;
                            proto::PrintServerFrame {
                                request_id,
                                body: Some(proto::print_server_frame::Body::PrinterList(
                                    encode_printer_list(
                                        &device_id,
                                        &device_name,
                                        &printers,
                                        ok_status(),
                                    ),
                                )),
                            }
                        }
                        Some(proto::print_client_frame::Body::CreateJob(request)) => {
                            let result = match decode_create_job(request) {
                                Ok(offer) => match self
                                    .runtime
                                    .jobs
                                    .offer(
                                        offer.client_job_id,
                                        source_device_id.clone(),
                                        offer.printer_share_id,
                                        offer.document,
                                        offer.options,
                                    )
                                    .await
                                {
                                    Ok(mut job) => {
                                        let resume = match job.state {
                                            PrintJobState::Validating => {
                                                match self.runtime.jobs.mark_valid(&job.id).await {
                                                    Ok(ready) => {
                                                        job = ready;
                                                        self.submit_ready_job(&job).await
                                                    }
                                                    Err(error) => Err(error),
                                                }
                                            }
                                            PrintJobState::Ready => {
                                                self.submit_ready_job(&job).await
                                            }
                                            _ => Ok(job.clone()),
                                        };
                                        match resume {
                                            Ok(job) => {
                                                let ticket = self
                                                    .issue_upload_ticket(&source_device_id, &job)
                                                    .await;
                                                proto::PrintJobResult {
                                                    status: Some(ok_status()),
                                                    job: Some(encode_job(&job)),
                                                    upload_ticket: ticket
                                                        .as_ref()
                                                        .map(|ticket| ticket.token.clone())
                                                        .unwrap_or_default(),
                                                    upload_ticket_expires_at_ms: ticket
                                                        .map(|ticket| ticket.expires_at_ms)
                                                        .unwrap_or_default(),
                                                }
                                            }
                                            Err(error) => proto::PrintJobResult {
                                                status: Some(error_status(
                                                    proto::ErrorCode::Internal,
                                                    error.to_string(),
                                                )),
                                                job: None,
                                                ..Default::default()
                                            },
                                        }
                                    }
                                    Err(error) => proto::PrintJobResult {
                                        status: Some(error_status(
                                            proto::ErrorCode::FailedPrecondition,
                                            error.to_string(),
                                        )),
                                        job: None,
                                        ..Default::default()
                                    },
                                },
                                Err(error) => proto::PrintJobResult {
                                    status: Some(error_status(
                                        proto::ErrorCode::InvalidArgument,
                                        error.to_string(),
                                    )),
                                    job: None,
                                    ..Default::default()
                                },
                            };
                            proto::PrintServerFrame {
                                request_id,
                                body: Some(proto::print_server_frame::Body::JobResult(result)),
                            }
                        }
                        Some(proto::print_client_frame::Body::GetJob(request)) => {
                            let result = match PrintJobId::parse(request.job_id) {
                                Ok(id) => match self
                                    .runtime
                                    .jobs
                                    .refresh_owned(&id, &source_device_id)
                                    .await
                                {
                                    Ok(job) => proto::PrintJobResult {
                                        status: Some(ok_status()),
                                        job: Some(encode_job(&job)),
                                        ..Default::default()
                                    },
                                    Err(error) => proto::PrintJobResult {
                                        status: Some(error_status(
                                            proto::ErrorCode::NotFound,
                                            error.to_string(),
                                        )),
                                        job: None,
                                        ..Default::default()
                                    },
                                },
                                Err(error) => proto::PrintJobResult {
                                    status: Some(error_status(
                                        proto::ErrorCode::InvalidArgument,
                                        error.to_string(),
                                    )),
                                    job: None,
                                    ..Default::default()
                                },
                            };
                            proto::PrintServerFrame {
                                request_id,
                                body: Some(proto::print_server_frame::Body::JobResult(result)),
                            }
                        }
                        Some(proto::print_client_frame::Body::CancelJob(request)) => {
                            let result = match PrintJobId::parse(request.job_id) {
                                Ok(id) => {
                                    match self
                                        .runtime
                                        .jobs
                                        .cancel_owned(&id, &source_device_id)
                                        .await
                                    {
                                        Ok(job) => proto::PrintJobResult {
                                            status: Some(ok_status()),
                                            job: Some(encode_job(&job)),
                                            ..Default::default()
                                        },
                                        Err(error) => proto::PrintJobResult {
                                            status: Some(error_status(
                                                proto::ErrorCode::FailedPrecondition,
                                                error.to_string(),
                                            )),
                                            job: None,
                                            ..Default::default()
                                        },
                                    }
                                }
                                Err(error) => proto::PrintJobResult {
                                    status: Some(error_status(
                                        proto::ErrorCode::InvalidArgument,
                                        error.to_string(),
                                    )),
                                    job: None,
                                    ..Default::default()
                                },
                            };
                            proto::PrintServerFrame {
                                request_id,
                                body: Some(proto::print_server_frame::Body::JobResult(result)),
                            }
                        }
                        _ => proto::PrintServerFrame {
                            request_id,
                            body: Some(proto::print_server_frame::Body::Status(error_status(
                                proto::ErrorCode::Unsupported,
                                "this print request is not implemented yet",
                            ))),
                        },
                    })
                })
                .await
                {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => proto::PrintServerFrame {
                        request_id,
                        body: Some(proto::print_server_frame::Body::Status(error_status(
                            proto::ErrorCode::Internal,
                            error.to_string(),
                        ))),
                    },
                    Err(_) => proto::PrintServerFrame {
                        request_id,
                        body: Some(proto::print_server_frame::Body::Status(error_status(
                            proto::ErrorCode::DeadlineExceeded,
                            "print request deadline exceeded",
                        ))),
                    },
                };
            write_server_frame(&mut send, response).await?;
        }
    }

    async fn handle_document_stream(
        &self,
        source_device_id: DeviceId,
        mut send: quinn::SendStream,
        mut receive: quinn::RecvStream,
    ) -> Result<()> {
        let _upload_slot =
            tokio::time::timeout(PRINT_UPLOAD_SETUP_TIMEOUT, self.upload_slots.acquire())
                .await
                .map_err(|_| PrintError::Backend("print upload capacity wait timed out".into()))?
                .map_err(|_| PrintError::Backend("print upload service is stopping".into()))?;
        if tokio::time::timeout(PRINT_UPLOAD_SETUP_TIMEOUT, read_stream_kind(&mut receive))
            .await
            .map_err(|_| PrintError::Backend("print upload setup timed out".into()))?
            .map_err(|error| frame_error(error, "read print document stream kind"))?
            != arcrelay_wire::STREAM_KIND_PRINT_DOCUMENT_UPLOAD
        {
            return Err(PrintError::Invalid(
                "unexpected print upload stream kind".into(),
            ));
        }
        let open = proto::PrintDocumentOpen::decode(
            tokio::time::timeout(
                PRINT_UPLOAD_SETUP_TIMEOUT,
                read_frame(&mut receive, MAX_PRINT_CONTROL_FRAME_SIZE),
            )
            .await
            .map_err(|_| PrintError::Backend("print upload metadata timed out".into()))?
            .map_err(|error| frame_error(error, "read print document metadata"))?
            .as_slice(),
        )
        .map_err(|error| PrintError::Backend(format!("decode print document metadata: {error}")))?;
        let job_id = PrintJobId::parse(open.job_id.clone())?;
        let job = self
            .runtime
            .store()
            .find(&job_id)
            .await?
            .ok_or_else(|| PrintError::NotFound(job_id.to_string()))?;
        if job.source_device_id != source_device_id {
            return Err(PrintError::Invalid(
                "print job belongs to a different device".into(),
            ));
        }
        if open.size_bytes != job.document.size_bytes
            || open.sha256.as_slice() != job.document.sha256.as_slice()
        {
            return Err(PrintError::Invalid(
                "print upload metadata differs from job".into(),
            ));
        }
        self.consume_upload_ticket(&source_device_id, &job_id, &open.upload_ticket)
            .await?;
        self.runtime.jobs.begin_receiving(&job_id).await?;
        let mut writer = self.runtime.spool.begin(&job).await?;
        let mut received = 0_u64;
        while received < open.size_bytes {
            let bytes = tokio::time::timeout(
                PRINT_UPLOAD_IDLE_TIMEOUT,
                read_frame(&mut receive, MAX_PRINT_DOCUMENT_CHUNK_SIZE + 128),
            )
            .await
            .map_err(|_| PrintError::Backend("print upload idle timeout".into()))?
            .map_err(|error| frame_error(error, "read print document chunk"))?;
            let chunk = proto::PrintDocumentChunk::decode(bytes.as_slice()).map_err(|error| {
                PrintError::Backend(format!("decode print document chunk: {error}"))
            })?;
            writer.write_chunk(chunk.offset, &chunk.data).await?;
            received = received
                .checked_add(chunk.data.len() as u64)
                .ok_or_else(|| PrintError::Invalid("print upload size overflow".into()))?;
        }
        let ready_path = writer.finish().await?;
        let digest: [u8; 32] = open
            .sha256
            .as_slice()
            .try_into()
            .map_err(|_| PrintError::Invalid("invalid print upload digest".into()))?;
        self.runtime.jobs.document_received(&job_id, digest).await?;
        self.runtime.jobs.mark_valid(&job_id).await?;
        drop(_upload_slot);
        let result = self.runtime.jobs.submit(&job_id, &ready_path).await;
        if result.is_ok() {
            if let Err(error) = self.runtime.spool.delete_ready(&job_id).await {
                tracing::warn!(%error, "could not remove submitted print spool document");
            }
        }
        write_encoded(
            &mut send,
            proto::PrintDocumentUploadResult {
                status: Some(match result {
                    Ok(_) => ok_status(),
                    Err(error) => error_status(proto::ErrorCode::Internal, error.to_string()),
                }),
                job_id: job_id.to_string(),
                received_bytes: received,
                sha256: digest.to_vec(),
            },
            "write print upload response",
        )
        .await?;
        send.finish()
            .map_err(|error| network_error("finish print upload response", error))?;
        Ok(())
    }

    async fn issue_upload_ticket(
        &self,
        source_device_id: &DeviceId,
        job: &crate::domain::PrintJob,
    ) -> Option<UploadTicket> {
        if !matches!(job.state, PrintJobState::Offered | PrintJobState::Receiving) {
            return None;
        }
        let mut tickets = self.upload_tickets.lock().await;
        let now = unix_time_ms();
        tickets.retain(|_, existing| existing.expires_at_ms > now);
        let key = (source_device_id.to_string(), job.id.to_string());
        if let Some(ticket) = tickets.get(&key) {
            return Some(ticket.clone());
        }
        let ticket = UploadTicket {
            token: uuid::Uuid::new_v4().as_bytes().to_vec(),
            expires_at_ms: now.saturating_add(PRINT_UPLOAD_TICKET_TTL_MS),
        };
        tickets.insert(key, ticket.clone());
        Some(ticket)
    }

    async fn consume_upload_ticket(
        &self,
        source_device_id: &DeviceId,
        job_id: &PrintJobId,
        provided: &[u8],
    ) -> Result<()> {
        let key = (source_device_id.to_string(), job_id.to_string());
        let mut tickets = self.upload_tickets.lock().await;
        let now = unix_time_ms();
        tickets.retain(|_, ticket| ticket.expires_at_ms > now);
        let valid = tickets
            .get(&key)
            .is_some_and(|ticket| ticket.token.as_slice() == provided);
        if !valid {
            return Err(PrintError::Invalid(
                "print upload ticket is invalid, expired, or already used".into(),
            ));
        }
        tickets.remove(&key);
        Ok(())
    }

    async fn submit_ready_job(
        &self,
        job: &crate::domain::PrintJob,
    ) -> Result<crate::domain::PrintJob> {
        let ready_path = self.runtime.spool.ready_path(&job.id)?;
        if !tokio::fs::try_exists(&ready_path).await? {
            return Err(PrintError::InvalidState(
                "ready print document is missing from the spool".into(),
            ));
        }
        let submitted = self.runtime.jobs.submit(&job.id, &ready_path).await?;
        if let Err(error) = self.runtime.spool.delete_ready(&job.id).await {
            tracing::warn!(
                job_id = %job.id,
                %error,
                "could not remove submitted print spool document"
            );
        }
        Ok(submitted)
    }
}

fn unix_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests;
