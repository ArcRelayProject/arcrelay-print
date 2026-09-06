use super::*;

pub(super) fn cached_printers_for_refresh(mut printers: Vec<RemotePrinter>) -> Vec<RemotePrinter> {
    for printer in &mut printers {
        printer.printer.status = PrinterStatus::Offline;
    }
    printers
}

pub(super) fn replace_device_printers(
    printers: &mut Vec<RemotePrinter>,
    device_id: &str,
    mut found: Vec<RemotePrinter>,
) {
    printers.retain(|printer| printer.printer.source_device_id.as_str() != device_id);
    printers.append(&mut found);
}

pub(super) fn decode_remote_job(value: proto::PrintJobInfo) -> Result<RemotePrintJob> {
    let state = match proto::PrintJobState::try_from(value.state) {
        Ok(proto::PrintJobState::Offered) => PrintJobState::Offered,
        Ok(proto::PrintJobState::Receiving) => PrintJobState::Receiving,
        Ok(proto::PrintJobState::Validating) => PrintJobState::Validating,
        Ok(proto::PrintJobState::Ready) => PrintJobState::Ready,
        Ok(proto::PrintJobState::Submitting) => PrintJobState::Submitting,
        Ok(proto::PrintJobState::Queued) => PrintJobState::Queued,
        Ok(proto::PrintJobState::Printing) => PrintJobState::Printing,
        Ok(proto::PrintJobState::Completed) => PrintJobState::Completed,
        Ok(proto::PrintJobState::Held) => PrintJobState::Held,
        Ok(proto::PrintJobState::Cancelled) => PrintJobState::Cancelled,
        Ok(proto::PrintJobState::Failed) => PrintJobState::Failed,
        Ok(proto::PrintJobState::Ambiguous) => PrintJobState::Ambiguous,
        _ => return Err(PrintError::Invalid("invalid remote print job state".into())),
    };
    Ok(RemotePrintJob {
        id: PrintJobId::parse(value.job_id)?,
        state,
        failure_message: value.failure_message,
    })
}

pub(super) async fn read_client_frame(
    receive: &mut quinn::RecvStream,
) -> Result<proto::PrintClientFrame> {
    let bytes = read_frame(receive, MAX_PRINT_CONTROL_FRAME_SIZE)
        .await
        .map_err(|error| frame_error(error, "read print client frame"))?;
    proto::PrintClientFrame::decode(bytes.as_slice())
        .map_err(|error| PrintError::Backend(format!("decode print client frame: {error}")))
}

pub(super) async fn read_server_frame(
    receive: &mut quinn::RecvStream,
) -> Result<proto::PrintServerFrame> {
    let bytes = read_frame(receive, MAX_PRINT_CONTROL_FRAME_SIZE)
        .await
        .map_err(|error| frame_error(error, "read print server frame"))?;
    proto::PrintServerFrame::decode(bytes.as_slice())
        .map_err(|error| PrintError::Backend(format!("decode print server frame: {error}")))
}

pub(super) fn new_client_frame(body: proto::print_client_frame::Body) -> proto::PrintClientFrame {
    let mut request_id = NEXT_PRINT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    if request_id == 0 {
        request_id = NEXT_PRINT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    }
    proto::PrintClientFrame {
        request_id,
        timeout_ms: PRINT_REQUEST_TIMEOUT_MS,
        body: Some(body),
    }
}

pub(super) async fn read_correlated_server_frame(
    receive: &mut quinn::RecvStream,
    request_id: u64,
) -> Result<proto::PrintServerFrame> {
    let frame = tokio::time::timeout(
        Duration::from_millis(PRINT_REQUEST_TIMEOUT_MS as u64),
        read_server_frame(receive),
    )
    .await
    .map_err(|_| PrintError::Backend("print request timed out".into()))??;
    if frame.request_id != request_id {
        return Err(PrintError::Invalid(format!(
            "print response id {} does not match request {request_id}",
            frame.request_id
        )));
    }
    Ok(frame)
}

pub(super) async fn write_client_frame(
    send: &mut quinn::SendStream,
    frame: proto::PrintClientFrame,
) -> Result<()> {
    write_encoded(send, frame, "write print client frame").await
}

pub(super) async fn write_server_frame(
    send: &mut quinn::SendStream,
    frame: proto::PrintServerFrame,
) -> Result<()> {
    write_encoded(send, frame, "write print server frame").await
}

pub(super) async fn write_encoded(
    send: &mut quinn::SendStream,
    frame: impl Message,
    context: &str,
) -> Result<()> {
    let bytes = frame.encode_to_vec();
    write_frame(send, &bytes, MAX_PRINT_CONTROL_FRAME_SIZE)
        .await
        .map_err(|error| frame_error(error, context))
}

pub(super) fn frame_error(error: arcrelay_transport::FrameError, context: &str) -> PrintError {
    match error {
        arcrelay_transport::FrameError::Io(error) => PrintError::Io(error),
        other => PrintError::Backend(format!("{context}: {other}")),
    }
}

pub(super) fn ok_status() -> proto::Status {
    proto::Status {
        code: proto::ErrorCode::Ok as i32,
        message: String::new(),
        retryable: false,
        retry_after_ms: 0,
        recovery_action: proto::RecoveryAction::None as i32,
        error_id: String::new(),
    }
}

pub(super) fn require_upload_ticket(result: &proto::PrintJobResult) -> Result<Vec<u8>> {
    if result.upload_ticket.is_empty() || result.upload_ticket_expires_at_ms <= 0 {
        return Err(PrintError::Backend(
            "print job response is missing its upload ticket".into(),
        ));
    }
    Ok(result.upload_ticket.to_vec())
}

pub(super) fn error_status(code: proto::ErrorCode, message: impl Into<String>) -> proto::Status {
    let recovery_action = match code {
        proto::ErrorCode::Busy
        | proto::ErrorCode::ResourceExhausted
        | proto::ErrorCode::DeadlineExceeded
        | proto::ErrorCode::Unavailable => proto::RecoveryAction::RetryWithBackoff,
        proto::ErrorCode::Conflict => proto::RecoveryAction::Refresh,
        proto::ErrorCode::InvalidArgument | proto::ErrorCode::FailedPrecondition => {
            proto::RecoveryAction::ChangeRequest
        }
        _ => proto::RecoveryAction::None,
    };
    proto::Status {
        code: code as i32,
        message: message.into(),
        retryable: matches!(
            recovery_action,
            proto::RecoveryAction::Retry | proto::RecoveryAction::RetryWithBackoff
        ),
        retry_after_ms: 0,
        recovery_action: recovery_action as i32,
        error_id: String::new(),
    }
}

pub(super) fn require_ok(status: Option<proto::Status>) -> Result<()> {
    let status = status.ok_or_else(|| PrintError::Backend("print status is missing".into()))?;
    if status.code == proto::ErrorCode::Ok as i32 {
        Ok(())
    } else {
        Err(PrintError::Backend(if status.message.is_empty() {
            "print request failed".into()
        } else {
            status.message
        }))
    }
}

pub(super) fn network_error(context: &str, error: impl std::fmt::Display) -> PrintError {
    PrintError::Backend(format!("{context}: {error}"))
}

pub(super) async fn sha256_file(path: &Path, offset: u64, size: u64) -> Result<[u8; 32]> {
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut remaining = size;
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut hasher = Sha256::new();
    while remaining > 0 {
        let limit = (remaining as usize).min(buffer.len());
        let read = file.read(&mut buffer[..limit]).await?;
        if read == 0 {
            return Err(PrintError::Invalid(
                "print document ended before its declared size".into(),
            ));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(hasher.finalize().into())
}
