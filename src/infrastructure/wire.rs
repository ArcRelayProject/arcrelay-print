use arcrelay_wire::proto;

use crate::domain::{
    ClientJobId, ColorMode, DeviceId, DuplexMode, MediaSize, Orientation, PageRange, PrintDocument,
    PrintJob, PrintOptions, PrintResolution, PrinterCapabilities, PrinterShareId, PrinterStatus,
    PublishedPrinter,
};
use crate::{PrintError, Result};

#[derive(Debug, Clone)]
pub struct CreateJobOffer {
    pub client_job_id: ClientJobId,
    pub printer_share_id: PrinterShareId,
    pub document: PrintDocument,
    pub options: PrintOptions,
}

pub fn encode_document(document: &PrintDocument) -> proto::PrintDocumentMetadata {
    proto::PrintDocumentMetadata {
        name: document.name.clone(),
        media_type: document.media_type.clone(),
        size_bytes: document.size_bytes,
        sha256: document.sha256.to_vec(),
    }
}

pub fn encode_options(options: &PrintOptions) -> proto::PrintJobOptions {
    proto::PrintJobOptions {
        copies: options.copies,
        color_mode: encode_color_mode(options.color_mode) as i32,
        duplex_mode: encode_duplex_mode(options.duplex_mode) as i32,
        orientation: match options.orientation {
            Orientation::Portrait => proto::PrintOrientation::Portrait,
            Orientation::Landscape => proto::PrintOrientation::Landscape,
        } as i32,
        media_name: options.media_name.clone(),
        page_ranges: options
            .page_ranges
            .iter()
            .map(|range| proto::PrintPageRange {
                first: range.first,
                last: range.last,
            })
            .collect(),
        collate: options.collate,
    }
}

pub fn decode_create_job(request: proto::PrintCreateJobRequest) -> Result<CreateJobOffer> {
    let document = request
        .document
        .ok_or_else(|| PrintError::Invalid("print job document is missing".into()))?;
    let sha256: [u8; 32] = document
        .sha256
        .as_slice()
        .try_into()
        .map_err(|_| PrintError::Invalid("print document SHA-256 must be 32 bytes".into()))?;
    let document = PrintDocument {
        name: document.name,
        media_type: document.media_type,
        size_bytes: document.size_bytes,
        sha256,
    };
    document.validate()?;

    let options = request
        .options
        .ok_or_else(|| PrintError::Invalid("print job options are missing".into()))?;
    let options = PrintOptions {
        copies: options.copies,
        color_mode: decode_color_mode(options.color_mode)?,
        duplex_mode: decode_duplex_mode(options.duplex_mode)?,
        orientation: decode_orientation(options.orientation)?,
        media_name: options.media_name,
        page_ranges: options
            .page_ranges
            .into_iter()
            .map(|range| PageRange {
                first: range.first,
                last: range.last,
            })
            .collect(),
        collate: options.collate,
    };
    options.validate()?;

    Ok(CreateJobOffer {
        client_job_id: ClientJobId::parse(request.client_job_id)?,
        printer_share_id: PrinterShareId::parse(request.printer_share_id)?,
        document,
        options,
    })
}

pub fn encode_printer(printer: &PublishedPrinter) -> proto::PrintPublishedPrinter {
    proto::PrintPublishedPrinter {
        share_id: printer.share_id.to_string(),
        display_name: printer.display_name.clone(),
        state: encode_printer_state(printer.status) as i32,
        capabilities: Some(encode_capabilities(&printer.capabilities)),
        capability_revision: printer.capability_revision,
    }
}

pub fn decode_printer(
    source_device_id: &DeviceId,
    source_device_name: &str,
    printer: proto::PrintPublishedPrinter,
) -> Result<PublishedPrinter> {
    let status = match proto::PrintPrinterState::try_from(printer.state).ok() {
        Some(proto::PrintPrinterState::Ready) => PrinterStatus::Ready,
        Some(proto::PrintPrinterState::Busy) => PrinterStatus::Busy,
        Some(proto::PrintPrinterState::Offline) => PrinterStatus::Offline,
        Some(proto::PrintPrinterState::Error) => PrinterStatus::Error,
        _ => PrinterStatus::Unknown,
    };
    Ok(PublishedPrinter {
        source_device_id: source_device_id.clone(),
        source_device_name: source_device_name.to_owned(),
        share_id: PrinterShareId::parse(printer.share_id)?,
        display_name: printer.display_name,
        status,
        capabilities: decode_capabilities(
            printer
                .capabilities
                .ok_or_else(|| PrintError::Invalid("printer capabilities are missing".into()))?,
        )?,
        capability_revision: printer.capability_revision,
    })
}

pub fn encode_printer_list(
    device_id: &DeviceId,
    device_name: &str,
    printers: &[PublishedPrinter],
    status: proto::Status,
) -> proto::PrintPrinterList {
    proto::PrintPrinterList {
        status: Some(status),
        source_device_id: device_id.to_string(),
        source_device_name: device_name.to_string(),
        printers: printers.iter().map(encode_printer).collect(),
    }
}

pub fn encode_job(job: &PrintJob) -> proto::PrintJobInfo {
    proto::PrintJobInfo {
        job_id: job.id.to_string(),
        client_job_id: job.client_job_id.to_string(),
        printer_share_id: job.printer_share_id.to_string(),
        state: match job.state {
            crate::domain::PrintJobState::Offered => proto::PrintJobState::Offered,
            crate::domain::PrintJobState::Receiving => proto::PrintJobState::Receiving,
            crate::domain::PrintJobState::Validating => proto::PrintJobState::Validating,
            crate::domain::PrintJobState::Ready => proto::PrintJobState::Ready,
            crate::domain::PrintJobState::Submitting => proto::PrintJobState::Submitting,
            crate::domain::PrintJobState::Queued => proto::PrintJobState::Queued,
            crate::domain::PrintJobState::Printing => proto::PrintJobState::Printing,
            crate::domain::PrintJobState::Completed => proto::PrintJobState::Completed,
            crate::domain::PrintJobState::Held => proto::PrintJobState::Held,
            crate::domain::PrintJobState::Cancelled => proto::PrintJobState::Cancelled,
            crate::domain::PrintJobState::Failed => proto::PrintJobState::Failed,
            crate::domain::PrintJobState::Ambiguous => proto::PrintJobState::Ambiguous,
        } as i32,
        failure_message: job.failure_message.clone(),
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
    }
}

fn encode_capabilities(capabilities: &PrinterCapabilities) -> proto::PrintPrinterCapabilities {
    proto::PrintPrinterCapabilities {
        media_sizes: capabilities
            .media_sizes
            .iter()
            .map(|media| proto::PrintMediaSize {
                name: media.name.clone(),
                width_microns: media.width_microns,
                height_microns: media.height_microns,
            })
            .collect(),
        color_modes: capabilities
            .color_modes
            .iter()
            .map(|mode| encode_color_mode(*mode) as i32)
            .collect(),
        duplex_modes: capabilities
            .duplex_modes
            .iter()
            .map(|mode| encode_duplex_mode(*mode) as i32)
            .collect(),
        resolutions: capabilities
            .resolutions
            .iter()
            .map(|resolution| proto::PrintResolution {
                horizontal_dpi: resolution.horizontal_dpi,
                vertical_dpi: resolution.vertical_dpi,
            })
            .collect(),
        max_copies: capabilities.max_copies,
        supports_page_ranges: capabilities.supports_page_ranges,
        supports_collation: capabilities.supports_collation,
        accepted_document_types: capabilities.accepted_document_types.clone(),
    }
}

pub fn decode_capabilities(
    capabilities: proto::PrintPrinterCapabilities,
) -> Result<PrinterCapabilities> {
    let capabilities = PrinterCapabilities {
        media_sizes: capabilities
            .media_sizes
            .into_iter()
            .map(|media| MediaSize::new(media.name, media.width_microns, media.height_microns))
            .collect::<Result<Vec<_>>>()?,
        color_modes: capabilities
            .color_modes
            .into_iter()
            .map(decode_color_mode)
            .collect::<Result<Vec<_>>>()?,
        duplex_modes: capabilities
            .duplex_modes
            .into_iter()
            .map(decode_duplex_mode)
            .collect::<Result<Vec<_>>>()?,
        resolutions: capabilities
            .resolutions
            .into_iter()
            .map(|resolution| PrintResolution {
                horizontal_dpi: resolution.horizontal_dpi,
                vertical_dpi: resolution.vertical_dpi,
            })
            .collect(),
        max_copies: capabilities.max_copies,
        supports_page_ranges: capabilities.supports_page_ranges,
        supports_collation: capabilities.supports_collation,
        accepted_document_types: capabilities.accepted_document_types,
    };
    capabilities.validate()?;
    Ok(capabilities)
}

fn encode_printer_state(status: PrinterStatus) -> proto::PrintPrinterState {
    match status {
        PrinterStatus::Ready => proto::PrintPrinterState::Ready,
        PrinterStatus::Busy => proto::PrintPrinterState::Busy,
        PrinterStatus::Offline => proto::PrintPrinterState::Offline,
        PrinterStatus::Error => proto::PrintPrinterState::Error,
        PrinterStatus::Unknown => proto::PrintPrinterState::Unspecified,
    }
}

fn encode_color_mode(mode: ColorMode) -> proto::PrintColorMode {
    match mode {
        ColorMode::Monochrome => proto::PrintColorMode::Monochrome,
        ColorMode::Color => proto::PrintColorMode::Color,
    }
}

fn decode_color_mode(value: i32) -> Result<ColorMode> {
    match proto::PrintColorMode::try_from(value).ok() {
        Some(proto::PrintColorMode::Monochrome) => Ok(ColorMode::Monochrome),
        Some(proto::PrintColorMode::Color) => Ok(ColorMode::Color),
        _ => Err(PrintError::Invalid("unsupported print color mode".into())),
    }
}

fn encode_duplex_mode(mode: DuplexMode) -> proto::PrintDuplexMode {
    match mode {
        DuplexMode::OneSided => proto::PrintDuplexMode::OneSided,
        DuplexMode::TwoSidedLongEdge => proto::PrintDuplexMode::TwoSidedLongEdge,
        DuplexMode::TwoSidedShortEdge => proto::PrintDuplexMode::TwoSidedShortEdge,
    }
}

fn decode_duplex_mode(value: i32) -> Result<DuplexMode> {
    match proto::PrintDuplexMode::try_from(value).ok() {
        Some(proto::PrintDuplexMode::OneSided) => Ok(DuplexMode::OneSided),
        Some(proto::PrintDuplexMode::TwoSidedLongEdge) => Ok(DuplexMode::TwoSidedLongEdge),
        Some(proto::PrintDuplexMode::TwoSidedShortEdge) => Ok(DuplexMode::TwoSidedShortEdge),
        _ => Err(PrintError::Invalid("unsupported print duplex mode".into())),
    }
}

fn decode_orientation(value: i32) -> Result<Orientation> {
    match proto::PrintOrientation::try_from(value).ok() {
        Some(proto::PrintOrientation::Portrait) => Ok(Orientation::Portrait),
        Some(proto::PrintOrientation::Landscape) => Ok(Orientation::Landscape),
        _ => Err(PrintError::Invalid("unsupported print orientation".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_job_rejects_unknown_enums_and_bad_digest() {
        let request = proto::PrintCreateJobRequest {
            client_job_id: "client".into(),
            printer_share_id: "share".into(),
            document: Some(proto::PrintDocumentMetadata {
                name: "test.pdf".into(),
                media_type: "application/pdf".into(),
                size_bytes: 10,
                sha256: vec![1; 31],
            }),
            options: Some(proto::PrintJobOptions {
                copies: 1,
                color_mode: proto::PrintColorMode::Monochrome as i32,
                duplex_mode: proto::PrintDuplexMode::OneSided as i32,
                orientation: proto::PrintOrientation::Portrait as i32,
                media_name: "iso_a4_210x297mm".into(),
                page_ranges: vec![],
                collate: false,
            }),
        };
        assert!(decode_create_job(request).is_err());
    }
}
