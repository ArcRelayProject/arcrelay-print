use std::ffi::c_void;
use std::mem::size_of;
use std::path::{Path, PathBuf};

use image::GenericImageView;
use windows::core::{HSTRING, PCWSTR, PWSTR};
use windows::Data::Pdf::{PdfDocument, PdfPageRenderOptions};
use windows::Storage::StorageFile;
use windows::Storage::Streams::{DataReader, InMemoryRandomAccessStream};
use windows::Win32::Foundation::POINT;
use windows::Win32::Graphics::Gdi::{
    CreateDCW, DeleteDC, GetDeviceCaps, SetStretchBltMode, StretchDIBits, BITMAPINFO,
    BITMAPINFOHEADER, BI_RGB, DEVMODEW, DIB_RGB_COLORS, DMCOLLATE_FALSE, DMCOLOR_COLOR,
    DMCOLOR_MONOCHROME, DMDUP_HORIZONTAL, DMDUP_SIMPLEX, DMDUP_VERTICAL, DMORIENT_LANDSCAPE,
    DMORIENT_PORTRAIT, DMPAPER_A4, DMPAPER_LETTER, DM_COLLATE, DM_COLOR, DM_DUPLEX, DM_ORIENTATION,
    DM_OUT_BUFFER, DM_PAPERSIZE, HALFTONE, HORZRES, SRCCOPY, VERTRES,
};
use windows::Win32::Graphics::Printing::{
    ClosePrinter, DocumentPropertiesW, GetJobW, OpenPrinterW, SetJobW, JOB_CONTROL_CANCEL,
    JOB_INFO_1W, JOB_STATUS_BLOCKED_DEVQ, JOB_STATUS_COMPLETE, JOB_STATUS_DELETED,
    JOB_STATUS_DELETING, JOB_STATUS_ERROR, JOB_STATUS_OFFLINE, JOB_STATUS_PAPEROUT,
    JOB_STATUS_PAUSED, JOB_STATUS_PRINTED, JOB_STATUS_PRINTING, JOB_STATUS_RESTART,
    JOB_STATUS_SPOOLING, JOB_STATUS_USER_INTERVENTION, PRINTER_HANDLE,
};
use windows::Win32::Storage::Xps::{
    AbortDoc, DeviceCapabilitiesW, EndDoc, EndPage, StartDocW, StartPage, DC_COLORDEVICE,
    DC_DUPLEX, DC_ENUMRESOLUTIONS, DC_PAPERS, DC_PAPERSIZE, DOCINFOW,
};
use windows::Win32::System::WinRT::{RoInitialize, RoUninitialize, RO_INIT_MULTITHREADED};

use super::{
    bounded_render_size, composite_rgba_to_bgra, within_render_memory_budget,
    MAX_RENDER_BUFFER_BYTES,
};
use crate::application::NativeJobStatus;
use crate::domain::{
    ColorMode, DuplexMode, LocalPrinter, LocalPrinterId, MediaSize, NativeJobId, Orientation,
    PrintResolution, PrinterCapabilities, PrinterStatus,
};
use crate::{PrintError, Result};

const MAX_PDF_PAGES: u32 = 2_000;
const CAPABILITY_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

pub async fn list_local_printers() -> Result<Vec<LocalPrinter>> {
    let script = "$default = Get-CimInstance Win32_Printer | Where-Object Default | Select-Object -First 1 -ExpandProperty Name; Get-Printer | Select-Object Name,PrinterStatus,DriverName,PortName,@{Name='Default';Expression={$_.Name -eq $default}} | ConvertTo-Json -Compress";
    let output = super::command_output(
        "powershell.exe",
        &["-NoProfile", "-NonInteractive", "-Command", script],
    )
    .await?;
    if output.trim().is_empty() {
        return Ok(Vec::new());
    }
    let value: serde_json::Value = serde_json::from_str(&output)?;
    let rows = value.as_array().cloned().unwrap_or_else(|| vec![value]);
    rows.into_iter()
        .filter_map(|row| {
            let name = row.get("Name")?.as_str()?.to_owned();
            let driver = row
                .get("DriverName")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_owned();
            Some((name, driver, row))
        })
        .filter(|(name, driver, _)| !interactive_virtual_printer(name, driver))
        .map(|(name, _, row)| {
            Ok(LocalPrinter {
                id: LocalPrinterId::parse(name.clone())?,
                display_name: name,
                status: windows_printer_status(
                    row.get("PrinterStatus")
                        .and_then(|value| value.as_u64())
                        .unwrap_or(u64::MAX),
                ),
                connection_kind: "windows-spooler".into(),
                is_default: row
                    .get("Default")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false),
                // DeviceCapabilitiesW can synchronously contact an offline
                // WSD queue. Keep inventory fast and probe only the printer
                // the user actually chooses to share.
                capabilities: conservative_capabilities(),
            })
        })
        .collect()
}

pub async fn find_local_printer(id: &LocalPrinterId) -> Result<Option<LocalPrinter>> {
    let Some(mut printer) = list_local_printers()
        .await?
        .into_iter()
        .find(|printer| &printer.id == id)
    else {
        return Ok(None);
    };
    let printer_name = printer.display_name.clone();
    let probe = tokio::task::spawn_blocking(move || probe_capabilities(&printer_name));
    match tokio::time::timeout(CAPABILITY_PROBE_TIMEOUT, probe).await {
        Ok(Ok(Ok(capabilities))) => printer.capabilities = capabilities,
        Ok(Ok(Err(error))) => tracing::warn!(
            printer = %printer.display_name,
            %error,
            "could not probe Windows printer capabilities"
        ),
        Ok(Err(error)) => tracing::warn!(
            printer = %printer.display_name,
            %error,
            "Windows printer capability worker failed"
        ),
        Err(_) => tracing::warn!(
            printer = %printer.display_name,
            "Windows printer capability probe timed out; using conservative capabilities"
        ),
    }
    Ok(Some(printer))
}

pub async fn submit_native(
    printer: &LocalPrinterId,
    document: &Path,
    job: &crate::domain::PrintJob,
    permit: arcrelay_content::ContentWorkPermit,
) -> Result<NativeJobId> {
    let printer = printer.as_str().to_owned();
    let document = document.to_owned();
    let job = job.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        print_pdf(&printer, &document, &job)
    })
    .await
    .map_err(|error| PrintError::Backend(format!("Windows print worker failed: {error}")))?
}

pub async fn native_status(id: &NativeJobId) -> Result<NativeJobStatus> {
    let native = decode_native_job_id(id)?;
    tokio::task::spawn_blocking(move || query_job_status(&native.0, native.1))
        .await
        .map_err(|error| PrintError::Backend(format!("Windows status worker failed: {error}")))?
}

pub async fn cancel_native(id: &NativeJobId) -> Result<()> {
    let native = decode_native_job_id(id)?;
    tokio::task::spawn_blocking(move || cancel_job(&native.0, native.1))
        .await
        .map_err(|error| PrintError::Backend(format!("Windows cancel worker failed: {error}")))?
}

fn print_pdf(
    printer_name: &str,
    document_path: &Path,
    job: &crate::domain::PrintJob,
) -> Result<NativeJobId> {
    let _apartment = WinRtApartment::initialize()?;
    let printer_wide = wide(printer_name);
    let printer = PrinterHandle::open(&printer_wide)?;
    let mut devmode = load_devmode(printer.handle, &printer_wide)?;
    configure_devmode(devmode.as_mut(), printer_name, &job.options)?;
    let hdc = unsafe {
        CreateDCW(
            PCWSTR::from_raw(wide("WINSPOOL").as_ptr()),
            PCWSTR::from_raw(printer_wide.as_ptr()),
            PCWSTR::null(),
            Some(devmode.as_ptr()),
        )
    };
    if hdc.is_invalid() {
        return Err(last_win_error("create printer device context"));
    }
    let dc = DeviceContext { hdc };
    let printable_width = unsafe { GetDeviceCaps(Some(dc.hdc), HORZRES) };
    let printable_height = unsafe { GetDeviceCaps(Some(dc.hdc), VERTRES) };
    if printable_width <= 0 || printable_height <= 0 {
        return Err(PrintError::Backend(
            "printer reported an invalid printable area".into(),
        ));
    }
    if !within_render_memory_budget(printable_width as u32, printable_height as u32) {
        return Err(PrintError::Backend(format!(
            "printer render area is too large: {printable_width}x{printable_height}"
        )));
    }

    let file_path = if document_path.is_absolute() {
        PathBuf::from(document_path)
    } else {
        std::env::current_dir()?.join(document_path)
    };
    let winrt_path = winrt_file_path(&file_path);
    let file = StorageFile::GetFileFromPathAsync(&HSTRING::from(winrt_path))
        .map_err(win_error)?
        .get()
        .map_err(win_error)?;
    let pdf = PdfDocument::LoadFromFileAsync(&file)
        .map_err(win_error)?
        .get()
        .map_err(win_error)?;
    if pdf.IsPasswordProtected().map_err(win_error)? {
        return Err(PrintError::Invalid(
            "password-protected PDF documents are not supported".into(),
        ));
    }
    let page_count = pdf.PageCount().map_err(win_error)?;
    if page_count == 0 || page_count > MAX_PDF_PAGES {
        return Err(PrintError::Invalid(format!(
            "PDF page count {page_count} is outside the supported range"
        )));
    }
    let pages = selected_pages(page_count, &job.options.page_ranges)?;
    let sequence = print_sequence(
        &pages,
        job.options.copies,
        job.options.collate || job.options.duplex_mode != DuplexMode::OneSided,
    );

    let document_name = wide(&job.document.name);
    let info = DOCINFOW {
        cbSize: size_of::<DOCINFOW>() as i32,
        lpszDocName: PCWSTR::from_raw(document_name.as_ptr()),
        ..Default::default()
    };
    let windows_job_id = unsafe { StartDocW(dc.hdc, &info) };
    if windows_job_id <= 0 {
        return Err(last_win_error("start Windows print job"));
    }
    let mut active_document = ActiveDocument {
        hdc: dc.hdc,
        committed: false,
    };
    for page_index in sequence {
        print_page(&pdf, page_index, dc.hdc, printable_width, printable_height)?;
    }
    if unsafe { EndDoc(dc.hdc) } <= 0 {
        return Err(last_win_error("finish Windows print job"));
    }
    active_document.committed = true;
    encode_native_job_id(printer_name, windows_job_id as u32)
}

fn print_page(
    pdf: &PdfDocument,
    page_index: u32,
    hdc: windows::Win32::Graphics::Gdi::HDC,
    printable_width: i32,
    printable_height: i32,
) -> Result<()> {
    let page = pdf.GetPage(page_index).map_err(win_error)?;
    let size = page.Size().map_err(win_error)?;
    if size.Width <= 0.0 || size.Height <= 0.0 {
        return Err(PrintError::Invalid(format!(
            "PDF page {} has invalid dimensions",
            page_index + 1
        )));
    }
    let scale = (printable_width as f32 / size.Width)
        .min(printable_height as f32 / size.Height)
        .max(0.01);
    let target_width = (size.Width * scale).round().max(1.0) as u32;
    let target_height = (size.Height * scale).round().max(1.0) as u32;
    let (render_width, render_height) = bounded_render_size(target_width, target_height);
    if !within_render_memory_budget(render_width, render_height) {
        return Err(PrintError::Invalid(format!(
            "PDF page {} exceeds the render pixel limit",
            page_index + 1
        )));
    }
    let stream = InMemoryRandomAccessStream::new().map_err(win_error)?;
    let options = PdfPageRenderOptions::new().map_err(win_error)?;
    options
        .SetDestinationWidth(render_width)
        .map_err(win_error)?;
    options
        .SetDestinationHeight(render_height)
        .map_err(win_error)?;
    page.RenderWithOptionsToStreamAsync(&stream, &options)
        .map_err(win_error)?
        .get()
        .map_err(win_error)?;
    let encoded_size = stream.Size().map_err(win_error)?;
    if encoded_size == 0 || encoded_size > 32 * 1024 * 1024 {
        return Err(PrintError::Invalid(
            "rendered PDF page has an invalid size".into(),
        ));
    }
    let input = stream.GetInputStreamAt(0).map_err(win_error)?;
    let reader = DataReader::CreateDataReader(&input).map_err(win_error)?;
    let loaded = reader
        .LoadAsync(encoded_size as u32)
        .map_err(win_error)?
        .get()
        .map_err(win_error)?;
    if loaded != encoded_size as u32 {
        return Err(PrintError::Backend(
            "Windows PDF renderer returned a truncated page".into(),
        ));
    }
    let mut encoded = vec![0_u8; loaded as usize];
    reader.ReadBytes(&mut encoded).map_err(win_error)?;
    drop(reader);
    drop(input);
    drop(stream);
    let mut image_reader = image::ImageReader::new(std::io::Cursor::new(encoded))
        .with_guessed_format()
        .map_err(|error| PrintError::Backend(error.to_string()))?;
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(MAX_RENDER_BUFFER_BYTES);
    limits.max_image_width = Some(render_width);
    limits.max_image_height = Some(render_height);
    image_reader.limits(limits);
    let bitmap = image_reader
        .decode()
        .map_err(|error| PrintError::Backend(format!("decode rendered PDF page: {error}")))?;
    let (width, height) = bitmap.dimensions();
    let mut bgra = bitmap.into_rgba8().into_raw();
    composite_rgba_to_bgra(&mut bgra);
    let bitmap_info = BITMAPINFO {
        bmiHeader: BITMAPINFOHEADER {
            biSize: size_of::<BITMAPINFOHEADER>() as u32,
            biWidth: width as i32,
            biHeight: -(height as i32),
            biPlanes: 1,
            biBitCount: 32,
            biCompression: BI_RGB.0,
            biSizeImage: bgra.len() as u32,
            ..Default::default()
        },
        ..Default::default()
    };
    let dest_x = (printable_width - target_width as i32).max(0) / 2;
    let dest_y = (printable_height - target_height as i32).max(0) / 2;
    if unsafe { StartPage(hdc) } <= 0 {
        return Err(last_win_error("start Windows printer page"));
    }
    let mut page_guard = ActivePage { hdc, ended: false };
    unsafe {
        SetStretchBltMode(hdc, HALFTONE);
    }
    let copied = unsafe {
        StretchDIBits(
            hdc,
            dest_x,
            dest_y,
            target_width as i32,
            target_height as i32,
            0,
            0,
            width as i32,
            height as i32,
            Some(bgra.as_ptr().cast::<c_void>()),
            &bitmap_info,
            DIB_RGB_COLORS,
            SRCCOPY,
        )
    };
    if copied == 0 {
        return Err(last_win_error("render PDF page to printer"));
    }
    if unsafe { EndPage(hdc) } <= 0 {
        return Err(last_win_error("finish Windows printer page"));
    }
    page_guard.ended = true;
    page.Close().map_err(win_error)?;
    Ok(())
}

fn configure_devmode(
    devmode: &mut DEVMODEW,
    printer_name: &str,
    options: &crate::domain::PrintOptions,
) -> Result<()> {
    devmode.dmFields = devmode.dmFields | DM_ORIENTATION | DM_COLOR | DM_DUPLEX | DM_COLLATE;
    devmode.Anonymous1.Anonymous1.dmOrientation = match options.orientation {
        Orientation::Portrait => DMORIENT_PORTRAIT as i16,
        Orientation::Landscape => DMORIENT_LANDSCAPE as i16,
    };
    devmode.dmColor = match options.color_mode {
        ColorMode::Monochrome => DMCOLOR_MONOCHROME,
        ColorMode::Color => DMCOLOR_COLOR,
    };
    devmode.dmDuplex = match options.duplex_mode {
        DuplexMode::OneSided => DMDUP_SIMPLEX,
        DuplexMode::TwoSidedLongEdge => DMDUP_VERTICAL,
        DuplexMode::TwoSidedShortEdge => DMDUP_HORIZONTAL,
    };
    // Copies and collation are rendered explicitly so driver behavior cannot
    // multiply the requested count a second time.
    devmode.Anonymous1.Anonymous1.dmCopies = 1;
    devmode.dmCollate = DMCOLLATE_FALSE;
    if let Some(paper_code) = media_code(printer_name, &options.media_name) {
        devmode.dmFields = devmode.dmFields | DM_PAPERSIZE;
        devmode.Anonymous1.Anonymous1.dmPaperSize = paper_code;
    } else {
        return Err(PrintError::Invalid(format!(
            "printer does not support media {}",
            options.media_name
        )));
    }
    Ok(())
}

fn load_devmode(handle: PRINTER_HANDLE, printer_name: &[u16]) -> Result<AlignedDevMode> {
    let size = unsafe {
        DocumentPropertiesW(
            None,
            handle,
            PCWSTR::from_raw(printer_name.as_ptr()),
            None,
            None,
            0,
        )
    };
    if size <= 0 {
        return Err(last_win_error("read printer configuration size"));
    }
    let mut storage = vec![0_usize; (size as usize).div_ceil(size_of::<usize>())];
    let pointer = storage.as_mut_ptr().cast::<DEVMODEW>();
    let result = unsafe {
        DocumentPropertiesW(
            None,
            handle,
            PCWSTR::from_raw(printer_name.as_ptr()),
            Some(pointer),
            None,
            DM_OUT_BUFFER.0,
        )
    };
    if result < 0 {
        return Err(last_win_error("read printer configuration"));
    }
    Ok(AlignedDevMode { storage })
}

fn query_job_status(printer_name: &str, job_id: u32) -> Result<NativeJobStatus> {
    let printer_wide = wide(printer_name);
    let printer = PrinterHandle::open(&printer_wide)?;
    let mut needed = 0_u32;
    unsafe {
        let _ = GetJobW(printer.handle, job_id, 1, None, &mut needed);
    }
    if needed == 0 {
        // Windows removes successfully completed jobs from the spooler quickly.
        return Ok(NativeJobStatus::Completed);
    }
    let mut buffer = vec![0_u8; needed as usize];
    unsafe { GetJobW(printer.handle, job_id, 1, Some(&mut buffer), &mut needed) }
        .ok()
        .map_err(win_error)?;
    let info = unsafe { &*buffer.as_ptr().cast::<JOB_INFO_1W>() };
    let status = info.Status;
    if status & (JOB_STATUS_DELETED | JOB_STATUS_DELETING) != 0 {
        Ok(NativeJobStatus::Cancelled)
    } else if status & (JOB_STATUS_COMPLETE | JOB_STATUS_PRINTED) != 0 {
        Ok(NativeJobStatus::Completed)
    } else if status & JOB_STATUS_PRINTING != 0 {
        Ok(NativeJobStatus::Printing)
    } else if status & (JOB_STATUS_ERROR | JOB_STATUS_OFFLINE | JOB_STATUS_PAPEROUT) != 0 {
        Ok(NativeJobStatus::Failed)
    } else if status & (JOB_STATUS_PAUSED | JOB_STATUS_BLOCKED_DEVQ | JOB_STATUS_USER_INTERVENTION)
        != 0
    {
        Ok(NativeJobStatus::Held)
    } else if status & (JOB_STATUS_SPOOLING | JOB_STATUS_RESTART) != 0 || status == 0 {
        Ok(NativeJobStatus::Queued)
    } else {
        Ok(NativeJobStatus::Unknown)
    }
}

fn cancel_job(printer_name: &str, job_id: u32) -> Result<()> {
    let printer_wide = wide(printer_name);
    let printer = PrinterHandle::open(&printer_wide)?;
    unsafe { SetJobW(printer.handle, job_id, 0, None, JOB_CONTROL_CANCEL) }
        .ok()
        .map_err(win_error)
}

fn probe_capabilities(printer_name: &str) -> Result<PrinterCapabilities> {
    let printer = wide(printer_name);
    let color = unsafe {
        DeviceCapabilitiesW(
            PCWSTR::from_raw(printer.as_ptr()),
            PCWSTR::null(),
            DC_COLORDEVICE,
            None,
            None,
        )
    } > 0;
    let duplex = unsafe {
        DeviceCapabilitiesW(
            PCWSTR::from_raw(printer.as_ptr()),
            PCWSTR::null(),
            DC_DUPLEX,
            None,
            None,
        )
    } > 0;
    let mut color_modes = vec![ColorMode::Monochrome];
    if color {
        color_modes.push(ColorMode::Color);
    }
    let mut duplex_modes = vec![DuplexMode::OneSided];
    if duplex {
        duplex_modes.extend([DuplexMode::TwoSidedLongEdge, DuplexMode::TwoSidedShortEdge]);
    }
    let media_sizes = enumerate_media(printer_name)
        .into_iter()
        .map(|(_, media)| media)
        .collect::<Vec<_>>();
    let resolutions = enumerate_resolutions(printer_name);
    Ok(PrinterCapabilities {
        media_sizes: if media_sizes.is_empty() {
            conservative_capabilities().media_sizes
        } else {
            media_sizes
        },
        color_modes,
        duplex_modes,
        resolutions: if resolutions.is_empty() {
            vec![PrintResolution {
                horizontal_dpi: 300,
                vertical_dpi: 300,
            }]
        } else {
            resolutions
        },
        max_copies: 100,
        supports_page_ranges: true,
        supports_collation: true,
        accepted_document_types: vec!["application/pdf".into()],
    })
}

fn conservative_capabilities() -> PrinterCapabilities {
    PrinterCapabilities {
        media_sizes: vec![
            MediaSize::new("iso_a4_210x297mm", 210_000, 297_000).expect("valid A4"),
            MediaSize::new("na_letter_8.5x11in", 215_900, 279_400).expect("valid Letter"),
        ],
        color_modes: vec![ColorMode::Monochrome],
        duplex_modes: vec![DuplexMode::OneSided],
        resolutions: vec![PrintResolution {
            horizontal_dpi: 300,
            vertical_dpi: 300,
        }],
        max_copies: 100,
        supports_page_ranges: true,
        supports_collation: true,
        accepted_document_types: vec!["application/pdf".into()],
    }
}

fn enumerate_media(printer_name: &str) -> Vec<(i16, MediaSize)> {
    let printer = wide(printer_name);
    let count = unsafe {
        DeviceCapabilitiesW(
            PCWSTR::from_raw(printer.as_ptr()),
            PCWSTR::null(),
            DC_PAPERS,
            None,
            None,
        )
    };
    if count <= 0 {
        return Vec::new();
    }
    let mut codes = vec![0_i16; count as usize];
    let mut sizes = vec![POINT::default(); count as usize];
    let code_count = unsafe {
        DeviceCapabilitiesW(
            PCWSTR::from_raw(printer.as_ptr()),
            PCWSTR::null(),
            DC_PAPERS,
            Some(PWSTR(codes.as_mut_ptr().cast::<u16>())),
            None,
        )
    };
    let size_count = unsafe {
        DeviceCapabilitiesW(
            PCWSTR::from_raw(printer.as_ptr()),
            PCWSTR::null(),
            DC_PAPERSIZE,
            Some(PWSTR(sizes.as_mut_ptr().cast::<u16>())),
            None,
        )
    };
    let usable = code_count.min(size_count).max(0) as usize;
    codes
        .into_iter()
        .zip(sizes)
        .take(usable)
        .filter_map(|(code, size)| {
            if size.x <= 0 || size.y <= 0 {
                return None;
            }
            let width = size.x as u32 * 100;
            let height = size.y as u32 * 100;
            let name = media_name(width, height);
            MediaSize::new(name, width, height)
                .ok()
                .map(|media| (code, media))
        })
        .fold(Vec::<(i16, MediaSize)>::new(), |mut values, item| {
            if !values.iter().any(|(_, media)| media.name == item.1.name) {
                values.push(item);
            }
            values
        })
}

fn enumerate_resolutions(printer_name: &str) -> Vec<PrintResolution> {
    let printer = wide(printer_name);
    let count = unsafe {
        DeviceCapabilitiesW(
            PCWSTR::from_raw(printer.as_ptr()),
            PCWSTR::null(),
            DC_ENUMRESOLUTIONS,
            None,
            None,
        )
    };
    if count <= 0 {
        return Vec::new();
    }
    let mut values = vec![0_i32; count as usize * 2];
    let actual = unsafe {
        DeviceCapabilitiesW(
            PCWSTR::from_raw(printer.as_ptr()),
            PCWSTR::null(),
            DC_ENUMRESOLUTIONS,
            Some(PWSTR(values.as_mut_ptr().cast::<u16>())),
            None,
        )
    };
    values
        .as_chunks::<2>()
        .0
        .iter()
        .take(actual.max(0) as usize)
        .filter_map(|pair| {
            if pair[0] <= 0 || pair[1] <= 0 {
                None
            } else {
                Some(PrintResolution {
                    horizontal_dpi: pair[0] as u32,
                    vertical_dpi: pair[1] as u32,
                })
            }
        })
        .collect()
}

fn media_code(printer_name: &str, requested: &str) -> Option<i16> {
    enumerate_media(printer_name)
        .into_iter()
        .find_map(|(code, media)| (media.name == requested).then_some(code))
        .or_else(|| match requested {
            "iso_a4_210x297mm" => Some(DMPAPER_A4 as i16),
            "na_letter_8.5x11in" => Some(DMPAPER_LETTER as i16),
            _ => None,
        })
}

fn media_name(width: u32, height: u32) -> String {
    let (short, long) = if width <= height {
        (width, height)
    } else {
        (height, width)
    };
    if short.abs_diff(210_000) <= 1_000 && long.abs_diff(297_000) <= 1_000 {
        "iso_a4_210x297mm".into()
    } else if short.abs_diff(215_900) <= 1_000 && long.abs_diff(279_400) <= 1_000 {
        "na_letter_8.5x11in".into()
    } else {
        format!("custom_{short}x{long}um")
    }
}

fn selected_pages(page_count: u32, ranges: &[crate::domain::PageRange]) -> Result<Vec<u32>> {
    if ranges.is_empty() {
        return Ok((0..page_count).collect());
    }
    let mut pages = Vec::new();
    for range in ranges {
        if range.first == 0 || range.last > page_count || range.first > range.last {
            return Err(PrintError::Invalid(format!(
                "page range {}-{} is outside the PDF",
                range.first, range.last
            )));
        }
        pages.extend((range.first - 1)..range.last);
    }
    pages.sort_unstable();
    pages.dedup();
    Ok(pages)
}

fn print_sequence(pages: &[u32], copies: u32, collate: bool) -> Vec<u32> {
    let mut sequence = Vec::with_capacity(pages.len().saturating_mul(copies as usize));
    if collate {
        for _ in 0..copies {
            sequence.extend_from_slice(pages);
        }
    } else {
        for page in pages {
            sequence.extend(std::iter::repeat_n(*page, copies as usize));
        }
    }
    sequence
}

fn interactive_virtual_printer(name: &str, driver: &str) -> bool {
    let name = name.to_ascii_lowercase();
    let driver = driver.to_ascii_lowercase();
    name == "fax"
        || name.contains("microsoft print to pdf")
        || name.contains("xps document writer")
        || name.contains("onenote")
        || driver.contains("shared fax")
        || driver.contains("microsoft print to pdf")
        || driver.contains("xps document writer")
        || driver.contains("onenote")
}

fn windows_printer_status(status: u64) -> PrinterStatus {
    if status == 0 {
        PrinterStatus::Ready
    } else if status & 128 != 0 {
        PrinterStatus::Offline
    } else if status & (512 | 1024 | 16_384) != 0 {
        PrinterStatus::Busy
    } else if status != u64::MAX {
        PrinterStatus::Error
    } else {
        PrinterStatus::Unknown
    }
}

fn encode_native_job_id(printer_name: &str, job_id: u32) -> Result<NativeJobId> {
    let encoded = printer_name
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    NativeJobId::parse(format!("winspool:v1:{job_id}:{encoded}"))
}

fn decode_native_job_id(id: &NativeJobId) -> Result<(String, u32)> {
    let mut parts = id.as_str().splitn(4, ':');
    if parts.next() != Some("winspool") || parts.next() != Some("v1") {
        return Err(PrintError::Invalid("invalid Windows native job id".into()));
    }
    let job_id = parts
        .next()
        .ok_or_else(|| PrintError::Invalid("Windows job id is missing".into()))?
        .parse::<u32>()
        .map_err(|_| PrintError::Invalid("Windows job id is invalid".into()))?;
    let encoded = parts
        .next()
        .ok_or_else(|| PrintError::Invalid("Windows printer name is missing".into()))?;
    if encoded.len() % 2 != 0 {
        return Err(PrintError::Invalid(
            "Windows printer name encoding is invalid".into(),
        ));
    }
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| PrintError::Invalid("Windows printer name encoding is invalid".into()))?;
    let printer = String::from_utf8(bytes)
        .map_err(|_| PrintError::Invalid("Windows printer name is not UTF-8".into()))?;
    Ok((printer, job_id))
}

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

fn winrt_file_path(path: &Path) -> String {
    let value = path.to_string_lossy();
    if let Some(value) = value.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{value}")
    } else if let Some(value) = value.strip_prefix(r"\\?\") {
        value.to_owned()
    } else {
        value.into_owned()
    }
}

fn win_error(error: windows::core::Error) -> PrintError {
    PrintError::Backend(error.to_string())
}

fn last_win_error(action: &str) -> PrintError {
    PrintError::Backend(format!("{action}: {}", windows::core::Error::from_win32()))
}

struct PrinterHandle {
    handle: PRINTER_HANDLE,
}

struct WinRtApartment;

impl WinRtApartment {
    fn initialize() -> Result<Self> {
        unsafe { RoInitialize(RO_INIT_MULTITHREADED) }.map_err(win_error)?;
        Ok(Self)
    }
}

impl Drop for WinRtApartment {
    fn drop(&mut self) {
        unsafe { RoUninitialize() };
    }
}

impl PrinterHandle {
    fn open(name: &[u16]) -> Result<Self> {
        let mut handle = PRINTER_HANDLE::default();
        unsafe { OpenPrinterW(PCWSTR::from_raw(name.as_ptr()), &mut handle, None) }
            .map_err(win_error)?;
        Ok(Self { handle })
    }
}

impl Drop for PrinterHandle {
    fn drop(&mut self) {
        unsafe {
            let _ = ClosePrinter(self.handle);
        }
    }
}

struct AlignedDevMode {
    storage: Vec<usize>,
}

impl AlignedDevMode {
    fn as_ptr(&self) -> *const DEVMODEW {
        self.storage.as_ptr().cast()
    }

    fn as_mut(&mut self) -> &mut DEVMODEW {
        unsafe { &mut *self.storage.as_mut_ptr().cast::<DEVMODEW>() }
    }
}

struct DeviceContext {
    hdc: windows::Win32::Graphics::Gdi::HDC,
}

impl Drop for DeviceContext {
    fn drop(&mut self) {
        unsafe {
            let _ = DeleteDC(self.hdc);
        }
    }
}

struct ActiveDocument {
    hdc: windows::Win32::Graphics::Gdi::HDC,
    committed: bool,
}

impl Drop for ActiveDocument {
    fn drop(&mut self) {
        if !self.committed {
            unsafe {
                let _ = AbortDoc(self.hdc);
            }
        }
    }
}

struct ActivePage {
    hdc: windows::Win32::Graphics::Gdi::HDC,
    ended: bool,
}

impl Drop for ActivePage {
    fn drop(&mut self) {
        if !self.ended {
            unsafe {
                let _ = EndPage(self.hdc);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn native_job_id_round_trips_unicode_printer_names() {
        let id = encode_native_job_id("办公室 打印机", 42).unwrap();
        assert_eq!(
            decode_native_job_id(&id).unwrap(),
            ("办公室 打印机".into(), 42)
        );
    }

    #[test]
    fn page_sequences_honor_collation() {
        assert_eq!(print_sequence(&[0, 1], 2, true), [0, 1, 0, 1]);
        assert_eq!(print_sequence(&[0, 1], 2, false), [0, 0, 1, 1]);
    }

    #[test]
    fn maps_windows_print_queue_status_flags() {
        assert_eq!(windows_printer_status(0), PrinterStatus::Ready);
        assert_eq!(windows_printer_status(128), PrinterStatus::Offline);
        assert_eq!(windows_printer_status(1024), PrinterStatus::Busy);
        assert_eq!(windows_printer_status(16), PrinterStatus::Error);
    }

    #[test]
    #[ignore = "prints one physical page to ARCRELAY_TEST_PRINTER"]
    fn windows_print_smoke_test() {
        let printer = std::env::var("ARCRELAY_TEST_PRINTER")
            .expect("ARCRELAY_TEST_PRINTER must name a non-interactive test printer");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let printers = runtime.block_on(list_local_printers()).unwrap();
        let discovered = printers
            .iter()
            .find(|candidate| candidate.display_name == printer)
            .expect("test printer must be discoverable and hostable");
        assert_eq!(discovered.status, PrinterStatus::Ready);
        assert!(discovered
            .capabilities
            .media_sizes
            .iter()
            .any(|media| media.name == "iso_a4_210x297mm"));
        let pdf = smoke_test_pdf();
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary
            .path()
            .join("arcrelay-windows-print-smoke.document");
        std::fs::write(&path, &pdf).unwrap();
        let job = crate::domain::PrintJob::offer(
            crate::domain::ClientJobId::parse("windows-smoke-client-job").unwrap(),
            crate::domain::DeviceId::parse("windows-smoke-device").unwrap(),
            crate::domain::PrinterShareId::parse("windows-smoke-share").unwrap(),
            crate::domain::PrintDocument {
                name: "ArcRelay Windows print smoke test.pdf".into(),
                media_type: "application/pdf".into(),
                size_bytes: pdf.len() as u64,
                sha256: Sha256::digest(&pdf).into(),
            },
            crate::domain::PrintOptions {
                copies: 1,
                color_mode: ColorMode::Monochrome,
                duplex_mode: DuplexMode::OneSided,
                orientation: Orientation::Portrait,
                media_name: "iso_a4_210x297mm".into(),
                page_ranges: Vec::new(),
                collate: false,
            },
            1,
        )
        .unwrap();
        let native_id = print_pdf(&printer, &path, &job).unwrap();
        let decoded = decode_native_job_id(&native_id).unwrap();
        assert_eq!(decoded.0, printer);
        assert!(decoded.1 > 0);
        let status = query_job_status(&decoded.0, decoded.1).unwrap();
        assert!(matches!(
            status,
            NativeJobStatus::Queued
                | NativeJobStatus::Printing
                | NativeJobStatus::Completed
                | NativeJobStatus::Held
        ));
    }

    fn smoke_test_pdf() -> Vec<u8> {
        let objects = [
            "<< /Type /Catalog /Pages 2 0 R >>",
            "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
            "<< /Length 65 >>\nstream\nBT /F1 24 Tf 72 720 Td (ArcRelay Windows print smoke test) Tj ET\nendstream",
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        ];
        let mut pdf = b"%PDF-1.4\n".to_vec();
        let mut offsets = Vec::new();
        for (index, object) in objects.iter().enumerate() {
            offsets.push(pdf.len());
            pdf.extend_from_slice(format!("{} 0 obj\n{object}\nendobj\n", index + 1).as_bytes());
        }
        let xref = pdf.len();
        pdf.extend_from_slice(format!("xref\n0 {}\n", objects.len() + 1).as_bytes());
        pdf.extend_from_slice(b"0000000000 65535 f \n");
        for offset in offsets {
            pdf.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
        }
        pdf.extend_from_slice(
            format!(
                "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n",
                objects.len() + 1
            )
            .as_bytes(),
        );
        pdf
    }
}
