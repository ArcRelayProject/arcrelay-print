//! Adapters for the operating system print subsystem.
//!
//! These adapters deliberately call the platform's supported user-space print
//! tools. ArcRelay therefore reuses installed printer drivers and never ships
//! a kernel printer driver of its own.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use tokio::process::Command;

use crate::application::{
    LocalPrinterProvider, NativeJobStatus, NativePrintSpooler, SystemQueueRegistrar,
    SystemQueueSpec,
};
#[cfg(any(target_os = "macos", target_os = "linux"))]
use crate::domain::{
    ColorMode, DuplexMode, MediaSize, PrintResolution, PrinterCapabilities, PrinterStatus,
};
use crate::domain::{LocalPrinter, LocalPrinterId, NativeJobId, SystemQueueId};
use crate::{PrintError, Result};

const LOCAL_PRINTER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(target_os = "windows")]
mod windows_backend;

#[cfg(any(target_os = "windows", test))]
const MAX_RENDER_BUFFER_BYTES: u64 = 64 * 1024 * 1024;

#[cfg(any(target_os = "windows", test))]
fn bounded_render_size(width: u32, height: u32) -> (u32, u32) {
    let width = width.max(1);
    let height = height.max(1);
    let pixels = MAX_RENDER_BUFFER_BYTES / 4;
    let scale = (pixels as f64 / (f64::from(width) * f64::from(height)))
        .sqrt()
        .min(1.0);
    let mut w = (f64::from(width) * scale).floor().max(1.0) as u32;
    let mut h = (f64::from(height) * scale).floor().max(1.0) as u32;
    if u64::from(w) * u64::from(h) > pixels {
        if w > h {
            w = (pixels / u64::from(h)) as u32;
        } else {
            h = (pixels / u64::from(w)) as u32;
        }
    }
    (w, h)
}

#[cfg(any(target_os = "windows", test))]
fn within_render_memory_budget(width: u32, height: u32) -> bool {
    u64::from(width)
        .checked_mul(u64::from(height))
        .and_then(|pixels| pixels.checked_mul(4))
        .is_some_and(|bytes| bytes <= MAX_RENDER_BUFFER_BYTES)
}

#[cfg(any(target_os = "windows", test))]
fn composite_rgba_to_bgra(pixels: &mut [u8]) {
    debug_assert_eq!(pixels.len() % 4, 0);
    for pixel in pixels.as_chunks_mut::<4>().0 {
        let [red, green, blue, alpha] = [pixel[0], pixel[1], pixel[2], pixel[3]];
        let alpha = u16::from(alpha);
        let composite =
            |channel: u8| ((u16::from(channel) * alpha + 255 * (255 - alpha)) / 255) as u8;
        pixel.copy_from_slice(&[composite(blue), composite(green), composite(red), 255]);
    }
}

#[derive(Debug, Default)]
pub struct SystemPrintBackend {
    resources: std::sync::Arc<arcrelay_content::ContentResources>,
}

impl SystemPrintBackend {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_resources(resources: std::sync::Arc<arcrelay_content::ContentResources>) -> Self {
        Self { resources }
    }

    /// Hosting requires a native PDF submission adapter. Windows can still
    /// discover and install remote IPP queues, but must not advertise local
    /// printers until that adapter is implemented.
    #[must_use]
    pub const fn hosting_supported() -> bool {
        cfg!(any(
            target_os = "macos",
            target_os = "linux",
            target_os = "windows"
        ))
    }
}

#[async_trait]
impl LocalPrinterProvider for SystemPrintBackend {
    async fn list(&self) -> Result<Vec<LocalPrinter>> {
        tokio::time::timeout(LOCAL_PRINTER_DISCOVERY_TIMEOUT, list_local_printers())
            .await
            .map_err(|_| PrintError::Backend("system printer discovery timed out".into()))?
    }

    async fn find(&self, id: &LocalPrinterId) -> Result<Option<LocalPrinter>> {
        tokio::time::timeout(LOCAL_PRINTER_DISCOVERY_TIMEOUT, find_local_printer(id))
            .await
            .map_err(|_| PrintError::Backend("system printer lookup timed out".into()))?
    }
}

#[cfg(not(target_os = "windows"))]
async fn find_local_printer(id: &LocalPrinterId) -> Result<Option<LocalPrinter>> {
    Ok(list_local_printers()
        .await?
        .into_iter()
        .find(|printer| &printer.id == id))
}

#[cfg(target_os = "windows")]
async fn find_local_printer(id: &LocalPrinterId) -> Result<Option<LocalPrinter>> {
    windows_backend::find_local_printer(id).await
}

#[async_trait]
impl SystemQueueRegistrar for SystemPrintBackend {
    async fn install(&self, spec: &SystemQueueSpec) -> Result<SystemQueueId> {
        validate_queue_name(&spec.queue_name)?;
        let system_id = SystemQueueId::parse(spec.queue_name.clone())?;
        if self.exists(&system_id).await? {
            return Ok(system_id);
        }
        install_queue(spec).await?;
        Ok(system_id)
    }

    async fn remove(&self, id: &SystemQueueId) -> Result<()> {
        remove_queue(id).await
    }

    async fn exists(&self, id: &SystemQueueId) -> Result<bool> {
        Ok(self
            .list()
            .await?
            .iter()
            .any(|printer| printer.id.as_str() == id.as_str()))
    }
}

#[async_trait]
impl NativePrintSpooler for SystemPrintBackend {
    async fn submit(
        &self,
        printer: &LocalPrinterId,
        document: &Path,
        job: &crate::domain::PrintJob,
    ) -> Result<NativeJobId> {
        let permit = self
            .resources
            .work(128 * 1024 * 1024)
            .await
            .map_err(|error| PrintError::Backend(error.to_string()))?;
        #[cfg(target_os = "windows")]
        {
            windows_backend::submit_native(printer, document, job, permit).await
        }
        #[cfg(not(target_os = "windows"))]
        {
            let _permit = permit;
            submit_native(printer, document, job).await
        }
    }

    async fn status(&self, id: &NativeJobId) -> Result<NativeJobStatus> {
        native_status(id).await
    }

    async fn cancel(&self, id: &NativeJobId) -> Result<()> {
        cancel_native(id).await
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn baseline_capabilities() -> PrinterCapabilities {
    PrinterCapabilities {
        media_sizes: vec![
            MediaSize::new("iso_a4_210x297mm", 210_000, 297_000).expect("valid A4 media"),
            MediaSize::new("na_letter_8.5x11in", 215_900, 279_400).expect("valid Letter media"),
        ],
        // Until the platform-specific capability probe has confirmed an
        // option, advertise the conservative subset every system queue has.
        color_modes: vec![ColorMode::Monochrome],
        duplex_modes: vec![DuplexMode::OneSided],
        resolutions: vec![PrintResolution {
            horizontal_dpi: 300,
            vertical_dpi: 300,
        }],
        max_copies: 100,
        supports_page_ranges: true,
        supports_collation: true,
        accepted_document_types: vec!["application/pdf".into(), "image/pwg-raster".into()],
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn list_local_printers() -> Result<Vec<LocalPrinter>> {
    // `lpstat -p/-d` prose is localized on macOS even with LC_ALL=C. The
    // destination-only form and the first token of `-a` remain machine-safe.
    let destinations = command_output("lpstat", &["-e"]).await?;
    let accepting = command_output("lpstat", &["-a"])
        .await
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.split_whitespace().next().map(str::to_owned))
        .collect::<std::collections::HashSet<_>>();
    let default_output = command_output("lpstat", &["-d"]).await.unwrap_or_default();
    let default = default_output
        .split_once(':')
        .map(|(_, destination)| destination.trim());
    destinations
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| {
            Ok(LocalPrinter {
                id: LocalPrinterId::parse(name)?,
                display_name: name.replace('_', " "),
                status: if accepting.contains(name) {
                    PrinterStatus::Ready
                } else {
                    PrinterStatus::Offline
                },
                connection_kind: "system".into(),
                is_default: default == Some(name),
                capabilities: baseline_capabilities(),
            })
        })
        .collect()
}

#[cfg(target_os = "windows")]
async fn list_local_printers() -> Result<Vec<LocalPrinter>> {
    windows_backend::list_local_printers().await
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
async fn list_local_printers() -> Result<Vec<LocalPrinter>> {
    Err(PrintError::Backend("unsupported operating system".into()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn install_queue(spec: &SystemQueueSpec) -> Result<()> {
    command_output(
        "lpadmin",
        &[
            "-p",
            &spec.queue_name,
            "-E",
            "-v",
            &spec.ipp_uri,
            "-m",
            "everywhere",
        ],
    )
    .await?;
    Ok(())
}

#[cfg(target_os = "windows")]
async fn install_queue(spec: &SystemQueueSpec) -> Result<()> {
    let ipp_uri = spec
        .ipp_uri
        .strip_prefix("ipp://")
        .map(|rest| format!("http://{rest}"))
        .unwrap_or_else(|| spec.ipp_uri.clone());
    elevated_powershell(
        "if (-not (Get-Printer -Name $env:ARCRELAY_QUEUE_NAME -ErrorAction SilentlyContinue)) { Add-Printer -Name $env:ARCRELAY_QUEUE_NAME -IppURL $env:ARCRELAY_IPP_URI }",
        &[
            ("ARCRELAY_QUEUE_NAME", spec.queue_name.as_str()),
            ("ARCRELAY_IPP_URI", ipp_uri.as_str()),
        ],
    )
    .await
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
async fn install_queue(_spec: &SystemQueueSpec) -> Result<()> {
    Err(PrintError::Backend("unsupported operating system".into()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn remove_queue(id: &SystemQueueId) -> Result<()> {
    command_output("lpadmin", &["-x", id.as_str()])
        .await
        .map(|_| ())
}

#[cfg(target_os = "windows")]
async fn remove_queue(id: &SystemQueueId) -> Result<()> {
    elevated_powershell(
        "Remove-Printer -Name $env:ARCRELAY_QUEUE_NAME",
        &[("ARCRELAY_QUEUE_NAME", id.as_str())],
    )
    .await
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
async fn remove_queue(_id: &SystemQueueId) -> Result<()> {
    Err(PrintError::Backend("unsupported operating system".into()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn submit_native(
    printer: &LocalPrinterId,
    document: &Path,
    job: &crate::domain::PrintJob,
) -> Result<NativeJobId> {
    let path = document
        .to_str()
        .ok_or_else(|| PrintError::Invalid("document path is not valid UTF-8".into()))?;
    let mut command = Command::new("lp");
    command
        .arg("-d")
        .arg(printer.as_str())
        .arg("-n")
        .arg(job.options.copies.to_string())
        .arg("-o")
        .arg(format!(
            "print-color-mode={}",
            match job.options.color_mode {
                ColorMode::Monochrome => "monochrome",
                ColorMode::Color => "color",
            }
        ))
        .arg("-o")
        .arg(format!(
            "sides={}",
            match job.options.duplex_mode {
                DuplexMode::OneSided => "one-sided",
                DuplexMode::TwoSidedLongEdge => "two-sided-long-edge",
                DuplexMode::TwoSidedShortEdge => "two-sided-short-edge",
            }
        ))
        .arg("-o")
        .arg(format!(
            "orientation-requested={}",
            match job.options.orientation {
                crate::domain::Orientation::Portrait => 3,
                crate::domain::Orientation::Landscape => 4,
            }
        ))
        .arg("-o")
        .arg(format!("media={}", job.options.media_name));
    if !job.options.page_ranges.is_empty() {
        command.arg("-o").arg(format!(
            "page-ranges={}",
            job.options
                .page_ranges
                .iter()
                .map(|range| format!("{}-{}", range.first, range.last))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if job.options.collate {
        command.arg("-o").arg("Collate=True");
    }
    command.env("LC_ALL", "C");
    command.arg(path);
    let output = checked_command(command).await?;
    let id = native_job_id_from_output(printer.as_str(), &output)
        .ok_or_else(|| PrintError::Backend(format!("could not parse native job id: {output}")))?;
    NativeJobId::parse(id)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn native_job_id_from_output(printer: &str, output: &str) -> Option<String> {
    let prefix = format!("{printer}-");
    let start = output.find(&prefix)?;
    let suffix = &output[start + prefix.len()..];
    let digit_count = suffix.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return None;
    }
    Some(format!("{prefix}{}", &suffix[..digit_count]))
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
async fn submit_native(
    _printer: &LocalPrinterId,
    _document: &Path,
    _job: &crate::domain::PrintJob,
) -> Result<NativeJobId> {
    Err(PrintError::Backend("unsupported operating system".into()))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn native_status(id: &NativeJobId) -> Result<NativeJobStatus> {
    let Some(destination) = native_job_destination(id.as_str()) else {
        return Ok(NativeJobStatus::Unknown);
    };
    let output = command_output("lpstat", &["-W", "not-completed", "-o", destination]).await;
    match output {
        Ok(value) if value.lines().any(|line| line.starts_with(id.as_str())) => {
            Ok(NativeJobStatus::Queued)
        }
        Ok(_) => Ok(NativeJobStatus::Completed),
        Err(_) => Ok(NativeJobStatus::Unknown),
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn native_job_destination(id: &str) -> Option<&str> {
    let (destination, job_number) = id.rsplit_once('-')?;
    (!destination.is_empty()
        && !job_number.is_empty()
        && job_number.bytes().all(|byte| byte.is_ascii_digit()))
    .then_some(destination)
}

#[cfg(target_os = "windows")]
async fn native_status(id: &NativeJobId) -> Result<NativeJobStatus> {
    windows_backend::native_status(id).await
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
async fn native_status(_id: &NativeJobId) -> Result<NativeJobStatus> {
    Ok(NativeJobStatus::Unknown)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn cancel_native(id: &NativeJobId) -> Result<()> {
    command_output("cancel", &[id.as_str()]).await.map(|_| ())
}

#[cfg(target_os = "windows")]
async fn cancel_native(id: &NativeJobId) -> Result<()> {
    windows_backend::cancel_native(id).await
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
async fn cancel_native(_id: &NativeJobId) -> Result<()> {
    Err(PrintError::Backend("unsupported operating system".into()))
}

fn validate_queue_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 127
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(PrintError::Invalid(
            "queue name may contain only letters, digits, '-' and '_'".into(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "windows")]
async fn elevated_powershell(script: &str, environment: &[(&str, &str)]) -> Result<()> {
    const LAUNCH_ELEVATED: &str = r#"
$ErrorActionPreference = 'Stop'
$resultPath = $env:ARCRELAY_ELEVATION_RESULT
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($env:ARCRELAY_ELEVATED_SCRIPT))
try {
    $process = Start-Process powershell.exe -Verb RunAs -WindowStyle Hidden -Wait -PassThru -ArgumentList @('-NoProfile', '-NonInteractive', '-EncodedCommand', $encoded)
} catch {
    [Console]::Error.Write($_.Exception.Message)
    exit 1
}
if ($process.ExitCode -ne 0) {
    if (Test-Path -LiteralPath $resultPath) {
        [Console]::Error.Write([IO.File]::ReadAllText($resultPath))
        Remove-Item -LiteralPath $resultPath -Force -ErrorAction SilentlyContinue
    } else {
        [Console]::Error.Write("elevated printer command failed with exit code " + $process.ExitCode)
    }
    exit $process.ExitCode
}
Remove-Item -LiteralPath $resultPath -Force -ErrorAction SilentlyContinue
"#;

    let result_path = std::env::temp_dir().join(format!(
        "ArcRelay-printer-{}.txt",
        uuid::Uuid::new_v4().simple()
    ));
    let result_path = result_path
        .to_str()
        .ok_or_else(|| PrintError::Backend("Windows temporary path is not valid UTF-8".into()))?;
    let elevated_script = build_elevated_powershell_script(script, environment, result_path);
    let mut command = Command::new("powershell.exe");
    command
        .args(["-NoProfile", "-NonInteractive", "-Command", LAUNCH_ELEVATED])
        .env("ARCRELAY_ELEVATED_SCRIPT", elevated_script)
        .env("ARCRELAY_ELEVATION_RESULT", result_path);
    checked_command(command).await.map(|_| ())
}

#[cfg(any(target_os = "windows", test))]
fn build_elevated_powershell_script(
    operation: &str,
    environment: &[(&str, &str)],
    result_path: &str,
) -> String {
    let mut script = String::from("$ErrorActionPreference = 'Stop'\n");
    for &(name, value) in environment {
        script.push_str(&format!(
            "$env:{name} = '{}'\n",
            powershell_single_quoted(value)
        ));
    }
    script.push_str("try {\n");
    script.push_str(operation);
    script.push_str("\n} catch {\n");
    script.push_str(&format!(
        "[IO.File]::WriteAllText('{}', $_.Exception.Message)\n",
        powershell_single_quoted(result_path)
    ));
    script.push_str("exit 1\n}\n");
    script
}

#[cfg(any(target_os = "windows", test))]
fn powershell_single_quoted(value: &str) -> String {
    value.replace('\'', "''")
}

async fn command_output(program: &str, args: &[&str]) -> Result<String> {
    let mut command = Command::new(program);
    command.args(args);
    #[cfg(unix)]
    command.env("LC_ALL", "C");
    checked_command(command).await
}

async fn checked_command(mut command: Command) -> Result<String> {
    // Timed-out discovery futures must not leave native print helpers behind.
    command.kill_on_drop(true);
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        // CREATE_NO_WINDOW: printer discovery and job polling invoke
        // PowerShell repeatedly and must remain invisible in the desktop app.
        command.creation_flags(0x0800_0000);
    }
    let output = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        return Err(PrintError::Backend(if error.is_empty() {
            format!("print command exited with {}", output.status)
        } else {
            error
        }));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        build_elevated_powershell_script, composite_rgba_to_bgra, validate_queue_name,
        within_render_memory_budget,
    };

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    use super::{native_job_destination, native_job_id_from_output};

    #[test]
    fn queue_names_are_safe_for_all_platform_adapters() {
        assert!(validate_queue_name("ArcRelay_Office-Printer").is_ok());
        assert!(validate_queue_name("office printer").is_err());
        assert!(validate_queue_name("printer;rm").is_err());
    }

    #[test]
    fn elevated_script_embeds_values_without_relying_on_child_environment() {
        let script = build_elevated_powershell_script(
            "Add-Printer -Name $env:QUEUE -IppURL $env:URI",
            &[("QUEUE", "Office'Printer"), ("URI", "http://127.0.0.1")],
            "C:\\Temp\\result.txt",
        );

        assert!(script.contains("$env:QUEUE = 'Office''Printer'"));
        assert!(script.contains("$env:URI = 'http://127.0.0.1'"));
        assert!(script.contains("[IO.File]::WriteAllText('C:\\Temp\\result.txt'"));
    }

    #[test]
    fn render_budget_caps_each_page_at_128_mib() {
        assert!(within_render_memory_budget(4_096, 4_096));
        assert!(!within_render_memory_budget(4_097, 4_096));
        for (width, height) in [(8_192, 8_192), (1, u32::MAX), (u32::MAX, 1), (2_480, 3_508)] {
            let (w, h) = super::bounded_render_size(width, height);
            assert!(within_render_memory_budget(w, h));
            assert!(w > 0 && h > 0 && w <= width && h <= height);
        }
    }

    #[test]
    fn rgba_pixels_are_composited_to_bgra_in_place() {
        let mut pixels = [10, 20, 30, 0, 10, 20, 30, 255];

        composite_rgba_to_bgra(&mut pixels);

        assert_eq!(pixels, [255, 255, 255, 255, 30, 20, 10, 255]);
    }

    #[test]
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    fn parses_native_job_id_from_localized_lp_output() {
        assert_eq!(
            native_job_id_from_output("HP_home", "request id is HP_home-35 (1 file(s))"),
            Some("HP_home-35".into())
        );
        assert_eq!(
            native_job_id_from_output("HP_home", "请求id是HP_home-35（1个文件）"),
            Some("HP_home-35".into())
        );
        assert_eq!(
            native_job_id_from_output("Office-Printer", "请求id是Office-Printer-127。"),
            Some("Office-Printer-127".into())
        );
        assert_eq!(native_job_id_from_output("HP_home", "打印请求失败"), None);
    }

    #[test]
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    fn extracts_destination_from_native_job_id() {
        assert_eq!(native_job_destination("HP_home-35"), Some("HP_home"));
        assert_eq!(
            native_job_destination("Office-Printer-127"),
            Some("Office-Printer")
        );
        assert_eq!(native_job_destination("HP_home"), None);
        assert_eq!(native_job_destination("HP_home-invalid"), None);
    }
}
