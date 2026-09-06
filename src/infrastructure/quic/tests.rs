use super::*;
use crate::domain::{
    ColorMode, DuplexMode, MediaSize, PrintResolution, PrinterCapabilities, PrinterShareId,
};

fn remote_printer(device_id: &str, share_id: &str, status: PrinterStatus) -> RemotePrinter {
    RemotePrinter {
        printer: PublishedPrinter {
            source_device_id: DeviceId::parse(device_id).expect("valid device id"),
            source_device_name: format!("Computer {device_id}"),
            share_id: PrinterShareId::parse(share_id).expect("valid share id"),
            display_name: format!("Printer {share_id}"),
            status,
            capabilities: PrinterCapabilities {
                media_sizes: vec![
                    MediaSize::new("iso_a4_210x297mm", 210_000, 297_000).expect("valid media")
                ],
                color_modes: vec![ColorMode::Monochrome],
                duplex_modes: vec![DuplexMode::OneSided],
                resolutions: vec![PrintResolution {
                    horizontal_dpi: 300,
                    vertical_dpi: 300,
                }],
                max_copies: 1,
                supports_page_ranges: false,
                supports_collation: false,
                accepted_document_types: vec!["application/pdf".into()],
            },
            capability_revision: 1,
        },
        address: "127.0.0.1".parse().expect("valid address"),
        port: 8766,
        certificate_sha256: "certificate".into(),
    }
}

#[test]
fn refresh_retains_cached_printer_offline_until_device_query_succeeds() {
    let cached = remote_printer("device-a", "share-a", PrinterStatus::Ready);
    let mut printers = cached_printers_for_refresh(vec![cached]);

    assert_eq!(printers.len(), 1);
    assert_eq!(printers[0].printer.status, PrinterStatus::Offline);

    replace_device_printers(&mut printers, "device-a", Vec::new());
    assert!(printers.is_empty());
}

#[test]
fn successful_device_query_replaces_only_that_devices_cached_printers() {
    let mut printers = cached_printers_for_refresh(vec![
        remote_printer("device-a", "old-share", PrinterStatus::Ready),
        remote_printer("device-b", "other-share", PrinterStatus::Busy),
    ]);
    let replacement = remote_printer("device-a", "new-share", PrinterStatus::Ready);

    replace_device_printers(&mut printers, "device-a", vec![replacement]);

    assert_eq!(printers.len(), 2);
    assert!(printers
        .iter()
        .any(|printer| printer.printer.share_id.as_str() == "new-share"));
    assert!(printers.iter().any(|printer| {
        printer.printer.share_id.as_str() == "other-share"
            && printer.printer.status == PrinterStatus::Offline
    }));
}

#[tokio::test]
async fn control_requests_to_the_same_peer_are_serialized() {
    let gates = PeerRequestGates::default();
    let first = gates.lock("device-a").await;

    assert!(
        tokio::time::timeout(Duration::from_millis(20), gates.lock("device-a"))
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), gates.lock("device-b"))
            .await
            .is_ok()
    );

    drop(first);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), gates.lock("device-a"))
            .await
            .is_ok()
    );
}
