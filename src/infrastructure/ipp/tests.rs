use super::*;

#[test]
fn parses_attributes_and_separates_document() {
    let mut bytes = vec![2, 0, 0, 2, 0, 0, 0, 7, 0x01];
    attribute(&mut bytes, 0x42, "job-name", b"Report");
    integer_attribute(&mut bytes, "copies", 2);
    bytes.push(0x03);
    bytes.extend_from_slice(b"%PDF-test");
    let request = IppRequest::parse(&bytes, bytes.len() as u64).unwrap();
    assert_eq!(request.request_id, 7);
    assert_eq!(request.text("job-name"), Some("Report"));
    assert_eq!(request.integer("copies"), Some(2));
    assert_eq!(&bytes[request.document_offset as usize..], b"%PDF-test");
    assert_eq!(request.document_size, 9);
}

#[test]
fn rejects_truncated_attributes() {
    let bytes = [2, 0, 0, 2, 0, 0, 0, 1, 0x01, 0x42];
    assert!(IppRequest::parse(&bytes, bytes.len() as u64).is_err());
}

#[test]
fn maps_page_ranges_and_collation_from_ipp() {
    let mut attributes = HashMap::new();
    attributes.insert(
        "page-ranges".to_string(),
        vec![[1_i32.to_be_bytes(), 3_i32.to_be_bytes()].concat()],
    );
    attributes.insert("sheet-collate".to_string(), vec![b"collated".to_vec()]);
    let request = IppRequest {
        version: [2, 0],
        operation: PRINT_JOB,
        request_id: 1,
        attributes,
        document_offset: 0,
        document_size: 1,
    };
    let options = options_from_request(&request);
    assert_eq!(options.page_ranges, vec![PageRange { first: 1, last: 3 }]);
    assert!(options.collate);
}

#[test]
fn maps_cups_page_size_keywords_to_ipp_media_names() {
    let mut attributes = HashMap::new();
    attributes.insert("media".to_string(), vec![b"A4".to_vec()]);
    let request = IppRequest {
        version: [2, 0],
        operation: PRINT_JOB,
        request_id: 1,
        attributes,
        document_offset: 0,
        document_size: 1,
    };

    assert_eq!(
        options_from_request(&request).media_name,
        "iso_a4_210x297mm"
    );
    assert_eq!(normalize_media_name("Letter"), "na_letter_8.5x11in");
}

#[test]
fn legacy_job_bindings_load_with_activity_defaults() {
    let binding: IppJobBinding = serde_json::from_str(
        r#"{"remoteDeviceId":"device","printerShareId":"share","remoteJobId":"job"}"#,
    )
    .unwrap();
    assert_eq!(binding.document_name, "");
    assert_eq!(binding.state, None);
    assert_eq!(binding.created_at_ms, 0);
}
