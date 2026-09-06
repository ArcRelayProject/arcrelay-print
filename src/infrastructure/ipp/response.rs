use super::*;

pub(super) fn operation_attributes() -> Vec<u8> {
    let mut payload = vec![0x01];
    attribute(&mut payload, 0x47, "attributes-charset", b"utf-8");
    attribute(&mut payload, 0x48, "attributes-natural-language", b"en");
    payload
}

pub(super) fn ipp_response(
    version: &[u8; 2],
    status: u16,
    request_id: u32,
    mut payload: Vec<u8>,
) -> Response<Body> {
    let mut bytes = Vec::with_capacity(payload.len() + 9);
    bytes.extend_from_slice(version);
    bytes.extend_from_slice(&status.to_be_bytes());
    bytes.extend_from_slice(&request_id.to_be_bytes());
    bytes.append(&mut payload);
    bytes.push(0x03);
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/ipp"),
        )
        .body(Body::from(bytes))
        .expect("valid IPP response")
}

pub(super) fn http_error(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::empty())
        .expect("valid HTTP error")
}

pub(super) fn attribute(output: &mut Vec<u8>, tag: u8, name: &str, value: &[u8]) {
    output.push(tag);
    output.extend_from_slice(&(name.len() as u16).to_be_bytes());
    output.extend_from_slice(name.as_bytes());
    output.extend_from_slice(&(value.len() as u16).to_be_bytes());
    output.extend_from_slice(value);
}

pub(super) fn repeated_attribute(output: &mut Vec<u8>, tag: u8, name: &str, values: &[&[u8]]) {
    for (index, value) in values.iter().enumerate() {
        attribute(output, tag, if index == 0 { name } else { "" }, value);
    }
}

pub(super) fn integer_attribute(output: &mut Vec<u8>, name: &str, value: i32) {
    attribute(output, 0x21, name, &value.to_be_bytes());
}

pub(super) fn enum_attribute(output: &mut Vec<u8>, name: &str, value: i32) {
    attribute(output, 0x23, name, &value.to_be_bytes());
}

pub(super) fn boolean_attribute(output: &mut Vec<u8>, name: &str, value: bool) {
    attribute(output, 0x22, name, &[u8::from(value)]);
}

pub(super) fn range_attribute(output: &mut Vec<u8>, name: &str, lower: i32, upper: i32) {
    let mut value = Vec::with_capacity(8);
    value.extend_from_slice(&lower.to_be_bytes());
    value.extend_from_slice(&upper.to_be_bytes());
    attribute(output, 0x33, name, &value);
}

pub(super) fn repeated_i32_attribute(output: &mut Vec<u8>, tag: u8, name: &str, values: &[i32]) {
    for (index, value) in values.iter().enumerate() {
        attribute(
            output,
            tag,
            if index == 0 { name } else { "" },
            &value.to_be_bytes(),
        );
    }
}

pub(super) fn resolution_attribute(
    output: &mut Vec<u8>,
    name: &str,
    horizontal: u32,
    vertical: u32,
) {
    let mut value = Vec::with_capacity(9);
    value.extend_from_slice(&(horizontal.min(i32::MAX as u32) as i32).to_be_bytes());
    value.extend_from_slice(&(vertical.min(i32::MAX as u32) as i32).to_be_bytes());
    value.push(3); // dots per inch
    attribute(output, 0x32, name, &value);
}

pub(super) fn repeated_resolution_attribute(
    output: &mut Vec<u8>,
    name: &str,
    resolutions: &[(u32, u32)],
) {
    for (index, &(horizontal, vertical)) in resolutions.iter().enumerate() {
        resolution_attribute(
            output,
            if index == 0 { name } else { "" },
            horizontal,
            vertical,
        );
    }
}

pub(super) fn collection_member_name(output: &mut Vec<u8>, name: &str) {
    attribute(output, 0x4a, "", name.as_bytes());
}

pub(super) fn collection_integer_member(output: &mut Vec<u8>, name: &str, value: i32) {
    collection_member_name(output, name);
    integer_attribute(output, "", value);
}

pub(super) fn media_col_default_attribute(
    output: &mut Vec<u8>,
    width_microns: u32,
    height_microns: u32,
) {
    attribute(output, 0x34, "media-col-default", b"");
    collection_member_name(output, "media-size");
    attribute(output, 0x34, "", b"");
    collection_integer_member(output, "x-dimension", (width_microns / 10) as i32);
    collection_integer_member(output, "y-dimension", (height_microns / 10) as i32);
    attribute(output, 0x37, "", b"");
    for margin in [
        "media-top-margin",
        "media-bottom-margin",
        "media-left-margin",
        "media-right-margin",
    ] {
        collection_integer_member(output, margin, 0);
    }
    attribute(output, 0x37, "", b"");
}
