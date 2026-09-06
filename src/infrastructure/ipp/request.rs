use super::*;

pub(super) struct IppRequest {
    pub(super) version: [u8; 2],
    pub(super) operation: u16,
    pub(super) request_id: u32,
    pub(super) attributes: HashMap<String, Vec<Vec<u8>>>,
    pub(super) document_offset: u64,
    pub(super) document_size: u64,
}

impl IppRequest {
    pub(super) fn parse(bytes: &[u8], total_size: u64) -> Result<Self> {
        if bytes.len() < 9 {
            return Err(PrintError::Invalid("IPP request is too short".into()));
        }
        let version = [bytes[0], bytes[1]];
        let operation = u16::from_be_bytes([bytes[2], bytes[3]]);
        let request_id = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let mut position = 8;
        let mut attributes = HashMap::<String, Vec<Vec<u8>>>::new();
        let mut previous_name = String::new();
        loop {
            let tag = *bytes
                .get(position)
                .ok_or_else(|| PrintError::Invalid("IPP attributes are truncated".into()))?;
            position += 1;
            if tag == 0x03 {
                break;
            }
            if tag <= 0x0f {
                previous_name.clear();
                continue;
            }
            let name_length = read_u16(bytes, &mut position)? as usize;
            let name = if name_length == 0 {
                previous_name.clone()
            } else {
                let name = read_slice(bytes, &mut position, name_length)?;
                let name = std::str::from_utf8(name)
                    .map_err(|_| PrintError::Invalid("IPP attribute name is invalid".into()))?
                    .to_owned();
                previous_name = name.clone();
                name
            };
            let value_length = read_u16(bytes, &mut position)? as usize;
            let value = read_slice(bytes, &mut position, value_length)?.to_vec();
            attributes.entry(name).or_default().push(value);
        }
        let document_offset = position as u64;
        let document_size = total_size.checked_sub(document_offset).ok_or_else(|| {
            PrintError::Invalid("IPP request is shorter than its attributes".into())
        })?;
        Ok(Self {
            version,
            operation,
            request_id,
            attributes,
            document_offset,
            document_size,
        })
    }

    pub(super) fn text(&self, name: &str) -> Option<&str> {
        self.attributes
            .get(name)?
            .first()
            .and_then(|value| std::str::from_utf8(value).ok())
    }

    pub(super) fn integer(&self, name: &str) -> Option<i32> {
        let value: [u8; 4] = self
            .attributes
            .get(name)?
            .first()?
            .as_slice()
            .try_into()
            .ok()?;
        Some(i32::from_be_bytes(value))
    }

    pub(super) fn ranges(&self, name: &str) -> Vec<(i32, i32)> {
        self.attributes
            .get(name)
            .into_iter()
            .flatten()
            .filter_map(|value| {
                let value: [u8; 8] = value.as_slice().try_into().ok()?;
                Some((
                    i32::from_be_bytes(value[..4].try_into().ok()?),
                    i32::from_be_bytes(value[4..].try_into().ok()?),
                ))
            })
            .collect()
    }
}

pub(super) fn read_u16(bytes: &[u8], position: &mut usize) -> Result<u16> {
    let value = read_slice(bytes, position, 2)?;
    Ok(u16::from_be_bytes([value[0], value[1]]))
}

pub(super) fn read_slice<'a>(
    bytes: &'a [u8],
    position: &mut usize,
    length: usize,
) -> Result<&'a [u8]> {
    let end = position
        .checked_add(length)
        .ok_or_else(|| PrintError::Invalid("IPP attribute length overflow".into()))?;
    let value = bytes
        .get(*position..end)
        .ok_or_else(|| PrintError::Invalid("IPP attribute is truncated".into()))?;
    *position = end;
    Ok(value)
}

pub(super) async fn file_range_starts_with(
    path: &std::path::Path,
    offset: u64,
    prefix: &[u8],
) -> Result<bool> {
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut actual = vec![0_u8; prefix.len()];
    match file.read_exact(&mut actual).await {
        Ok(_) => Ok(actual == prefix),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(super) async fn sha256_file_range(
    path: &std::path::Path,
    offset: u64,
    size: u64,
) -> Result<[u8; 32]> {
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
                "IPP document ended before its declared size".into(),
            ));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(hasher.finalize().into())
}
