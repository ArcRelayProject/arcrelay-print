use serde::{Deserialize, Serialize};

use crate::{PrintError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum ColorMode {
    Monochrome,
    Color,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum DuplexMode {
    OneSided,
    TwoSidedLongEdge,
    TwoSidedShortEdge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Orientation {
    Portrait,
    Landscape,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct MediaSize {
    pub name: String,
    pub width_microns: u32,
    pub height_microns: u32,
}

impl MediaSize {
    pub fn new(name: impl Into<String>, width_microns: u32, height_microns: u32) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() || width_microns == 0 || height_microns == 0 {
            return Err(PrintError::Invalid("invalid media size".into()));
        }
        Ok(Self {
            name,
            width_microns,
            height_microns,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct PrintResolution {
    pub horizontal_dpi: u32,
    pub vertical_dpi: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct PrinterCapabilities {
    pub media_sizes: Vec<MediaSize>,
    pub color_modes: Vec<ColorMode>,
    pub duplex_modes: Vec<DuplexMode>,
    pub resolutions: Vec<PrintResolution>,
    pub max_copies: u32,
    pub supports_page_ranges: bool,
    pub supports_collation: bool,
    pub accepted_document_types: Vec<String>,
}

impl PrinterCapabilities {
    pub fn validate(&self) -> Result<()> {
        if self.media_sizes.is_empty() {
            return Err(PrintError::Invalid(
                "printer must advertise at least one media size".into(),
            ));
        }
        if self.max_copies == 0 {
            return Err(PrintError::Invalid(
                "printer max copies must be positive".into(),
            ));
        }
        if self.accepted_document_types.is_empty() {
            return Err(PrintError::Invalid(
                "printer must accept at least one document type".into(),
            ));
        }
        Ok(())
    }
}
