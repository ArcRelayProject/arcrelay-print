//! ArcRelay's public LAN printer-sharing bounded context.
//!
//! Printer sharing is deliberately independent from control-channel pairing:
//! a device publishes a printer to its LAN, and another device may install a
//! local queue that forwards jobs to that publisher. The domain layer contains
//! no operating-system, transport, or UI types.

pub mod application;
pub mod domain;
pub mod infrastructure;

pub use application::{PrintJobService, PrinterShareService, SystemQueueService};
pub use domain::*;

#[derive(Debug, thiserror::Error)]
pub enum PrintError {
    #[error("invalid print value: {0}")]
    Invalid(String),
    #[error("print state does not allow this operation: {0}")]
    InvalidState(String),
    #[error("print resource not found: {0}")]
    NotFound(String),
    #[error("print resource already exists: {0}")]
    Conflict(String),
    #[error("print backend failed: {0}")]
    Backend(String),
    #[error("print persistence failed: {0}")]
    Persistence(String),
    #[error("print serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("print I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

impl PrintError {
    /// Stable machine-readable category for transport and UI adapters.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "print.invalid_argument",
            Self::InvalidState(_) => "print.failed_precondition",
            Self::NotFound(_) => "print.not_found",
            Self::Conflict(_) => "print.conflict",
            Self::Backend(_) | Self::Io(_) => "print.unavailable",
            Self::Persistence(_) | Self::Serialization(_) => "print.internal",
        }
    }
}

pub type Result<T> = std::result::Result<T, PrintError>;
