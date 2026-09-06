use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{PrintError, Result};

macro_rules! opaque_id {
    ($name:ident, $label:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ts_rs::TS)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn parse(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                if value.trim().is_empty() || value.len() > 512 {
                    return Err(PrintError::Invalid(format!(
                        "{} must contain 1 to 512 characters",
                        $label
                    )));
                }
                Ok(Self(value))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

opaque_id!(DeviceId, "device id");
opaque_id!(LocalPrinterId, "local printer id");
opaque_id!(SystemQueueId, "system queue id");
opaque_id!(NativeJobId, "native job id");
opaque_id!(ClientJobId, "client job id");

macro_rules! aggregate_id {
    ($name:ident, $label:literal) => {
        opaque_id!($name, $label);

        impl $name {
            #[must_use]
            pub fn random() -> Self {
                Self(Uuid::new_v4().to_string())
            }
        }
    };
}

aggregate_id!(PrinterShareId, "printer share id");
aggregate_id!(QueueBindingId, "queue binding id");
aggregate_id!(PrintJobId, "print job id");
