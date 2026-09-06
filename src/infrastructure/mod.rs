//! Infrastructure adapters for persistence, native print systems, IPP, and
//! the public print transport. Platform implementations are added behind the
//! application ports so the domain stays portable.

pub mod ipp;
pub mod persistence;
pub mod platform;
pub mod quic;
pub mod spool;
pub mod wire;
