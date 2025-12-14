//! Response decoder module
//!
//! Supports: JSON, JSONL, CSV, XML
//!
//! # Overview
//!
//! The decode module provides parsers for common API response formats.
//! Each decoder extracts records from the response body using a configured path.

mod decoders;
mod types;

pub use decoders::{CsvDecoder, JsonDecoder, JsonlDecoder, XmlDecoder};
pub use types::{DecoderConfig, DecoderFormat, RecordDecoder};

#[cfg(test)]
mod tests;
