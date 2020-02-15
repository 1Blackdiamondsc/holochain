use crate::conductor::CellHandle;
use std::fmt;
use sx_types::error::SkunkError;
use thiserror::Error;
use sx_core::cell::{CellId, error::CellError};

pub type ConductorResult<T> = Result<T, ConductorError>;

#[derive(Error, Debug)]
pub enum ConductorError {
    #[error(transparent)]
    SkunkError(#[from] SkunkError),

    #[error("Internal Cell error: {0}")]
    InternalCellError(#[from] CellError),

    #[error("Conductor API error: {0}")]
    ApiError(#[from] ConductorApiError),

    #[error("Cell is not active yet.")]
    CellNotActive,

    #[error("Cell is already active.")]
    CellAlreadyActive,

    #[error("Cell is not initialized.")]
    CellNotInitialized,

    #[error("Cell was referenced, but is missing from the conductor. CellId: {0:?}")]
    CellMissing(CellId),

    #[error("No such cell: {0}")]
    NoSuchCell(CellHandle),

    #[error("Required bridge missing. Detail: {0}")]
    RequiredBridgeMissing(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Miscellaneous error: {0}")]
    Misc(String),
}

impl From<String> for ConductorError {
    fn from(s: String) -> Self {
        ConductorError::Misc(s)
    }
}


impl PartialEq for ConductorError {
    fn eq(&self, other: &Self) -> bool {
        use ConductorError::*;
        match (self, other) {
            (InternalCellError(a), InternalCellError(b)) => a.to_string() == b.to_string(),
            (InternalCellError(_), _) => false,
            (_, InternalCellError(_)) => false,
            (a, b) => a == b
        }
    }
}


#[derive(Error, PartialEq, Debug)]
pub enum ConductorApiError {}

pub type ConductorApiResult<T> = Result<T, ConductorApiError>;