use byte_unit::ByteError;
use plotters::drawing::DrawingAreaErrorKind;
use std::fmt;
use std::io::ErrorKind;
use std::num::{ParseFloatError, ParseIntError};
use std::time::SystemTimeError;

///
/// Problems that can arise in fs-bench.
///
#[derive(Debug)]
pub enum Error {
    /// There was an error reading formatted data
    FormatError {
        format_of: String,
        detail: String,
    },

    /// Configuration information was incorrect
    InvalidConfig(String),

    /// There has been an attempt to access data at an invalid index
    InvalidIndex {
        kind: String,
        index: usize,
        max: usize,
    },

    /// An incorrect path was specified
    InvalidPath(String),

    /// An error occurred on the disk or network
    IO(std::io::Error),

    PlottersError(String),

    CsvError(String),

    SystemTimeError(String),

    Unknown(String),

    ParseError(String),

    WasmerError(String),

    /// A synchronization channel experienced an error
    SyncError(String),
}

impl Error {
    pub fn format<S1, S2>(format_of: S1, detail: S2) -> Error
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Error::FormatError {
            format_of: format_of.into(),
            detail: detail.into(),
        }
    }

    pub fn index<S>(kind: S, index: usize, max: usize) -> Error
    where
        S: Into<String>,
    {
        Error::InvalidIndex {
            kind: kind.into(),
            index,
            max,
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            &Error::IO(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        Error::WasmerError(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IO(err)
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> std::io::Error {
        std::io::Error::new(ErrorKind::Other, err)
    }
}

impl From<DrawingAreaErrorKind<std::io::Error>> for Error {
    fn from(err: DrawingAreaErrorKind<std::io::Error>) -> Self {
        Error::PlottersError(err.to_string())
    }
}

impl From<byte_unit::ByteError> for Error {
    fn from(err: ByteError) -> Self {
        Error::format("Byte conversion", err.to_string())
    }
}

impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Self {
        Error::CsvError(err.to_string())
    }
}

impl From<SystemTimeError> for Error {
    fn from(err: SystemTimeError) -> Self {
        Error::SystemTimeError(err.to_string())
    }
}

impl From<ParseFloatError> for Error {
    fn from(err: ParseFloatError) -> Self {
        Error::ParseError(err.to_string())
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Self {
        Error::ParseError(err.to_string())
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for Error {
    fn from(err: std::sync::mpsc::SendError<T>) -> Error {
        Error::SyncError(err.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::FormatError {
                ref format_of,
                ref detail,
            } => write!(f, "Format error in {}: {:?}", format_of, detail),
            &Error::IO(ref err) => write!(f, "IO error: {}", err),
            &Error::InvalidConfig(ref detail) => write!(f, "Configuration error: {}", detail),
            &Error::InvalidIndex {
                ref kind,
                index,
                max,
            } => write!(f, "Invalid {} index: {} (max: {})", kind, index, max),
            &Error::InvalidPath(ref path) => write!(f, "Invalid path: '{}'", path),
            &Error::PlottersError(ref detail) => write!(f, "Plotters error: {}", detail),
            &Error::CsvError(ref detail) => write!(f, "Csv error: {}", detail),
            &Error::SystemTimeError(ref detail) => write!(f, "SystemTime error: {}", detail),
            &Error::Unknown(ref detail) => write!(f, "Unknown error: {}", detail),
            &Error::ParseError(ref detail) => write!(f, "Parse error: {}", detail),
            &Error::WasmerError(ref detail) => write!(f, "Wasmer error: {}", detail),
            Error::SyncError(ref detail) => write!(f, "Sync error: {}", detail),
        }
    }
}
