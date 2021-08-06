
use std::fmt;

type EK = ErrorKind;

#[derive(Debug)]
pub struct Error {
    repr: Repr,
}

/// Error that can occur while mutating the database
#[derive(Debug)]
pub enum ErrorKind {
    /// Address arithmetic overflowed, addresses are u64 so it just means 12TB's has been reached.
    AddressOverflow,
    /// This can occur in instances where usize is not 64 bits.
    AddressConversionFailed,
    /// A buffer necessary to write to the data base was empty or not provided.
    BufferMissing,
    /// `FileDB` had a previous `std::io::Error` and file was dropped due to that error. Attempt to
    /// reload the database.
    DBFileMissing,
    /// Attempting to insert into an already written to index is not possible, to do this `update`
    /// _must_ be used.
    IndexAlreadyExists,
    /// Index is empty, generally useful for deciding whether to insert or update.
    IndexEmpty,
    /// Metadata is missing
    MetadataMissing,
    /// Block buffer not given, should only occur when attempting to write block to database.
    BlockDataNotProvided,
    /// Fatal error, leaves the database in an unrecoverable state. Manual intervention is
    /// necessary.
    SectionCorrupted,
}

#[derive(Debug)]
enum Repr {
    Wrapper(Box<dyn std::error::Error>),
    Simple(ErrorKind),
}

impl Error {
    pub fn into_inner(self) -> Option<Box<dyn std::error::Error>> {
        match self.repr {
            Repr::Wrapper(w) => Some(w),
            _ => None,
        }
    }
}

impl std::error::Error for Error {}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            repr: Repr::Simple(kind),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {
            repr: Repr::Wrapper(Box::new(err)),
        }
    }
}

impl ErrorKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match *self {
            EK::AddressOverflow => "address overflowed.",
            EK::AddressConversionFailed => "address conversion failed.",
            EK::BufferMissing => "write/Read data buffer is missing.",
            EK::DBFileMissing => "database file was dropped, no longer accessible.",
            EK::IndexAlreadyExists => "value cannot be inserted, already exists. Use update instead",
            EK::IndexEmpty => "value at index is empty, try inserting a value first.",
            EK::MetadataMissing => "unable to obtain metadata.",
            EK::BlockDataNotProvided => "trying to write block, index or value, without data.",
            EK::SectionCorrupted => "section corrupted, manual intervention necessary.",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        type R = Repr;

        match &self.repr {
            R::Simple(s) => write!(f, "FileDB: {}", s.as_str()),
            R::Wrapper(c) => write!(f, "FileDB experience an error with external library: {}", c),
        }
    }
}


