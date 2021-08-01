










use std::fs::File;
use std::path::Path;
use std::error::Error;
use std::io::prelude::*;

type DynErr<T> = Result<T, Box<dyn Error>>;

#[derive(Debug)]
pub struct Metadata {
    pointers: Vec<u64>,
    major_version: u32,
    minor_version: u32,
}

#[derive(Debug)]
pub enum Section {
    Metadata,
    Indexes,
    Data,
}

impl Metadata {
    pub fn new(pointers: Vec<u64>, major_version: u32, minor_version: u32) -> Self {
        Self {
            pointers,
            major_version,
            minor_version,
        }
    }

    pub fn section_address(&self, section: Section) -> Option<u64> {
        match section {
            Section::Metadata => self.pointers.get(0),
            Section::Indexes => self.pointers.get(1),
            Section::Data => self.pointers.get(2),
        }
        // u64 so clone is just a copy
        .map(|v| v.clone())
    }
}

pub fn read_metadata(path: impl AsRef<Path>) -> DynErr<Metadata> {
    let major_ver;
    let minor_ver;
    let mut current_pos;
    let mut file;
    let mut buffer;
    let mut pointers;

    file = File::open(&path)?;
    pointers = vec!();
    current_pos = 0;

    buffer = [0_u8; 8];

    // pointer section
    loop {
        file.read(&mut buffer[..])?;
        pointers.push(u64::from_be_bytes(buffer));
        current_pos += 64; // first pointer to metadata start

        if current_pos >= pointers[0] {
            break
        }
    }

    // change buffer length to 32 bits for versions
    let mut buffer = [0_u8; 4];

    // metadata section
    file.read(&mut buffer)?;
    major_ver = u32::from_be_bytes(buffer);
    file.read(&mut buffer)?;
    minor_ver = u32::from_be_bytes(buffer);

    Ok(Metadata::new(
            pointers,
            major_ver,
            minor_ver,
    ))
}

