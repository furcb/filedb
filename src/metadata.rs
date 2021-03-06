use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::path::Path;
use std::convert::TryFrom;

use crate::error::{Error, ErrorKind};
use crate::db;

type EkErr<T> = Result<T, Error>;
type EK = ErrorKind;

#[derive(Debug)]
pub struct Metadata {
    addresses: Vec<u64>,
    major_version: u32,
    minor_version: u32,
}

#[derive(Debug)]
pub enum Section {
    Metadata,
    Indexes,
    Data,
    DataEnd,
}

impl Metadata {
    pub fn new(addresses: Vec<u64>, major_version: u32, minor_version: u32) -> Self {
        Self {
            addresses,
            major_version,
            minor_version,
        }
    }

    pub fn capacity(&self) -> EkErr<u64> {
        let index_section_address;
        let data_section_address;

        index_section_address = self.section_address(Section::Indexes)?;
        data_section_address = self.section_address(Section::Data)?;

        Ok(
            (data_section_address - index_section_address) / db::INDEX_BLOCK_SIZE
        )
    }

    pub fn section_address(&self, section: Section) -> EkErr<u64> {
        let address;

        address = match section {
            Section::Metadata => self.addresses.get(0),
            Section::Indexes => self.addresses.get(1),
            Section::Data => self.addresses.get(2),
            Section::DataEnd => self.addresses.get(3),
        }
        // u64 so clone is just a copy
        .copied();

        address.ok_or(EK::MetadataMissing.into())
    }

    pub fn update_section_address(
        &mut self,
        file: &mut File,
        section: Section,
        address: u64,
    ) -> EkErr<()> {
        let index: usize;
        let index_address;
        let buffer;

        index = match section {
            Section::Metadata => 0,
            Section::Indexes => 1,
            Section::Data => 2,
            Section::DataEnd => 3,
        };

        index_address = (index as u64) * 8;
        buffer = address.to_be_bytes();

        file.seek(SeekFrom::Start(index_address))?;
        file.write_all(&buffer)?;

        // successfully updated address
        self.addresses[index] = address;

        Ok(())
    }

}

impl TryFrom<&Path> for Metadata {
    type Error = Error;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        let major_ver;
        let minor_ver;
        let mut current_pos;
        let mut file;
        let mut buffer;
        let mut addresses;

        file = File::open(&path)?;
        addresses = vec![];
        current_pos = 0;

        buffer = [0_u8; 8];

        // address section
        loop {
            file.read_exact(&mut buffer)?;
            addresses.push(u64::from_be_bytes(buffer));
            current_pos += 8; // first address to metadata start

            if current_pos >= addresses[0] {
                break;
            }
        }

        // change buffer length to 32 bits for versions
        let mut buffer = [0_u8; 4];

        // metadata section
        file.read_exact(&mut buffer)?;
        major_ver = u32::from_be_bytes(buffer);
        file.read_exact(&mut buffer)?;
        minor_ver = u32::from_be_bytes(buffer);

        Ok(Metadata::new(addresses, major_ver, minor_ver))
    }
}

