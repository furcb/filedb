use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{self, prelude::*, BufWriter, SeekFrom};
use std::mem;
use std::path::Path;

use crate::metadata::{self, Metadata, Section};

type DynErr<T> = Result<T, Box<dyn Error>>;
type EKErr<T> = Result<T, ErrorKind>;

pub const MJR_VER: u32 = 0;
pub const MIN_VER: u32 = 0;

#[derive(Debug)]
pub enum ErrorKind {
    IO(io::Error),
    AddressOverflow,
    AddressConversionFailed,
    BufferMissing,
    DBFileMissing,
    IndexAlreadyExists,
    IndexEmpty,
    MetadataMissing,
    BlockDataNotProvided,
}

#[derive(Debug)]
/// `IndexBlock` container for the tuple in an index block, both values are u64.
///
/// Data block values, address and length, are options for instances when values are not known
/// ahead of time in the case of an access, they can be set to `Option::None`.
pub struct IndexBlock {
    /// Block address for the index block itself
    index_address: u64,
    /// Relative address of a data block
    address: Option<u64>,
    /// Length of the block itself
    length: Option<u64>,
}

#[derive(Debug)]
pub struct DataBlock<'a> {
    address: u64,
    length: u64,
    data: Option<&'a [u8]>,
}

#[derive(Debug)]
pub struct FileDB {
    file_db: Option<File>,
    metadata: Metadata,
}

impl IndexBlock {
    pub fn new(index_address: u64, address: Option<u64>, length: Option<u64>) -> Self {
        Self {
            index_address,
            address,
            length,
        }
    }

    /// Return `IndexBlock` data based on index in Indexes
    ///
    /// If look up fails for any reason assume file had an issue and is no longer accessible
    fn from_filedb(file: &mut File, metadata: &Metadata, index: u64) -> EKErr<Self> {
        let index_section_address;
        let index_address;
        let mut index_block;

        // if index section does not exist return None
        index_section_address = metadata
            .section_address(Section::Indexes)
            .ok_or(ErrorKind::MetadataMissing)?;
        index_address = index_section_address + (16 * index);

        index_block = IndexBlock::new(index_address, None, None);
        IndexBlock::read_from_file(&mut index_block, file)?;

        eprintln!("{:?}", index_block);

        Ok(index_block)
    }

    fn try_from_metadata(metadata: &Metadata, index: u64, data: &[u8]) -> EKErr<Self> {
        let index_address;
        let address;
        let length;

        index_address = metadata
            .section_address(Section::Indexes)
            .ok_or(ErrorKind::MetadataMissing)?;
        address = metadata
            .section_address(Section::DataEnd)
            .ok_or(ErrorKind::MetadataMissing)?;
        length = data
            .len()
            .try_into()
            .map_err(|_| ErrorKind::AddressConversionFailed)?;

        Ok(Self {
            index_address: index_address + (index * 16),
            address: Some(address),
            length: Some(length),
        })
    }

    pub fn address(&self) -> Result<u64, ErrorKind> {
        self.address.ok_or(ErrorKind::BlockDataNotProvided)
    }

    pub fn length(&self) -> Result<u64, ErrorKind> {
        self.length.ok_or(ErrorKind::BlockDataNotProvided)
    }

    pub fn address_lossy(&self) -> u64 {
        self.address.unwrap_or(0)
    }

    pub fn length_lossy(&self) -> u64 {
        self.length.unwrap_or(0)
    }

    pub fn write_to_file(&self, file: &mut File) -> EKErr<()> {
        let address;
        let length;
        let mut buffer;

        address = self.address()?;
        length = self.length()?;

        // write new block information
        file.seek(SeekFrom::Start(self.index_address))
            .map_err(ErrorKind::IO)?;
        buffer = address.to_be_bytes();
        file.write_all(&buffer).map_err(ErrorKind::IO)?;

        buffer = length.to_be_bytes();
        file.write_all(&buffer).map_err(ErrorKind::IO)?;

        Ok(())
    }

    pub fn read_from_file(&mut self, file: &mut File) -> EKErr<()> {
        let address;
        let length;
        let mut buffer;

        buffer = [0_u8; 8];

        // get index position
        file.seek(SeekFrom::Start(self.index_address))
            .map_err(ErrorKind::IO)?;
        file.read_exact(&mut buffer).map_err(ErrorKind::IO)?;
        address = u64::from_be_bytes(buffer);

        file.read_exact(&mut buffer).map_err(ErrorKind::IO)?;
        length = u64::from_be_bytes(buffer);

        self.address = Some(address);
        self.length = Some(length);

        Ok(())
    }
}

impl Default for IndexBlock {
    fn default() -> Self {
        Self {
            index_address: 0,
            address: None,
            length: None,
        }
    }
}

impl<'a> DataBlock<'a> {
    /// Creates a new DataBlock that can be used to read to or write from a data section segment of
    /// memory
    ///
    /// Start must be relative to whole file, since the expected workflow is to use the data
    /// section end address
    pub fn new(address: u64, length: u64, data: Option<&'a [u8]>) -> Self {
        Self {
            address,
            length,
            data,
        }
    }

    pub fn with_data(mut self, data: &'a [u8]) -> Self {
        self.data = Some(data);
        self
    }

    /// Writes the data that was provided
    ///
    /// Can fail for IO reason, but also if no data was provided. For example expected use was to
    /// read a block but mistakenly call write instead.
    pub fn write_block(&mut self, file: &mut File) -> EKErr<()> {
        let buffer;

        // take data
        buffer = self.data.take().ok_or(ErrorKind::BufferMissing)?;

        // write data
        file.seek(SeekFrom::Start(self.address))
            .map_err(ErrorKind::IO)?;
        file.write_all(buffer).map_err(ErrorKind::IO)?;

        // replace buffer
        self.data = Some(buffer);

        Ok(())
    }

    /// Reads the data in the data block but does not decode it
    ///
    /// Can fail for IO reasons
    ///
    /// Useful for reading a block, but also validating that a block contains information before
    /// writing to it.
    pub fn read_block(&mut self, file: &mut File) -> EKErr<Vec<u8>> {
        let mut buffer;

        buffer = vec![0_u8; self.length as usize];

        file.seek(SeekFrom::Start(self.address))
            .map_err(ErrorKind::IO)?;
        file.read_exact(&mut buffer).map_err(ErrorKind::IO)?;

        Ok(buffer)
    }
}

impl<'a> TryFrom<&IndexBlock> for DataBlock<'a> {
    type Error = ErrorKind;

    fn try_from(value: &IndexBlock) -> Result<Self, Self::Error> {
        Ok(Self {
            address: value.address()?,
            length: value.length()?,
            data: None,
        })
    }
}

impl FileDB {
    pub fn create_db(path: impl AsRef<Path>, expected_capacity: u64) -> EKErr<Self> {
        // TODO: improve this, better to do just one file open and save the generated data as we go
        // and return it as a FileDB instance

        // path does not exist create a new db
        if !path.as_ref().exists() {
            crate::db::create_db(&path, expected_capacity)?;
        }

        // load the db
        FileDB::load_db(&path)
    }

    pub fn load_db(path: impl AsRef<Path>) -> EKErr<Self> {
        let metadata;
        let file;

        file = OpenOptions::new()
            .read(true)
            .write(true)
            //.append(true) // WARN: caused seek to fail for some reason, don't use
            .open(&path)
            .map_err(ErrorKind::IO)?;

        metadata = Metadata::try_from(path.as_ref())?;

        Ok(Self {
            file_db: Some(file),
            metadata,
        })
    }

    pub fn get(&mut self, index: u64) -> EKErr<Vec<u8>> {
        let value;
        let index_block;
        let mut file;
        let mut data_block;

        // take file
        file = self.file_db.take().ok_or(ErrorKind::DBFileMissing)?;

        // if any section does not exist return None
        index_block = IndexBlock::from_filedb(&mut file, &self.metadata, index)?;
        data_block = DataBlock::try_from(&index_block)?;

        value = data_block.read_block(&mut file)?;

        // give back file
        self.file_db = Some(file);

        Ok(value)
    }

    pub fn insert(&mut self, index: u64, data: &[u8]) -> EKErr<()> {
        // inserts an item into an empty position
        let index_block;
        let mut data_section_end_address;
        let mut data_block;
        let mut file;

        // data section end
        data_section_end_address = self
            .metadata
            .section_address(Section::DataEnd)
            .ok_or(ErrorKind::MetadataMissing)?;

        // at this point we should have a valid state were we just need to catalog the position of
        // new data and write the new data

        file = self.file_db.take().ok_or(ErrorKind::DBFileMissing)?;

        // if index is empty we can insert otherwise propagate the error or say index already
        // exists
        match IndexBlock::from_filedb(&mut file, &self.metadata, index) {
            Err(ErrorKind::IndexEmpty) => (),
            Err(e) => return Err(e),
            Ok(_) => return Err(ErrorKind::IndexAlreadyExists),
        }

        // index does not exist so create a new one
        index_block = IndexBlock::try_from_metadata(&self.metadata, index, &data)?;

        // setup data block metadata
        data_block = DataBlock::try_from(&index_block)?.with_data(data);

        // update end of data address
        data_section_end_address = data_section_end_address
            .checked_add(
                data.len()
                    .try_into()
                    .map_err(|_| ErrorKind::AddressConversionFailed)?,
            )
            .ok_or(ErrorKind::AddressOverflow)?;

        // modify data section
        data_block.write_block(&mut file)?;

        // update index block
        index_block.write_to_file(&mut file)?;

        // update data end address

        self.metadata
            .update_section_address(&mut file, Section::DataEnd, data_section_end_address)?;

        self.file_db = Some(file);

        Ok(())
    }

    pub fn update(&mut self, index: u64, data: &[u8], resize: bool) -> DynErr<()> {
        // TODO: if resize is false and data is not exactly the same size as old data return an
        // error, otherwise replace old data. If resize is true and data is the exact same size
        // don't resize.
        // Strategy: any updates that are not the same size as the original data block will be
        // moved to the end of the data section. This is better than adjusting the old location
        // because it removes any additional calculations for movements.
        // update is then a delete and insert with a check on whether we resize or not
        todo!()
    }

    pub fn delete(&mut self, index: u64) -> DynErr<()> {
        // TODO: don't actually the delete the item, just set index default again
        todo!();
    }

    /// Obtain the byte position in the file
    fn eof_position(&mut self) -> Result<u64, ErrorKind> {
        let file;
        let eof_position;

        file = self.file_db.take().ok_or(ErrorKind::DBFileMissing)?;

        match file.metadata() {
            Ok(m) => eof_position = m.len(),
            Err(e) => return Err(ErrorKind::IO(e)),
        }

        self.file_db = Some(file);

        Ok(eof_position)
    }

    fn shift_data(&mut self, start: u64, amount: i64) -> DynErr<()> {
        todo!();

    }
}

/// Creates an initialized FileDB
///
/// `expected_capacity` must be a power of 2.
fn create_db(path: impl AsRef<Path>, expected_capacity: u64) -> EKErr<()> {
    let section_addresses;
    let start_section;
    let index_section;
    let data_section;
    let data_section_end;
    let adjusted_capacity;
    let mut file;
    let mut indexes;
    let mut index_default;
    let mut count;

    // make sure that indexes segment size is within u64 bounds
    // 1 index block is 128 bit long or 16 u8's
    // make sure that u64 is smaller or equal to usize for array bounds
    debug_assert!(mem::size_of::<u64>() <= mem::size_of::<usize>());
    // shadow capacity with section length in bytes
    adjusted_capacity = expected_capacity
        .checked_mul(16)
        .ok_or(ErrorKind::AddressOverflow)?;

    // [ metadata start, index start, data start ]
    start_section = 4 * 8; // each u64 is 8 bytes (64bit)
    index_section = start_section + 2 * 4; // each u32 is 4 bytes (32bits)
    data_section = index_section + adjusted_capacity; // each u32 is 4 bytes (32bits)
    data_section_end = data_section; // 1 byte to serve as a null byte (dropped in favor of just using zero)

    section_addresses = vec![start_section, index_section, data_section, data_section_end];

    file = File::create(&path).map_err(ErrorKind::IO)?;

    // write addresses to file
    for address in section_addresses {
        file.write_all(&address.to_be_bytes())
            .map_err(ErrorKind::IO)?;
    }

    // write metadata to file
    file.write_all(&MJR_VER.to_be_bytes())
        .map_err(ErrorKind::IO)?;
    file.write_all(&MIN_VER.to_be_bytes())
        .map_err(ErrorKind::IO)?;

    // write indexes to file
    count = 0;
    indexes = vec![];
    index_default = vec![];
    // extend twice since an index is two u64 numbers
    index_default.extend_from_slice(&0_u64.to_be_bytes());
    index_default.extend_from_slice(&0_u64.to_be_bytes());

    while count < expected_capacity {
        indexes.extend_from_slice(&index_default);

        count += 1;
    }

    file.write_all(&indexes).map_err(ErrorKind::IO)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::metadata;
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn get_basic() {
        let mut fdb;

        fdb = load_db();

        eprintln!("{}", String::from_utf8_lossy(&fdb.get(0).unwrap()));
        eprintln!("{}", String::from_utf8_lossy(&fdb.get(1).unwrap()));
        assert!(false)
    }

    #[test]
    fn insert_basic() {
        let mut fdb;

        fdb = load_db();

        fdb.insert(0, "hello world how are you".as_bytes()).unwrap();
        fdb.insert(1, "the sky is blue and the clouds gray".as_bytes())
            .unwrap();

        for i in 2..100 {
            fdb.insert(i, "this is a test".as_bytes()).unwrap();
        }

        assert!(false)
    }

    #[test]
    #[ignore]
    fn eof_is_correct() {
        let path;
        let mut filedb;

        path = PathBuf::from("./test_space/filedb");
        filedb = FileDB::load_db(&path).unwrap();

        eprintln!("file length: {}", filedb.eof_position().unwrap());
        eprintln!(
            "data section: {}",
            filedb
                .metadata
                .section_address(metadata::Section::Data)
                .unwrap()
        );
    }

    #[test]
    #[ignore]
    fn create_basic() {
        let path;

        path = PathBuf::from("./test_space/filedb");

        if !path.exists() {
            create_db(&path, 256).unwrap();
        }

        eprintln!("{:?}", Metadata::try_from(path.as_path()).unwrap());

        assert!(false);
    }

    fn load_db() -> FileDB {
        let path;

        path = PathBuf::from("./test_space/filedb");

        FileDB::load_db(&path).unwrap()
    }
}
