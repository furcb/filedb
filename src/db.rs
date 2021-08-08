use std::convert::{TryFrom, TryInto};
use std::fs::{File, OpenOptions};
use std::io::{prelude::*, SeekFrom};
use std::mem;
use std::path::Path;

use crate::metadata::{Metadata, Section};
use crate::error::{Error, ErrorKind};
use crate::iter;

type EK = ErrorKind;
type EkErr<T> = Result<T, Error>;

/// Major version: Incremented when changes are made to the specification or implementation that
/// break backwards compatibility. Baked into the metadata of the file.
pub const MJR_VER: u32 = 0;
/// Minor version: Incremented when changes are introduced that do not break backwards
/// compatibility. Baked into the metadata of the file.
pub const MIN_VER: u32 = 1;

/// Block width of an individual `IndexBlock`, 16 bytes or two 64bit numbers.
pub const INDEX_BLOCK_SIZE: u64 = 16;

/// `IndexBlock` container for the tuple in an index block, both values are u64.
///
/// Data block values, address and length, are options for instances when values are not known
/// ahead of time in the case of an access, they can be set to `Option::None`.
#[derive(Debug)]
pub struct IndexBlock {
    /// Block address for the index block itself
    index_address: u64,
    /// Relative address of a data block
    address: Option<u64>,
    /// Length of the block itself
    length: Option<u64>,
}

/// `DataBlock` is a container for the metadata of that block as well as the content. Address and
/// length are u64, and the data is a byte slice.
#[derive(Debug)]
pub struct DataBlock {
    address: u64,
    length: u64,
    data: Option<Vec<u8>>,
}

/// `FileDB` is the main point of interaction, it contains a mutable pointer to a `File` and an
/// owned `Metadata` of FileDB.
///
/// Actions:
///
/// - Are in-place unless otherwise stated, so temporary files are not created.
/// - Mutating the FileDB can should only be done through `FileDB`.
///
/// Warning:
///
/// - Some actions can leave FileDB in an unrecoverable state due to the fact that changes are done
/// in place. Only insert changes should be considered safe. They are only applied after a
/// successful write.  In the event a write fails, addresses are not updated allowing for another
/// attempt at a write.
#[derive(Debug)]
pub struct FileDB {
    file_db: Option<File>,
    metadata: Metadata,
}

impl IndexBlock {
    /// Create an "un-initialized" `IndexBlock`
    pub fn new(index_address: u64) -> Self {
        Self {
            index_address,
            address: None,
            length: None,
        }
    }

    /// Set the address, used when the intention is to write to the file
    pub fn with_address(mut self, address: u64) -> Self {
        self.address = Some(address);
        self
    }

    /// Set the length, used when the intention is to write to the file
    pub fn with_length(mut self, length: u64) -> Self {
        self.length = Some(length);
        self
    }

    pub(crate) fn try_from_address(file: &mut File, section_address: u64, index: u64) -> EkErr<Self> {
        let index_address;
        let mut index_block;

        index_address = section_address + (INDEX_BLOCK_SIZE * index);

        index_block = IndexBlock::new(index_address);
        IndexBlock::read_from_file(&mut index_block, file)?;

        if index_block.address()? == 0 {
            Err(EK::IndexEmpty.into())
        } else {
            Ok(index_block)
        }
    }

    /// Return `IndexBlock` data based on index in Indexes
    ///
    /// If look up fails for any reason assume file had an issue and is no longer accessible
    fn try_from_filedb(file: &mut File, metadata: &Metadata, index: u64) -> EkErr<Self> {
        let index_section_address;

        index_section_address = metadata.section_address(Section::Indexes)?;

        IndexBlock::try_from_address(file, index_section_address, index)
    }

    /// Create an `IndexBlock` from `Metadata` information, `index` is necessary so the
    /// `index_address` can be calculated. `data` for the length of length of the `DataBlock`.
    /// The data block address can be automatically calculated.
    fn try_from_metadata(metadata: &Metadata, index: u64, data: &[u8]) -> EkErr<Self> {
        let index_address;
        let address;
        let length;

        index_address = metadata.section_address(Section::Indexes)?;
        address = metadata.section_address(Section::DataEnd)?;
        length = data
            .len()
            .try_into()
            .map_err(|_| EK::AddressConversionFailed)?;

        Ok(Self {
            index_address: index_address + (index * INDEX_BLOCK_SIZE),
            address: Some(address),
            length: Some(length),
        })
    }

    /// Get the data block address, may be un-initialized or an error could occur on retrieval
    pub fn address(&self) -> EkErr<u64> {
        // NOTE: since an address can only be zero or section_data + offset, we don't need to
        // consider the case where 0 < address < section_address. At that point FileDB is probably
        // in an unrecoverable state.
        if self.address.is_none() {
            Err(EK::BlockDataNotProvided.into())
        } else if self.address.unwrap() == 0 {
            Err(EK::IndexEmpty.into())
        } else {
            Ok(self.address.unwrap())
        }
    }

    /// Get the data block length, may be un-initialized or an error could occur on retrieval
    pub fn length(&self) -> EkErr<u64> {
        self.length.ok_or(EK::BlockDataNotProvided.into())
    }

    /// Get the data block address or return a zero
    pub fn address_lossy(&self) -> u64 {
        self.address.unwrap_or(0)
    }

    /// Get the data block length or return a zero
    pub fn length_lossy(&self) -> u64 {
        self.length.unwrap_or(0)
    }

    pub fn reset_index(&mut self, file: &mut File) -> EkErr<()> {
        let address;
        let length;

        address = 0_u64;
        length = 0_u64;

        self.address = Some(address);
        self.length = Some(length);

        self.write_block_data(file, address, length)
    }

    /// Write the `IndexBlock` information to the file
    pub fn write_to_file(&self, file: &mut File) -> EkErr<()> {
        let address;
        let length;

        address = self.address()?;
        length = self.length()?;

        self.write_block_data(file, address, length)
    }

    /// Read and parse the `IndexBlock` in the file
    pub fn read_from_file(&mut self, file: &mut File) -> EkErr<()> {
        let address;
        let length;
        let mut buffer;

        buffer = [0_u8; 8];

        // get index position
        file.seek(SeekFrom::Start(self.index_address))?;
        file.read_exact(&mut buffer)?;
        address = u64::from_be_bytes(buffer);

        file.read_exact(&mut buffer)?;
        length = u64::from_be_bytes(buffer);

        self.address = Some(address);
        self.length = Some(length);

        Ok(())
    }

    fn write_block_data(&self, file: &mut File, address: u64, length: u64) -> EkErr<()> {
        let mut buffer;

        // write new block information
        file.seek(SeekFrom::Start(self.index_address))?;
        buffer = address.to_be_bytes();
        file.write_all(&buffer)?;

        buffer = length.to_be_bytes();
        file.write_all(&buffer)?;

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

impl DataBlock {
    /// Creates a new DataBlock that can be used to read to or write from a data section segment of
    /// memory
    ///
    /// Start must be relative to whole file, since the expected workflow is to use the data
    /// section end address
    pub fn new(address: u64, length: u64, data: Option<Vec<u8>>) -> Self {
        Self {
            address,
            length,
            data,
        }
    }

    /// Assign the data in builder fashion
    pub fn with_data(mut self, data: Vec<u8>) -> Self {
        self.data = Some(data);
        self
    }

    /// Writes the data that was provided
    ///
    /// Can fail for IO reason, but also if no data was provided. For example expected use was to
    /// read a block but mistakenly call write instead.
    pub fn write_block(&mut self, file: &mut File) -> EkErr<()> {
        let buffer;

        // take data
        buffer = self.data.take().ok_or(EK::BufferMissing)?;

        // write data
        file.seek(SeekFrom::Start(self.address))?;
        file.write_all(buffer.as_slice())?;

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
    pub fn read_block(&mut self, file: &mut File) -> EkErr<Vec<u8>> {
        let mut buffer;

        buffer = vec![0_u8; self.length as usize];

        file.seek(SeekFrom::Start(self.address))?;
        file.read_exact(&mut buffer)?;

        Ok(buffer)
    }
}

impl TryFrom<&IndexBlock> for DataBlock {
    type Error = Error;

    fn try_from(value: &IndexBlock) -> Result<Self, Self::Error> {
        Ok(Self {
            address: value.address()?,
            length: value.length()?,
            data: None,
        })
    }
}

impl FileDB {
    /// Create a new empty data base with an expected capacity.
    ///
    /// Expected capacity must be known at creation time otherwise section lengths would not be
    /// able to be calculated. Even if `FileDB` had the ability to grow and shrink the Metadata
    /// sections, we must know the length of each section except the data section.
    pub fn create_db(path: impl AsRef<Path>, expected_capacity: u64) -> EkErr<Self> {
        // TODO: improve this, better to do just one file open and save the generated data as we go
        // and return it as a FileDB instance

        // path does not exist create a new db
        if !path.as_ref().exists() {
            crate::db::create_db(&path, expected_capacity)?;
        }

        // load the db
        FileDB::load_db(&path)
    }

    /// Load `FileDB` at the given path.
    pub fn load_db(path: impl AsRef<Path>) -> EkErr<Self> {
        let metadata;
        let file;

        file = OpenOptions::new()
            .read(true)
            .write(true)
            //.append(true) // WARN: caused seek to fail for some reason, don't use
            .open(&path)?;

        metadata = Metadata::try_from(path.as_ref())?;

        Ok(Self {
            file_db: Some(file),
            metadata,
        })
    }

    /// Retrieve a byte vector of the data at a given index
    pub fn get(&mut self, index: u64) -> EkErr<Vec<u8>> {
        let value;
        let index_block;
        let mut file;
        let mut data_block;

        // take file
        file = self.file_db.take().ok_or(EK::DBFileMissing)?;

        // if any section does not exist return None
        index_block = IndexBlock::try_from_filedb(&mut file, &self.metadata, index)?;
        data_block = DataBlock::try_from(&index_block)?;

        value = data_block.read_block(&mut file)?;

        // give back file
        self.file_db = Some(file);

        Ok(value)
    }

    /// Insert a byte vector at the given index
    pub fn insert(&mut self, index: u64, data: &[u8]) -> EkErr<()> {
        // inserts an item into an empty position
        let index_block;
        let mut data_section_end_address;
        let mut data_block;
        let mut file;

        // data section end
        data_section_end_address = self.metadata.section_address(Section::DataEnd)?;

        // at this point we should have a valid state were we just need to catalog the position of
        // new data and write the new data

        file = self.file_db.take().ok_or(EK::DBFileMissing)?;

        // if index is empty we can insert otherwise propagate the error or say index already
        // exists
        match IndexBlock::try_from_filedb(&mut file, &self.metadata, index) {
            //Err(EK::IndexEmpty) => (),
            //Err(e) => return Err(e),
            Err(_) => (),
            Ok(_) => return Err(EK::IndexAlreadyExists.into()),
        }

        // index does not exist so create a new one
        index_block = IndexBlock::try_from_metadata(&self.metadata, index, &data)?;

        // setup data block metadata
        data_block = DataBlock::try_from(&index_block)?.with_data(Vec::from(data));

        // update end of data address
        data_section_end_address = data_section_end_address
            .checked_add(
                data.len()
                    .try_into()
                    .map_err(|_| EK::AddressConversionFailed)?,
            )
            .ok_or(EK::AddressOverflow)?;

        // modify data section
        data_block.write_block(&mut file)?;

        // update index block
        index_block.write_to_file(&mut file)?;

        // update data end address

        self.metadata.update_section_address(
            &mut file,
            Section::DataEnd,
            data_section_end_address,
        )?;

        self.file_db = Some(file);

        Ok(())
    }

    /// Update the value at a given index
    ///
    /// Alias for a delete and insert
    ///
    /// `resize` currently does not due anything, reserved parameter
    pub fn update(&mut self, index: u64, data: &[u8]) -> EkErr<()> {
        // TODO: if resize is false and data is not exactly the same size as old data return an
        // error, otherwise replace old data. If resize is true and data is the exact same size
        // don't resize.
        // Strategy: any updates that are not the same size as the original data block will be
        // moved to the end of the data section. This is better than adjusting the old location
        // because it removes any additional calculations for movements.
        // update is then a delete and insert with a check on whether we resize or not
        self.delete(index)?;
        self.insert(index, data)?;

        Ok(())
    }

    /// Delete value at index and resets the index
    ///
    /// Deleting is costly as it requires shifting the data in the database and updating all index
    /// values even index position less than the one specified. This is due to the fact that index
    /// order does not translate to data order.
    pub fn delete(&mut self, index: u64) -> EkErr<Vec<u8>> {
        let data;
        let data_section_end_address;
        let new_section_end_address;
        let mut file;

        data_section_end_address = self.metadata.section_address(Section::DataEnd)?;

        file = self.take_file()?;

        // delete block
        data = FileDB::delete_block(&mut file, &self.metadata, index)?;
        new_section_end_address = data_section_end_address - data.len() as u64;

        // update section address by deleted data length
        self.metadata.update_section_address(
            &mut file,
            Section::DataEnd,
            new_section_end_address,
        )?;

        file.set_len(new_section_end_address)?;

        // give back file
        self.file_db = Some(file);

        Ok(data)
    }

    pub fn take_file(&mut self) -> EkErr<File> {
        self.file_db.take().ok_or(EK::DBFileMissing.into())
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn into_index_iter(self) -> EkErr<iter::IntoIndexIterator> {
        iter::IntoIndexIterator::try_from(self)
    }

    pub fn into_data_iter(self) -> EkErr<iter::IntoDataIterator> {
        iter::IntoDataIterator::try_from(self)
    }

    pub fn into_indexed_iter(self) -> EkErr<iter::IntoIndexedIterator> {
        iter::IntoIndexedIterator::try_from(self)
    }

    /// Update all index values within the index section
    ///
    /// Useful when a deletion or update occurs.
    ///
    /// This is costly as _all_ indexes must be updated due to the nature of how data is inserted
    /// in the data section, and all indexes act as pointers rather than offset markers.
    ///
    /// Warning: Failing here can potentially leave the database in an unrecoverable state without
    /// manual intervention. Since data is in the data section is unordered, unless it is easily
    /// segmented visually or by other means, it is impossible to use a corrupted index list to
    /// view data.
    fn update_indexes(
        file: &mut File,
        metadata: &Metadata,
        cutoff: u64,
        shift_amount: u64,
    ) -> EkErr<()> {
        // any index with an address > cutoff
        // needs the data address shifted left by the `shift_amount`

        // get the byte slice of the entire section, chunk by index size
        // write updated section
        let index_section_start;
        let index_section_end;
        let mut chunked_buffer;
        let mut index_buffer;

        index_section_start = metadata.section_address(Section::Indexes)?;
        index_section_end = metadata.section_address(Section::Data)?;

        index_buffer = vec![0_u8; (index_section_end - index_section_start) as usize];

        file.seek(SeekFrom::Start(index_section_start))?;
        file.read_exact(&mut index_buffer)?;

        chunked_buffer = index_buffer.chunks_mut(8).peekable();

        loop {
            let length_chunk;
            let address_chunk;
            let mut tmp_slice;
            let mut address;

            address_chunk = chunked_buffer.next().ok_or(EK::SectionCorrupted)?;
            length_chunk = chunked_buffer.next().ok_or(EK::SectionCorrupted)?;

            tmp_slice = [0_u8; 8];

            // if chunk is not 16, then something went wrong and addresses are of, which means the
            // data is not accessible
            if address_chunk.len() < 8 || length_chunk.len() < 8 {
                return Err(EK::SectionCorrupted.into());
            }

            // parse chunks
            tmp_slice.copy_from_slice(&address_chunk);
            address = u64::from_be_bytes(tmp_slice);

            // check values and adjust values
            if address > cutoff {
                // adjust value
                address -= shift_amount;

                // update value
                address_chunk.copy_from_slice(&address.to_be_bytes());
            }

            if chunked_buffer.peek().is_none() {
                break;
            }
        }

        file.seek(SeekFrom::Start(index_section_start))?;
        file.write_all(&index_buffer)?;

        Ok(())
    }

    /// Will attempt to delete the index block and accompanying data block for a specific index
    ///
    /// Carries the same warning as `updating_indexes` because when a delete occurs indexes must be
    /// updated
    fn delete_block(file: &mut File, metadata: &Metadata, index: u64) -> EkErr<Vec<u8>> {
        let data;
        let mut index_block;
        let mut data_block;

        // take snapshot of the block to return it
        index_block = IndexBlock::try_from_filedb(file, metadata, index)?;
        data_block = DataBlock::try_from(&index_block)?;
        data = data_block.read_block(file)?;
        
        // shift data to the left
        FileDB::shift_data(file, index_block.address()?, index_block.length()? as i64)?;

        // update indexes
        FileDB::update_indexes(
            file,
            metadata,
            index_block.address()?,
            index_block.length()?,
        )?;

        // update index block data
        index_block.reset_index(file)?;

        Ok(data)
    }

    /// Will shift data by a specific amount
    fn shift_data(file: &mut File, address: u64, mut shift_amount: i64) -> EkErr<()> {
        // Assumption: address is the start of the datablock
        // NOTE: potential improvement: partial or whole shift
        //  whole: load the entire segment of the file from the shift location to the end of the
        //  file.
        //  partial: load smaller sections of the file and shift those
        let mut buffer;
        let mut buffer_length; // size of buffer returned by read

        // NOTE: potential improvement: adjustable buffer length for partial method
        buffer = [0_u8; 1024];

        shift_amount = shift_amount.abs();

        // set stage for shift
        file.seek(SeekFrom::Start(address))?;

        loop {
            file.seek(SeekFrom::Current(shift_amount)).unwrap();
            buffer_length = file.read(&mut buffer).unwrap() as i64;
            file.seek(SeekFrom::Current(-(shift_amount + buffer_length)))
                .unwrap();
            file.write_all(&buffer[..buffer_length as usize]).unwrap();

            // end of file
            if buffer_length < 1024 {
                break;
            }
        }

        Ok(())
    }

    /// Obtain the byte position in the file
    #[allow(dead_code)] // necessary in the future
    fn eof_position(&mut self) -> EkErr<u64> {
        let file;
        let eof_position;

        file = self.file_db.take().ok_or(EK::DBFileMissing)?;
        eof_position = file.metadata()?.len();
        self.file_db = Some(file);

        Ok(eof_position)
    }
}

/// Creates an initialized FileDB
///
/// `expected_capacity` must be a power of 2.
fn create_db(path: impl AsRef<Path>, expected_capacity: u64) -> EkErr<()> {
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
        .checked_mul(INDEX_BLOCK_SIZE)
        .ok_or(EK::AddressOverflow)?;

    // [ metadata start, index start, data start ]
    start_section = 4 * 8; // each u64 is 8 bytes (64bit)
    index_section = start_section + 2 * 4; // each u32 is 4 bytes (32bits)
    data_section = index_section + adjusted_capacity; // each u32 is 4 bytes (32bits)
    data_section_end = data_section; // 1 byte to serve as a null byte (dropped in favor of just using zero)

    section_addresses = vec![start_section, index_section, data_section, data_section_end];

    file = File::create(&path)?;

    // write addresses to file
    for address in section_addresses {
        file.write_all(&address.to_be_bytes())?;
    }

    // write metadata to file
    file.write_all(&MJR_VER.to_be_bytes())?;
    file.write_all(&MIN_VER.to_be_bytes())?;

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

    file.write_all(&indexes)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use test_utils::Space;

    #[test]
    fn update_basic() {
        let space;
        let mut fdb;

        space = created_space("update_basic");
        fdb = new_db(&space, 20);

        for i in 0..20 {
            fdb.insert(i, format!("{}", i).as_bytes()).unwrap();
        }

        for i in 0..20 {
            fdb.update(i, (i*2).to_string().as_bytes()).unwrap();
        }

        for i in 0..20 {
            eprintln!("checking {}", i);
            assert_eq!(String::from_utf8_lossy(&fdb.get(i).unwrap()), (i*2).to_string());
        }
    }

    #[test]
    fn delete_basic() {
        let space;
        let mut fdb;

        space = created_space("delete_basic");
        fdb = new_db(&space, 20);

        for i in 0..20 {
            fdb.insert(i, format!("{}", i).as_bytes()).unwrap();
        }

        for i in 0..20 {
            eprintln!("deleting {}", i);
            assert_eq!(String::from_utf8_lossy(&fdb.delete(i).unwrap()), i.to_string());
        }
    }

    #[test]
    fn get_basic() {
        let space;
        let mut fdb;

        space = created_space("get_basic");
        fdb = new_db(&space, 20);

        for i in 0..20 {
            fdb.insert(i, format!("{}", i).as_bytes()).unwrap();
        }

        for i in 0..20 {
            assert_eq!(String::from_utf8_lossy(&fdb.get(i).unwrap()), i.to_string());
        }
    }

    #[test]
    fn insert_basic() {
        let space;
        let mut fdb;

        space = created_space("insert_basic");
        fdb = new_db(&space, 20);

        for i in 0..20 {
            fdb.insert(i, i.to_string().as_bytes()).unwrap();
        }
    }

    #[test]
    fn eof_is_correct() {
        let space;
        let mut fdb;

        space = created_space("eof_is_correct");
        fdb = new_db(&space, 20);

        eprintln!("file length: {}", fdb.eof_position().unwrap());
    }

    #[test]
    fn create_basic() {
        let space;
        let sizes;

        space = created_space("create_basic");

        sizes = vec![256, 1024, 10_240, 500_000];
        
        for size in sizes {
            eprintln!("Creating a filedb with capacity: {}", size);
            new_db(&space, size);
        }
    }

    fn new_db(space: &Space, size: u64) -> FileDB {
        FileDB::create_db(space.test_path().join("filedb"), size).unwrap()
    }

    fn created_space(test_name: impl AsRef<str>) -> Space {
        let space;

        space = new_space(test_name).cleanup_always();
        space.create_space().unwrap();

        space
    }

    fn new_space(test_name: impl AsRef<str>) -> Space {
        Space::new("filedb_db", test_name)
    }
}
