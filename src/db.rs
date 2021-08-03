use std::convert::{TryFrom, TryInto};
use std::fs::{File, OpenOptions};
use std::io::{self, prelude::*, SeekFrom};
use std::mem;
use std::path::Path;

use crate::metadata::{Metadata, Section};

type EK = ErrorKind;
type EkErr<T> = Result<T, EK>;

/// Major version: Incremented when changes are made to the specification or implementation that
/// break backwards compatibility. Baked into the metadata of the file.
pub const MJR_VER: u32 = 0;
/// Minor version: Incremented when changes are introduced that do not break backwards
/// compatibility. Baked into the metadata of the file.
pub const MIN_VER: u32 = 1;

/// Error that can occur while mutating the database
#[derive(Debug)]
pub enum ErrorKind {
    /// Simple wrapper for `std::io::Error`s.
    IO(io::Error),
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
pub struct DataBlock<'a> {
    address: u64,
    length: u64,
    data: Option<&'a [u8]>,
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

    /// Return `IndexBlock` data based on index in Indexes
    ///
    /// If look up fails for any reason assume file had an issue and is no longer accessible
    fn from_filedb(file: &mut File, metadata: &Metadata, index: u64) -> EkErr<Self> {
        let index_section_address;
        let index_address;
        let mut index_block;

        // if index section does not exist return None
        index_section_address = metadata
            .section_address(Section::Indexes)?;
        index_address = index_section_address + (16 * index);

        index_block = IndexBlock::new(index_address);
        IndexBlock::read_from_file(&mut index_block, file)?;

        eprintln!("{:?}", index_block);

        if index_block.address()? == 0 {
            Err(EK::IndexEmpty)
        } else {
            Ok(index_block)
        }
    }

    /// Create an `IndexBlock` from `Metadata` information, `index` is necessary so the
    /// `index_address` can be calculated. `data` for the length of length of the `DataBlock`.
    /// The data block address can be automatically calculated.
    fn try_from_metadata(metadata: &Metadata, index: u64, data: &[u8]) -> EkErr<Self> {
        let index_address;
        let address;
        let length;

        index_address = metadata
            .section_address(Section::Indexes)?;
        address = metadata
            .section_address(Section::DataEnd)?;
        length = data
            .len()
            .try_into()
            .map_err(|_| EK::AddressConversionFailed)?;

        Ok(Self {
            index_address: index_address + (index * 16),
            address: Some(address),
            length: Some(length),
        })
    }

    /// Get the data block address, may be un-initialized or an error could occur on retrieval
    pub fn address(&self) -> EkErr<u64> {
        self.address.ok_or(EK::BlockDataNotProvided)
    }

    /// Get the data block length, may be un-initialized or an error could occur on retrieval
    pub fn length(&self) -> EkErr<u64> {
        self.length.ok_or(EK::BlockDataNotProvided)
    }

    /// Get the data block address or return a zero
    pub fn address_lossy(&self) -> u64 {
        self.address.unwrap_or(0)
    }

    /// Get the data block length or return a zero
    pub fn length_lossy(&self) -> u64 {
        self.length.unwrap_or(0)
    }

    /// Write the `IndexBlock` information to the file
    pub fn write_to_file(&self, file: &mut File) -> EkErr<()> {
        let address;
        let length;
        let mut buffer;

        address = self.address()?;
        length = self.length()?;

        // write new block information
        file.seek(SeekFrom::Start(self.index_address))
            .map_err(EK::IO)?;
        buffer = address.to_be_bytes();
        file.write_all(&buffer).map_err(EK::IO)?;

        buffer = length.to_be_bytes();
        file.write_all(&buffer).map_err(EK::IO)?;

        Ok(())
    }

    /// Read and parse the `IndexBlock` in the file
    pub fn read_from_file(&mut self, file: &mut File) -> EkErr<()> {
        let address;
        let length;
        let mut buffer;

        buffer = [0_u8; 8];

        // get index position
        file.seek(SeekFrom::Start(self.index_address))
            .map_err(EK::IO)?;
        file.read_exact(&mut buffer).map_err(EK::IO)?;
        address = u64::from_be_bytes(buffer);

        file.read_exact(&mut buffer).map_err(EK::IO)?;
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

    /// Assign the data in builder fashion
    pub fn with_data(mut self, data: &'a [u8]) -> Self {
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
        file.seek(SeekFrom::Start(self.address)).map_err(EK::IO)?;
        file.write_all(buffer).map_err(EK::IO)?;

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

        file.seek(SeekFrom::Start(self.address)).map_err(EK::IO)?;
        file.read_exact(&mut buffer).map_err(EK::IO)?;

        Ok(buffer)
    }
}

impl<'a> TryFrom<&IndexBlock> for DataBlock<'a> {
    type Error = EK;

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
            .open(&path)
            .map_err(EK::IO)?;

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
        index_block = IndexBlock::from_filedb(&mut file, &self.metadata, index)?;
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
        data_section_end_address = self
            .metadata
            .section_address(Section::DataEnd)?;

        // at this point we should have a valid state were we just need to catalog the position of
        // new data and write the new data

        file = self.file_db.take().ok_or(EK::DBFileMissing)?;

        // if index is empty we can insert otherwise propagate the error or say index already
        // exists
        match IndexBlock::from_filedb(&mut file, &self.metadata, index) {
            Err(EK::IndexEmpty) => (),
            Err(e) => return Err(e),
            Ok(_) => return Err(EK::IndexAlreadyExists),
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
    pub fn update(&mut self, index: u64, data: &[u8], _resize: bool) -> EkErr<()> {
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
        let mut file;

        data_section_end_address = self.metadata.section_address(Section::DataEnd)?;

        file = self.file_db.take().ok_or(EK::DBFileMissing)?;

        // delete block
        data = FileDB::delete_block(&mut file, &self.metadata, index)?;

        // update section address by deleted data length
        self.metadata.update_section_address(
            &mut file,
            Section::DataEnd,
            data_section_end_address - data.len() as u64,
        )?;

        // give back file
        self.file_db = Some(file);

        Ok(data)
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
    ) -> EkErr<()>
    {
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

        file.seek(SeekFrom::Start(index_section_start)).map_err(EK::IO)?;
        file.read_exact(&mut index_buffer).map_err(EK::IO)?;

        chunked_buffer = index_buffer.chunks_mut(8).peekable();

        loop {
            let length_chunk;
            let address_chunk;
            let mut tmp_slice;
            let mut address;

            address_chunk = chunked_buffer.next().ok_or(EK::SectionCorrupted)?;
            length_chunk = chunked_buffer.next().ok_or(EK::SectionCorrupted)?;

            tmp_slice = [0_u8; 8];

            // if chunk is not 16, then something went wrong and addresses are of which means the
            // data is not accessible
            if address_chunk.len() < 8 || length_chunk.len() < 8 {
                return Err(EK::SectionCorrupted);
            }

            // parse chunks
            tmp_slice.copy_from_slice(&address_chunk);
            address = u64::from_be_bytes(tmp_slice);

            // check values and adjust values
            if address > cutoff {
                // adjust value
                address += shift_amount;

                // update value
                address_chunk.copy_from_slice(&address.to_be_bytes());
            }

            if chunked_buffer.peek().is_none() {
                break;
            }
        }

        file.seek(SeekFrom::Start(index_section_start)).map_err(EK::IO)?;
        file.write_all(&dbg!(index_buffer)).map_err(EK::IO)?;

        Ok(())
    }

    /// Will attempt to delete a data block for a specific index
    ///
    /// Carries the same warning as `updating_indexes` because when a delete occurs indexes must be
    /// updated
    fn delete_block(file: &mut File, metadata: &Metadata, index: u64) -> EkErr<Vec<u8>> {
        let data;
        let mut index_block;
        let mut data_block;

        // take snapshot of the block to return it
        index_block = IndexBlock::from_filedb(file, metadata, index)?;
        data_block = DataBlock::try_from(&index_block)?;
        data = data_block.read_block(file)?;

        // delete the data block
        FileDB::shift_data(file, index_block.address()?, index_block.length()? as i64)?;
        
        // update index block data
        index_block = index_block
            .with_address(0)
            .with_length(0);
        index_block.write_to_file(file)?;

        // update indexes
        FileDB::update_indexes(file, metadata, index_block.address()?, index_block.length()?)?;

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
        file.seek(SeekFrom::Start(address)).map_err(EK::IO)?;

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

        match file.metadata() {
            Ok(m) => eof_position = m.len(),
            Err(e) => return Err(EK::IO(e)),
        }

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
        .checked_mul(16)
        .ok_or(EK::AddressOverflow)?;

    // [ metadata start, index start, data start ]
    start_section = 4 * 8; // each u64 is 8 bytes (64bit)
    index_section = start_section + 2 * 4; // each u32 is 4 bytes (32bits)
    data_section = index_section + adjusted_capacity; // each u32 is 4 bytes (32bits)
    data_section_end = data_section; // 1 byte to serve as a null byte (dropped in favor of just using zero)

    section_addresses = vec![start_section, index_section, data_section, data_section_end];

    file = File::create(&path).map_err(EK::IO)?;

    // write addresses to file
    for address in section_addresses {
        file.write_all(&address.to_be_bytes()).map_err(EK::IO)?;
    }

    // write metadata to file
    file.write_all(&MJR_VER.to_be_bytes()).map_err(EK::IO)?;
    file.write_all(&MIN_VER.to_be_bytes()).map_err(EK::IO)?;

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

    file.write_all(&indexes).map_err(EK::IO)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::metadata;
    use std::path::PathBuf;

    use super::*;

    #[test]
    #[ignore]
    fn update_basic() {
        let mut fdb;
        let number;

        fdb = load_db();

        number = fdb.delete(15).unwrap();
        fdb.update(16, format!("21").as_bytes(), false).unwrap();

        eprintln!("{}", String::from_utf8_lossy(&number));
    }

    #[test]
    #[ignore]
    fn delete_basic() {
        let mut fdb;
        let number;

        fdb = load_db();

        number = fdb.delete(15).unwrap();

        eprintln!("{}", String::from_utf8_lossy(&number));
    }

    #[test]
    fn get_basic() {
        let mut fdb;

        fdb = load_db();

        eprintln!("{}", String::from_utf8_lossy(&fdb.get(0).unwrap()));
        eprintln!("{}", String::from_utf8_lossy(&fdb.get(1).unwrap()));
        assert!(false)
    }

    #[test]
    #[ignore]
    fn insert_basic() {
        let mut fdb;

        fdb = load_db();

        //fdb.insert(0, "hello world how are you".as_bytes()).unwrap();
        //fdb.insert(1, "the sky is blue and the clouds gray".as_bytes())
        //.unwrap();

        for i in 1..20 {
            fdb.insert(i, format!("{}", i).as_bytes()).unwrap();
        }
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
    }

    fn load_db() -> FileDB {
        let path;

        path = PathBuf::from("./test_space/filedb");

        FileDB::load_db(&path).unwrap()
    }
}
