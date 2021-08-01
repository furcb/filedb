use std::error::Error;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::mem;
use std::path::Path;

use crate::metadata::{self, Metadata, Section};

type DynErr<T> = Result<T, Box<dyn Error>>;

pub const MJR_VER: u32 = 0;
pub const MIN_VER: u32 = 0;

pub struct Index {
    start: u64,
    length: u64,
}

pub struct FileDB {
    file_db: Option<File>,
    metadata: Metadata,
}

impl Default for Index {
    fn default() -> Self {
        Self {
            start: 0,
            length: 0,
        }
    }
}

impl FileDB {
    pub fn create_db(path: impl AsRef<Path>, expected_capacity: u64) -> DynErr<Self> {
        // TODO: improve this, better to do just one file open and save the generated data as we go
        // and return it as a FileDB instance

        // path does not exist create a new db
        if !path.as_ref().exists() {
            crate::db::create_db(&path, expected_capacity)?;
        }

        // load the db
        FileDB::load_db(&path)
    }

    pub fn load_db(path: impl AsRef<Path>) -> DynErr<Self> {
        let metadata;
        let file;

        file = File::open(&path)?;
        metadata = metadata::read_metadata(&path)?;

        Ok(Self {
            file_db: Some(file),
            metadata,
        })
    }

    pub fn get(&mut self, index: u64) -> Option<Vec<u8>> {
        // TODO: think of a way to handle case where the db is empty, maybe have a flag to know if
        // its empty, or check via some file length calculation
        // Better alternative: first byte in data section is zero, data starts at first byte
        let data_section_address;
        let value;
        let reader;
        let index_position;
        let mut tmp_file;

        index_position = self.index(index)?;

        // if any section does not exist return None
        data_section_address = self.metadata.section_address(Section::Data)?;

        // take file
        tmp_file = self.file_db.take()?;

        tmp_file.seek(SeekFrom::Start(data_section_address)).ok()?;
        reader = std::io::BufReader::with_capacity(index_position.length as usize, tmp_file);
        value = Vec::from(reader.buffer());

        // replace file
        std::mem::swap(&mut self.file_db, &mut Some(reader.into_inner()));

        Some(value)
    }

    pub fn insert(&mut self, index: u64, data: &[u8]) -> DynErr<()> {
        // inserts an item into an empty position
        todo!();
    }

    pub fn update(&mut self, index: u64, data: &[u8], resize: bool) -> DynErr<()> {
        // TODO: if resize is false and data is not exactly the same size as old data return an
        // error, otherwise replace old data. If resize is true and data is the exact same size
        // don't resize.
        todo!()
    }

    pub fn delete(&mut self, index: u64) -> DynErr<()> {
        // TODO: don't actually the delete the item, just set index default again
        todo!();
    }

    fn shift_data(&mut self, start: u64, amount: i64) -> DynErr<()> {
        todo!();
    }

    fn index(&mut self, index: u64) -> Option<Index> {
        let mut index_section_address;
        let mut index_position;
        let mut buffer;
        let mut tmp_file;

        buffer = [0_u8; 8];
        index_position = Index::default();

        // if index section does not exist return None
        index_section_address = self.metadata.section_address(Section::Indexes)?;
        index_section_address += 128 * index;

        // take file
        tmp_file = self.file_db.take()?;

        // get index position
        tmp_file.seek(SeekFrom::Start(index_section_address)).ok()?;
        tmp_file.read(&mut buffer).ok()?;
        index_position.start = u64::from_be_bytes(buffer);

        tmp_file.read(&mut buffer).ok()?;
        index_position.length = u64::from_be_bytes(buffer);

        // replace file
        std::mem::swap(&mut self.file_db, &mut Some(tmp_file));

        Some(index_position)
    }
}

/// Creates an initialized FileDB
///
/// `expected_capacity` must be a power of 2.
fn create_db(path: impl AsRef<Path>, expected_capacity: u64) -> DynErr<()> {
    let pointers;
    let indexes;
    let mut file;
    //let mut buffer;

    // [ metadata start, index start, data start ]
    pointers = vec![192, 192 + 64, 192 + expected_capacity];

    file = File::create(&path)?;

    // write pointers to file
    for pointer in pointers {
        file.write_all(&pointer.to_be_bytes())?;
    }

    // write metadata to file
    file.write_all(&MJR_VER.to_be_bytes())?;
    file.write_all(&MIN_VER.to_be_bytes())?;

    // write indexes to file
    //
    // make sure that indexes segment size is within u64 bounds
    // 1 block is 128 bit long or 16 u8's
    debug_assert!(expected_capacity * 16 <= u64::MAX);
    // make sure that u64 is smaller or equal to usize for array bounds
    debug_assert!(mem::size_of::<u64>() <= usize::MAX);
    // shadow capacity with segment length in bytes
    let expected_capacity = expected_capacity * 16;
    indexes = vec![0_u8; expected_capacity as usize];

    file.write_all(&indexes)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{db, metadata};
    use std::path::PathBuf;

    #[test]
    fn basic() {
        let path;

        path = PathBuf::from("./test_space/filedb");

        if !path.exists() {
            create_db(&path, 1024).unwrap();
        }

        eprintln!("{:?}", metadata::read_metadata(&path).unwrap());

        assert!(false);
    }
}
