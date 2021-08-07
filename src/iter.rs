// iterator capabilities
//
// - iterate over non-empty keys
// - iterate over non-empty values
//
// should it consume the FileDB instance
//
// - why it's important:
//  - file could be read as new data is being written to (this is an assumption, io may be
//  blocking)
//
// - yes, it ensures that the user must actively opt into multiple FileDB instances. This will rely
// on the user to write correct code
//
// - no, even if io can cause races that would only occur in multi-threaded code. We can make the
// assumption that in no-threaded code the user will only ever be able to read or write to the file
// at one time. Using this assumption we can make the implementation for next read from the file
// once for each block to return. E.g. we take index 1 and value 1 only, we know that at the time
// that the file was read the addresses and values are valid.

use std::convert::TryFrom;
use std::fs::File;

use crate::db::{DataBlock, FileDB, IndexBlock};
use crate::error::Error;
use crate::metadata::Section;

type IterResult<T> = Result<T, Error>;

#[derive(Debug)]
struct IterInternal {
    file: File,
    capacity: u64,
    index: u64,
    index_section_address: u64,
}

#[derive(Debug)]
pub struct IntoIndexIterator {
    internal: IterInternal,
}

#[derive(Debug)]
pub struct IntoDataIterator {
    internal: IterInternal,
}

#[derive(Debug)]
pub struct IntoIndexedIterator {
    internal: IterInternal,
}

impl IterInternal {
    fn has_next(&self) -> bool {
        self.index < self.capacity
    }

    fn next_index(&mut self) -> IterResult<u64> {
        let _index_block;
        let current_index;

        // increment counter even if error occurs
        current_index = self.index;
        self.index += 1;

        // attempt to read from indexes, if successful it is not empty so we can return index
        _index_block = IndexBlock::try_from_address(
            &mut self.file,
            self.index_section_address,
            current_index,
        )?;

        Ok(current_index)
    }

    fn next_data(&mut self) -> IterResult<Vec<u8>> {
        let current_index;
        let index_block;
        let mut data_block;
        let data;

        // increment counter even if error occurs
        current_index = self.index;
        self.index += 1;

        // get current index_block
        index_block = IndexBlock::try_from_address(
            &mut self.file,
            self.index_section_address,
            current_index,
        )?;

        // attempt to read from data block. if it succeeds, there is data so we can
        // return an owned vec<u8>
        data_block = DataBlock::try_from(&index_block)?;
        data = data_block.read_block(&mut self.file)?;

        Ok(data)
    }
}

impl Iterator for IntoIndexIterator {
    type Item = IterResult<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.internal.has_next() {
            Some(self.internal.next_index())
        } else {
            None
        }
    }
}

impl Iterator for IntoDataIterator {
    type Item = IterResult<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.internal.has_next() {
            Some(self.internal.next_data())
        } else {
            None
        }
    }
}

impl Iterator for IntoIndexedIterator {
    type Item = IterResult<(u64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let current_index;

        // get index before getting next
        current_index = self.internal.index;

        if self.internal.has_next() {
            match self.internal.next_data() {
                Ok(v) => Some(Ok((current_index, v))),
                Err(e) => Some(Err(e)),
            }
        } else {
            None
        }
    }
}

impl TryFrom<FileDB> for IterInternal {
    type Error = Error;

    fn try_from(mut value: FileDB) -> Result<Self, Self::Error> {
        let index_section_address;
        let capacity;
        let file;

        // must calculate values before taking ownership of the File
        capacity = value.metadata().capacity()?;
        index_section_address = value.metadata().section_address(Section::Indexes)?;
        file = value.take_file()?;

        Ok(Self {
            file,
            index: 0,
            index_section_address,
            capacity,
        })
    }
}

impl TryFrom<FileDB> for IntoIndexedIterator {
    type Error = Error;

    fn try_from(value: FileDB) -> Result<Self, Self::Error> {
        Ok(IterInternal::try_from(value)?.into())
    }
}

impl TryFrom<FileDB> for IntoDataIterator {
    type Error = Error;

    fn try_from(value: FileDB) -> Result<Self, Self::Error> {
        Ok(IterInternal::try_from(value)?.into())
    }
}

impl TryFrom<FileDB> for IntoIndexIterator {
    type Error = Error;

    fn try_from(value: FileDB) -> Result<Self, Self::Error> {
        Ok(IterInternal::try_from(value)?.into())
    }
}

impl From<IterInternal> for IntoIndexedIterator {
    fn from(value: IterInternal) -> Self {
        Self { internal: value }
    }
}

impl From<IterInternal> for IntoDataIterator {
    fn from(value: IterInternal) -> Self {
        Self { internal: value }
    }
}

impl From<IterInternal> for IntoIndexIterator {
    fn from(value: IterInternal) -> Self {
        Self { internal: value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;

    use crate::db::FileDB;
    use crate::error::Error;

    use test_utils::Space;

    #[test]
    fn into_iter_indexes() {
        let space;
        let mut iter;
        let filedb;

        space = new_space("into_iter_indexes").cleanup_always();
        //space = new_space("into_iter_indexes");
        space.create_space().unwrap();

        filedb = new_filedb(space.test_path()).unwrap();
        iter = IntoIndexIterator::try_from(filedb).unwrap();

        for i in 0..5 {
            if i % 2 == 0 {
                assert_eq!(iter.next().unwrap().unwrap(), i);
            } else {
                let _ = iter.next();
            }
        }

        assert!(iter.next().is_none());
    }

    #[test]
    fn into_iter_data() {
        let space;
        let mut iter;
        let filedb;

        space = new_space("into_iter_data").cleanup_always();
        space.create_space().unwrap();

        filedb = new_filedb(space.test_path()).unwrap();
        iter = IntoDataIterator::try_from(filedb).unwrap();

        for i in 0..5 {
            if i % 2 == 0 {
                assert_eq!(iter.next().unwrap().unwrap(), format!("{}", i).as_bytes());
            } else {
                let _ = iter.next();
            }
        }

        assert!(iter.next().is_none());
    }

    #[test]
    fn into_iter_indexed() {
        let space;
        let mut iter;
        let filedb;

        space = new_space("into_iter_indexed").cleanup_always();
        space.create_space().unwrap();

        filedb = new_filedb(space.test_path()).unwrap();
        iter = IntoIndexedIterator::try_from(filedb).unwrap();

        for i in 0..5 {
            if i % 2 == 0 {
                assert_eq!(
                    iter.next().unwrap().unwrap(),
                    (i, Vec::from(format!("{}", i).as_bytes()))
                );
            } else {
                let _ = iter.next();
            }
        }

        assert!(iter.next().is_none());
    }

    fn new_space(test_name: impl AsRef<str>) -> Space {
        Space::new("filedb_iter", &test_name)
    }

    fn new_filedb(test_space: impl AsRef<Path>) -> Result<FileDB, Error> {
        let path;
        let mut filedb;

        path = dbg!(test_space.as_ref().join("filedb"));

        filedb = FileDB::create_db(path, 5)?;

        for i in 0..5 {
            if i % 2 == 0 {
                filedb.insert(i, format!("{}", i).as_bytes())?;
            }
        }

        Ok(filedb)
    }
}
