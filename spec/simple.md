
# FileDB Specification (Simple)

A first attempt at the specification.

## What FileDB is

FileDB is a very basic database implementation that uses a file or files to store data in the same
fashion as a hash map. The core components of the file itself are a set of pointers at the beginning
of the file that point to the subsequent sections. The sections include the metadata, index, and
content sections.

## Sections

All byte offsets are relative to the start of the section with the exception of the Pointers section
as those are offset from the start of the file.

### Pointers

Three u64 integers that point to the start of each subsequent section. No assumption should be made
as to whether the pointer section is only 172 bytes as it could increase in the future. Using the
pointer locations is the recommended behaviour.

### Metadata

For now only contains:

- u8: Major version
- u8: Minor version

### Indexes

A sequential list of indexes that contain two integers that will act as pointers to the start and
end positions of the data. Both integers are u64, the start position is inclusive and the end
position is exclusive as it will be used to store a new line character to make this format more
editor friendly.

- u64: start of data (offset)
- u64: end of data (offset) / length of the segment [based on how which is easier to implement]

### Data

The segment of the file that contains the contents of the map. (not sure whether to make it line
delimited)

## Layout

- 192 bytes (first three pointers)
- first u64 pointer is the end of the first section

## Implementation Details and Pitfalls

- All values will use Big-Endian.
- First byte in data section is an anchor byte, any index that points to zero is empty.
- Update data block, delete and insert.

- Usage of u64: File seeking in Rust only accepts u64 because the Files in rust are wrapper around
	the IO implementation in `libc`, and that uses 64bit numbers.

- Case: partial update, eof now at end of file but not end of data section due to partial data
 - Simple: Add pointer to pointer section to keep track of end of data section. May be the better
	 option since the arithmetic and access seems simpler.
 - Medium: Add length to `hole` of data section that all indexes point to, hole would become 8 bytes
	 long rather then 1 byte long
- Case: update an old value, and the new value is either shorter or longer than the old value which
	cause a "hole" or missing space.
 - Simple solution: shift the data to the right by the difference
 - Complex solution: store the data in blocks similar to an filesystem with an additional section
	 that keeps track of empty blocks. Indexes section would need to be a list of pointers to block
	 pointer section that contains tuples of block pointers and next pointer pointers.
