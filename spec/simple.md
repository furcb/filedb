
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

## Implementation Pitfalls

- Case: update an old value, and the new value is either shorter or longer than the old value which
	cause a "hole" or missing space.
 - Simple solution: shift the data to the right by the difference
 - Complex solution: store the data in blocks similar to an filesystem with an additional section
	 that keeps track of empty blocks. Indexes section would need to be a list of pointers to block
	 pointer section that contains tuples of block pointers and next pointer pointers.
