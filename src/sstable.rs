//! An implementation of SSTable (Static Sorted Table) from RocksDB
//!
//! We use the "Plain Table Format" (see
//! https://github.com/facebook/rocksdb/wiki/PlainTable-Format)
//! with Plain Key Encoding
//!
//! This format has the following limitations
//!
//!     - File size may not be greater than 2^31 - 1 bytes
//!     - Data compression / Delta encoding is not supported, which may result in
//!     bigger file sizes compared to the block-based table
//!     - Backward scan not supported
//!     - Non-prefix based `seek` not supported
//!     - Table loading is slower, since indexes are built on the fly by 2-pass table scanning
//!     - Only support mmap mode

#![allow(dead_code)]

// NOTE(alvaro): This is a personal take of the SSTable, since the blog series
// does not have a post with the implementation of this component as of today
// (2023-09-12)

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

use memmap2::Mmap;

use crate::memtable::MemTable;

const PREFIX_LENGTH: usize = 10;
const SEQUENCE_ID: u64 = 42;

/// Type of row this is
///
/// Simplification of https://github.com/facebook/rocksdb/blob/8fc78a3a9e1d24ba55731b70c0c25cef0765dbc8/db/dbformat.h#L39
#[derive(Copy, Clone)]
#[repr(u8)]
enum RowType {
    Deletion = 0,
    Value = 1,
    Merge = 2,
}

impl From<u8> for RowType {
    fn from(value: u8) -> Self {
        match value {
            0 => RowType::Deletion,
            1 => RowType::Value,
            2 => RowType::Merge,
            _ => panic!("invalid u8 value for RowType: {}", value),
        }
    }
}

impl From<RowType> for u8 {
    fn from(val: RowType) -> Self {
        match val {
            RowType::Deletion => 0,
            RowType::Value => 1,
            RowType::Merge => 2,
        }
    }
}

/// An row in the SSTable
struct SSTableRow {
    pub key: Vec<u8>,
    pub row_type: RowType,
    pub sequence_id: u64,
    pub value: Vec<u8>,
}

struct PlainSSTable {
    /// A reference to the file that this SSTable is backed by
    file: File,

    /// The file data `mmap`-ed into memory
    data: Mmap,

    /// The index to find the offset in the file where to find some key
    ///
    /// This is implemented as a HashMap that maps key prefixes to an offset in
    /// the `data` (a file mmap-ed into memory).
    ///
    /// This is a slight simplification over the actual format of RocksDB.
    /// The actual implementation contains optimizations for the hashing algorithm
    /// and bucket data structure for handling collisions.
    index: HashMap<Vec<u8>, usize>,
}

// TODO(alvaro): Make the prefix length configurable through a builder
impl PlainSSTable {
    fn new(path: PathBuf) -> io::Result<Self> {
        let file = File::open(path)?;
        let data = unsafe { Mmap::map(&file)? };

        Ok(Self {
            file,
            data,
            index: HashMap::new(),
        })
    }

    /// Retrieve a Key-Value pair
    pub fn get(&self, key: &[u8]) -> Option<SSTableRow> {
        // TODO(alvaro): When we have a bloom filter, this is the place to check
        // it exists
        // Compute the key prefix
        let prefix = self.key_prefix(key);

        // Check the index to find the offset
        let offset = self.index.get(prefix)?;

        // Perform a linear search through the data starting at offset
        self.linear_search(key, *offset)
    }

    /// Compute the key prefix for the given key
    fn key_prefix<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        &key[..PREFIX_LENGTH]
    }

    /// Perform a linear search through the memory to look for an row with a
    /// matching key
    fn linear_search(&self, key: &[u8], offset: usize) -> Option<SSTableRow> {
        let mut offset = offset;

        while offset < self.data.len() {
            // Read the key at the offset
            let (row_key, row_type, sequence_id, row_value, next_incr) =
                read_key_value(&self.data[offset..])?;

            // Check if the key matches
            if row_key == key {
                // We found the row
                let row = SSTableRow {
                    key: row_key.into(),
                    row_type,
                    sequence_id,
                    value: row_value.into(),
                };
                return Some(row);
            } else {
                if self.key_prefix(key) != self.key_prefix(row_key) {
                    break;
                }
                offset += next_incr;
            }
        }
        None
    }
}

/// Read a key at the beginning of data
/// If successful, will return the number the offset from the beginning of
/// the received data where the value next to the key should be
fn read_key(data: &[u8]) -> Option<(&[u8], RowType, u64, usize)> {
    let key_len = u32::from_le_bytes(data[..4].try_into().unwrap());
    let key_offset = 4 + key_len as usize;
    let key = &data[4..key_offset];

    // Read the trailer
    let row_mask = 0xFF << 24;

    let internal_bytes = u64::from_le_bytes(data[key_offset..key_offset + 8].try_into().unwrap());
    let row_type = (((internal_bytes & row_mask) >> 24) as u8).into();
    let sequence_id = internal_bytes & !row_mask;
    let offset = key_offset + 8;

    Some((key, row_type, sequence_id, offset))
}

/// Read a value at the beginning of data
fn read_value(data: &[u8]) -> Option<(&[u8], usize)> {
    let value_len = u32::from_le_bytes(
        data[..4]
            .try_into()
            .expect("the slice to be the right size"),
    );
    let offset = 4 + value_len as usize;
    let key = &data[4..offset];
    Some((key, offset))
}

type KeyValueParts<'a> = (&'a [u8], RowType, u64, &'a [u8], usize);

/// Read a Key-Value pair from the SSTable file
fn read_key_value(data: &[u8]) -> Option<KeyValueParts> {
    let (key, row_type, sequence_id, value_off) = read_key(data)?;
    let (value, next_key_off) = read_value(&data[value_off..])?;
    Some((key, row_type, sequence_id, value, next_key_off))
}

/// Write an SSTable to disk and return an SSTable reference that points to it
///
/// ### File Format
/// #### Basic
///     <beginning_of_file>
///       [data row1]
///       [data row1]
///       [data row1]
///       ...
///       [data rowN]
///       [Property Block]
///       [Footer]                               (fixed size; starts at file_size - sizeof(Footer))
///     <end_of_file>
///
/// A row has the following format
///
/// <beginning of a row>
///     encoded key
///     length of value: varint32
///     value bytes
/// <end of a row>
///
/// A key with the plain format (variable length) has the following structure:
///
/// [length of key: varint32] + user key + internal bytes
///
/// With internal bytes being
///
/// +----------- ...... -------------+----+---+---+---+---+---+---+---+
/// |       user key                 |type|   sequence ID (7 bytes)   |
/// +----------- ..... --------------+----+---+---+---+---+---+---+---+
///
fn write(memtable: MemTable, path: &Path) -> io::Result<PathBuf> {
    let file = OpenOptions::new().write(true).open(path)?;

    let mut bufwriter = BufWriter::new(file);

    // Write the rows
    for entry in memtable.entries.iter() {
        // Write the key
        let key_len = entry.key.len() as u32;
        bufwriter.write_all(key_len.to_le_bytes().as_slice())?;
        bufwriter.write_all(entry.key.as_slice())?;

        let row_type = if entry.deleted {
            RowType::Deletion
        } else {
            RowType::Value
        };

        // NOTE(alvaro): For now we don't use any sequence id
        let sequence_number = SEQUENCE_ID;

        // The sequence number uses only 56 bits
        let seq_mask = !(0xFF << 24);
        let internal_bytes = ((std::convert::Into::<u8>::into(row_type) as u64) << 24)
            | (sequence_number & seq_mask);
        bufwriter.write_all(internal_bytes.to_le_bytes().as_slice())?;

        // Write the value
        if let RowType::Deletion = row_type {
            bufwriter.write_all(0u32.to_le_bytes().as_slice())?;
        } else {
            let value = entry.value.as_ref().unwrap();
            let value_len = value.len() as u32;
            bufwriter.write_all(value_len.to_le_bytes().as_slice())?;
        }
    }

    // Write the property block
    // Write the footer
    todo!()
}
