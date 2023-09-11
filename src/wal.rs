//! A Write Ahead Log storage for persisting operations performed to
//! the Database.
//! An entry in the Log has the following structure:
//!
//! +---------------+---------------+-----------------+-...-+--...--+-----------------+
//! | Key Size (8B) | Tombstone(1B) | Value Size (8B) | Key | Value | Timestamp (16B) |
//! +---------------+---------------+-----------------+-...-+--...--+-----------------+
//! Key Size = Length of the Key data
//! Tombstone = If this record was deleted and has a value
//! Value Size = Length of the Value data
//! Key = Key data
//! Value = Value data
//! Timestamp = Timestamp of the operation in microseconds

#![allow(dead_code)]

use std::{
    fs::{remove_file, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{memtable::MemTable, utils::files_with_ext};

pub struct WalEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

/// Write Ahead Log (WAL)
///
/// An append-only file that holds the operations performed on the MemTable.
/// The WAL is intended for recovery of the MemTable when the server is shutdown
pub struct Wal {
    path: PathBuf,
    file: BufWriter<File>,
}

impl IntoIterator for Wal {
    type Item = WalEntry;

    type IntoIter = WalIterator;

    fn into_iter(self) -> Self::IntoIter {
        WalIterator::new(self.path).expect("the iterator to work")
    }
}

impl Wal {
    pub fn new(dir: &Path) -> io::Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(format!("{}.wal", timestamp));
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);
        Ok(Self { path, file })
    }

    pub fn from_path(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let file = BufWriter::new(file);

        Ok(Self {
            path: path.to_owned(),
            file,
        })
    }

    /// Sets a Key-Value pair and the operation is appended to the WAL
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    /// Deletes a Key-Value pair and the operation is appended to the WAL
    ///
    /// This is achieved using Tombstones
    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    /// Flushes the WAL to disk
    ///
    /// This is useful for applying bulk operations and flushing the final result
    /// to disk. Waiting to flush after the bulk operations have been performed will
    /// improve the write performance substantially
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    /// Loads the WAL(s) within a directory, returning a new WAL and the recovered MemTable.
    ///
    /// If multiple WAL exist in a directory, they are merged by file date.
    pub fn load_from_dir(dir: &Path) -> io::Result<(Wal, MemTable)> {
        let mut wal_files = files_with_ext(dir, "wal");
        wal_files.sort();

        let mut new_memtable = MemTable::new();
        let mut new_wal = Wal::new(dir)?;

        for wal_file in wal_files.iter() {
            if let Ok(wal) = Wal::from_path(wal_file) {
                for entry in wal.into_iter() {
                    if entry.deleted {
                        new_memtable.delete(entry.key.as_slice(), entry.timestamp);
                        new_wal.delete(entry.key.as_slice(), entry.timestamp)?;
                    } else {
                        new_memtable.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().expect("a value to exist").as_slice(),
                            entry.timestamp,
                        );
                        new_wal.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().expect("a value to exist").as_slice(),
                            entry.timestamp,
                        )?;
                    }
                }
            }
        }
        new_wal.flush()?;
        wal_files.into_iter().try_for_each(remove_file)?;
        Ok((new_wal, new_memtable))
    }
}

/// An iterator over all the entries in a WAL file
pub struct WalIterator {
    reader: BufReader<File>,
}

impl WalIterator {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WalIterator { reader })
    }
}

impl Iterator for WalIterator {
    type Item = WalEntry;

    fn next(&mut self) -> Option<Self::Item> {
        // Read the key size field
        let mut len_buffer = [0; 8];
        self.reader.read_exact(&mut len_buffer).ok()?;
        let key_len = usize::from_le_bytes(len_buffer);

        // Read the tombstone field
        let mut bool_buffer = [0; 1];
        self.reader.read_exact(&mut bool_buffer).ok()?;
        let deleted = bool_buffer[0] != 0;

        // Read the key and value
        let mut key = vec![0; key_len];
        let mut value = None;

        if deleted {
            self.reader.read_exact(&mut key).ok()?;
        } else {
            self.reader.read_exact(&mut len_buffer).ok()?;
            let value_len = usize::from_le_bytes(len_buffer);
            self.reader.read_exact(&mut key).ok()?;

            let mut value_buf = vec![0; value_len];
            self.reader.read_exact(&mut value_buf).ok()?;
            value = Some(value_buf);
        }

        // Read the timestamp
        let mut timestamp_buffer = [0; 16];
        self.reader.read_exact(&mut timestamp_buffer).ok();
        let timestamp = u128::from_le_bytes(timestamp_buffer);

        Some(WalEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    use std::fs::{create_dir, remove_dir_all};
    use std::fs::{metadata, File, OpenOptions};
    use std::io::BufReader;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn check_entry(
        reader: &mut BufReader<File>,
        key: &[u8],
        value: Option<&[u8]>,
        timestamp: u128,
        deleted: bool,
    ) {
        let mut len_buffer = [0; 8];
        reader.read_exact(&mut len_buffer).unwrap();
        let file_key_len = usize::from_le_bytes(len_buffer);
        assert_eq!(file_key_len, key.len());

        let mut bool_buffer = [0; 1];
        reader.read_exact(&mut bool_buffer).unwrap();
        let file_deleted = bool_buffer[0] != 0;
        assert_eq!(file_deleted, deleted);

        if deleted {
            let mut file_key = vec![0; file_key_len];
            reader.read_exact(&mut file_key).unwrap();
            assert_eq!(file_key, key);
        } else {
            reader.read_exact(&mut len_buffer).unwrap();
            let file_value_len = usize::from_le_bytes(len_buffer);
            assert_eq!(file_value_len, value.unwrap().len());
            let mut file_key = vec![0; file_key_len];
            reader.read_exact(&mut file_key).unwrap();
            assert_eq!(file_key, key);
            let mut file_value = vec![0; file_value_len];
            reader.read_exact(&mut file_value).unwrap();
            assert_eq!(file_value, value.unwrap());
        }

        let mut timestamp_buffer = [0; 16];
        reader.read_exact(&mut timestamp_buffer).unwrap();
        let file_timestamp = u128::from_le_bytes(timestamp_buffer);
        assert_eq!(file_timestamp, timestamp);
    }

    #[test]
    fn test_write_one() {
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&dir).unwrap();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let mut wal = Wal::new(&dir).unwrap();
        wal.set(b"Lime", b"Lime Smoothie", timestamp).unwrap();
        wal.flush().unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        check_entry(
            &mut reader,
            b"Lime",
            Some(b"Lime Smoothie"),
            timestamp,
            false,
        );

        remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_write_many() {
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&dir).unwrap();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];

        let mut wal = Wal::new(&dir).unwrap();

        for e in entries.iter() {
            wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
        }
        wal.flush().unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for e in entries.iter() {
            check_entry(&mut reader, e.0, e.1, timestamp, false);
        }

        remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_write_delete() {
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&dir).unwrap();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];

        let mut wal = Wal::new(&dir).unwrap();

        for e in entries.iter() {
            wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
        }
        for e in entries.iter() {
            wal.delete(e.0, timestamp).unwrap();
        }

        wal.flush().unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for e in entries.iter() {
            check_entry(&mut reader, e.0, e.1, timestamp, false);
        }
        for e in entries.iter() {
            check_entry(&mut reader, e.0, None, timestamp, true);
        }

        remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_read_wal_none() {
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&dir).unwrap();

        let (new_wal, new_mem_table) = Wal::load_from_dir(&dir).unwrap();
        assert_eq!(new_mem_table.len(), 0);

        let m = metadata(new_wal.path).unwrap();
        assert_eq!(m.len(), 0);

        remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_read_wal_one() {
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&dir).unwrap();

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];

        let mut wal = Wal::new(&dir).unwrap();

        for (i, e) in entries.iter().enumerate() {
            wal.set(e.0, e.1.unwrap(), i as u128).unwrap();
        }
        wal.flush().unwrap();

        let (new_wal, new_mem_table) = Wal::load_from_dir(&dir).unwrap();

        let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for (i, e) in entries.iter().enumerate() {
            check_entry(&mut reader, e.0, e.1, i as u128, false);

            let mem_e = new_mem_table.get(e.0).unwrap();
            assert_eq!(mem_e.key, e.0);
            assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
            assert_eq!(mem_e.timestamp, i as u128);
        }

        remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_read_wal_multiple() {
        let mut rng = rand::thread_rng();
        let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&dir).unwrap();

        let entries_1: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Apple", Some(b"Apple Smoothie")),
            (b"Lime", Some(b"Lime Smoothie")),
            (b"Orange", Some(b"Orange Smoothie")),
        ];
        let mut wal_1 = Wal::new(&dir).unwrap();
        for (i, e) in entries_1.iter().enumerate() {
            wal_1.set(e.0, e.1.unwrap(), i as u128).unwrap();
        }
        wal_1.flush().unwrap();

        let entries_2: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Strawberry", Some(b"Strawberry Smoothie")),
            (b"Blueberry", Some(b"Blueberry Smoothie")),
            (b"Orange", Some(b"Orange Milkshake")),
        ];
        let mut wal_2 = Wal::new(&dir).unwrap();
        for (i, e) in entries_2.iter().enumerate() {
            wal_2.set(e.0, e.1.unwrap(), (i + 3) as u128).unwrap();
        }
        wal_2.flush().unwrap();

        let (new_wal, new_mem_table) = Wal::load_from_dir(&dir).unwrap();

        let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for (i, e) in entries_1.iter().enumerate() {
            check_entry(&mut reader, e.0, e.1, i as u128, false);

            let mem_e = new_mem_table.get(e.0).unwrap();
            if i != 2 {
                assert_eq!(mem_e.key, e.0);
                assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
                assert_eq!(mem_e.timestamp, i as u128);
            } else {
                assert_eq!(mem_e.key, e.0);
                assert_ne!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
                assert_ne!(mem_e.timestamp, i as u128);
            }
        }
        for (i, e) in entries_2.iter().enumerate() {
            check_entry(&mut reader, e.0, e.1, (i + 3) as u128, false);

            let mem_e = new_mem_table.get(e.0).unwrap();
            assert_eq!(mem_e.key, e.0);
            assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
            assert_eq!(mem_e.timestamp, (i + 3) as u128);
        }

        remove_dir_all(&dir).unwrap();
    }
}
