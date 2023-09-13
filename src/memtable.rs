#![allow(dead_code)]

/// MemTable holds a sorted list of the latest written records.
///
/// Writes are duplicated to the WAL for recovery of the MemTable in the event
/// of a restart.
///
/// MemTables have a max capacity and when that is reached, we flush them to
/// disk as a Table (SSTable)
///
/// Entries are stored in a Vector instead of a HashMap to support Scans
pub struct MemTable {
    pub entries: Vec<MemTableEntry>,
    pub size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            size: 0,
        }
    }

    /// Get a Key-Value pair from the MemTable
    ///
    /// If no record with the same key exists, return None
    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        self.get_index(key)
            .ok()
            .and_then(|idx| self.entries.get(idx))
    }

    /// Sets a Key-Value pair in the MemTable
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        // TODO(alvaro): Can we pass ownership of the key and value here instead
        // of copying
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
            deleted: false,
        };

        match self.get_index(key) {
            Ok(idx) => {
                // TODO(alvaro): We should be to use some kind of add operation
                // to represent this logic

                // If a value already existed on the deleted record, add the
                // difference of the new and old value to the MemTable's size
                if let Some(v) = self.entries[idx].value.as_ref() {
                    if value.len() < v.len() {
                        self.size -= v.len() - value.len();
                    } else {
                        self.size += value.len() - v.len();
                    }
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                // TODO(alvaro): I am sure there's some built in way to use the
                // REAL sizes of the fields in here, instead of hardcoding (considering
                // alignment, padding, null pointer optimization, etc.)

                // Increase the size of the MemTable by the Key size, the Value size, Timestamp
                // size (16 bytes) and Tombstone size (1 byte)
                self.size += key.len() + value.len() + 16 + 1;
                self.entries.insert(idx, entry);
            }
        }
    }

    /// Deletes a Key-Value pair in the MemTable
    ///
    /// This is achieved by inserting a Tombstone, which will be checked and
    /// actually cleaned by the compaction process
    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        // TODO(alvaro): Can we pass ownership of the key and value here instead
        // of copying
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: None,
            timestamp,
            deleted: true,
        };

        match self.get_index(key) {
            Ok(idx) => {
                // If a Value existed on the deleted record, then subtract the
                // size of the Value from the MemTable
                if let Some(value) = self.entries[idx].value.as_ref() {
                    self.size -= value.len();
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                // Increase the size of the MemTable by the Key size, Timestamp
                // size (16 bytes) and Tombstone size (1 byte)
                self.size += key.len() + 16 + 1;
                self.entries.insert(idx, entry);
            }
        }
    }

    /// Performs Binary Search to find a record in the MemTable
    ///
    /// If the record is found `[Result::Ok]` is returned with the index of the
    /// record. If the record is not found, then `[Result::Err]` is returned with
    /// the index to insert the record at while maintaining sorted order.
    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&key, |e| e.key.as_slice())
    }

    /// Return the number of entries in the MemTable
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// TODO(alvaro): Explore if we can skip the `deleted` flag and instead use
/// the fact that only Tombstones have a `None` in the value. Would this save
/// disk space using null pointer optimization? Would it be faster?
/// An entry in the MemTable
pub struct MemTableEntry {
    pub key: Vec<u8>,
    /// Value of the entry, will be `None` when used as Tombstone
    pub value: Option<Vec<u8>>,
    /// Time this write occurred in microseconds, used to order writes when
    /// cleaning old data in SSTables
    pub timestamp: u128,
    /// Tombstone mark
    pub deleted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_table_put_start() {
        let mut table = MemTable::new();
        table.set(b"Lime", b"Lime Smoothie", 0); // 17 + 16 + 1
        table.set(b"Orange", b"Orange Smoothie", 10); // 21 + 16 + 1

        table.set(b"Apple", b"Apple Smoothie", 20); // 19 + 16 + 1

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 20);
        assert!(!table.entries[0].deleted);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp, 0);
        assert!(!table.entries[1].deleted);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 10);
        assert!(!table.entries[2].deleted);

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_middle() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Orange", b"Orange Smoothie", 10);

        table.set(b"Lime", b"Lime Smoothie", 20);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 0);
        assert!(!table.entries[0].deleted);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp, 20);
        assert!(!table.entries[1].deleted);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 10);
        assert!(!table.entries[2].deleted);

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_end() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);

        table.set(b"Orange", b"Orange Smoothie", 20);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 0);
        assert!(!table.entries[0].deleted);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp, 10);
        assert!(!table.entries[1].deleted);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 20);
        assert!(!table.entries[2].deleted);

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_overwrite() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);
        table.set(b"Orange", b"Orange Smoothie", 20);

        table.set(b"Lime", b"A sour fruit", 30);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 0);
        assert!(!table.entries[0].deleted);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"A sour fruit");
        assert_eq!(table.entries[1].timestamp, 30);
        assert!(!table.entries[1].deleted);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 20);
        assert!(!table.entries[2].deleted);

        assert_eq!(table.size, 107);
    }

    #[test]
    fn test_mem_table_get_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);
        table.set(b"Orange", b"Orange Smoothie", 20);

        let entry = table.get(b"Orange").unwrap();

        assert_eq!(entry.key, b"Orange");
        assert_eq!(entry.value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(entry.timestamp, 20);
    }

    #[test]
    fn test_mem_table_get_not_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 0);
        table.set(b"Orange", b"Orange Smoothie", 0);

        let res = table.get(b"Potato");
        assert!(res.is_none());
    }

    #[test]
    fn test_mem_table_delete_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);

        table.delete(b"Apple", 10);

        let res = table.get(b"Apple").unwrap();
        assert_eq!(res.key, b"Apple");
        assert_eq!(res.value, None);
        assert_eq!(res.timestamp, 10);
        assert!(res.deleted);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value, None);
        assert_eq!(table.entries[0].timestamp, 10);
        assert!(table.entries[0].deleted);

        assert_eq!(table.size, 22);
    }

    #[test]
    fn test_mem_table_delete_empty() {
        let mut table = MemTable::new();

        table.delete(b"Apple", 10);

        let res = table.get(b"Apple").unwrap();
        assert_eq!(res.key, b"Apple");
        assert_eq!(res.value, None);
        assert_eq!(res.timestamp, 10);
        assert!(res.deleted);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value, None);
        assert_eq!(table.entries[0].timestamp, 10);
        assert!(table.entries[0].deleted);

        assert_eq!(table.size, 22);
    }
}
