# Notes

## Blog Missing Implementations

The blog does not contain articles that explain how to implement the rest of 
the pieces. RocksDB contains pretty good documentation to try and implement them
ourselves.

- [ ] SSTable
- [ ] Top level DB API
- [ ] Compaction

## Future Improvements

- Cleanup code
    - [ ] MemTable sizing computation and logic on set / delete
- [ ] Change the MemTable implementation to a SkipList similar to what RocksDB does: https://github.com/facebook/rocksdb/wiki/MemTable
- [ ] Use `None` and Null Pointer Optimization
- SSTable
    - [ ] Add a bloom filter for prefixes
    - [ ] Implement the custom hashing scheme for in-memory index
    - [ ] Use a different HashMap implementation for cases with multiple collisions
- Efficiency and correctness
    - [ ] Change "data" representation from Vec<u8> to Arc<[u8]> (or Rc<[u8]>)
    - [ ] Transactions
        - [ ] Sequence numbers and MVCC
