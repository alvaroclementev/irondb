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
