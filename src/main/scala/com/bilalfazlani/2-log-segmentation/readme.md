# write-ahead-log implementation with segmentation and snapshots for cleanup

Append-Only Log

- [x] Write entry to log file
- [x] Read all entries from log files (from given offset *new)
- [ ] Segment log file *new
- [ ] Snapshot log files *new
- [ ] Cleanup log files *new

Write-Ahead-Log based KV Store

- [x] Set a key value pair
  - [x] Save to log file
  - [x] Update inmemory state
- [x] Get a value for a key
  - [x] Read from inmemory state
- [x] Construct state from the log file

Limitations:

- [ ] No replication support
- [ ] No partitioning support
