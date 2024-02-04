# write-ahead-log implementation with segmentation and snapshots for cleanup

Append-Only Log

- [x] Write entry to log file
- [x] Read all entries from log files
- [x] Segment log file *new

Write-Ahead-Log based KV Store

- [x] Set a key-value pair
  - [x] Save to log file
  - [x] Update in-memory state
- [x] Get a value for a key
  - [x] Read from in-memory state
- [x] Construct state from the log file

Limitations:

- [ ] No replication support
- [ ] No partitioning support
