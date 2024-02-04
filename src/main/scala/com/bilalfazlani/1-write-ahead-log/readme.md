# write-ahead-log implementation

Append-Only Log

- [x] Write entry to log file
- [x] Read all entries from log file

Write-Ahead-Log based KV Store

- [x] Set a key value pair
  - [x] Save to log file
  - [x] Update in-memory state
- [x] Get a value for a key
  - [x] Read from in-memory state
- [x] Construct state from the log file

Limitations:

- [ ] Not segmented. Single infinite log file
- [ ] No replication support
- [ ] No partitioning support
