# write-ahead-log implementation with segmentation and snapshots for cleanup

Design: 
- Anytime we need to append to log file, we go via semaphore
- Anytime we need to access Pointer, we go via semaphore

Append-Only Log

- [x] Write entry to log file
- [x] Read all entries from log files
- [x] Segment log file
- [x] Snapshot-based low watermark cleanup *new

Write-Ahead-Log based KV Store

- [x] Set a key-value pair
  - [x] Save to log file
  - [x] Update in-memory state
- [x] Get a value for a key
  - [x] Read from in-memory state
- [x] Construct state from the log file
