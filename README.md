# Distributed Systems Patterns

## Append Only Log

Append only log appends logs to disk. It is used to ensure readers of the data do not lose the data if they crash.

See [AppendOnlyLogJsonImpl.scala](src/main/scala/com/bilalfazlani/3-log-snapshots/log/AppendOnlyLogJsonImpl.scala) for implementation.

## Write Ahead Log

Write Ahead Log uses Append Only Log to persist data. Additionally it stores the data in memory. It provides durability with faster reads.

See [DurableKVStoreImpl.scala](src/main/scala/com/bilalfazlani/3-log-snapshots/kv/DurableKVStoreImpl.scala) for implementation.
