# Distributed Systems Patterns

## Append Only Log

Append only log appends logs to disk. It is used to ensure readers of the data do not lose the data if they crash.

See [AppendOnlyLog.scala](src/main/scala/com/bilalfazlani/AppendOnlyLog.scala) for implementation.

## Write Ahead Log

Write Ahead Log uses Append Only Log to store data. Additionally it stores the data in memory. It comes with the same advantages as Append Only Log but also additionally provides faster reads.

See [DurableKVStore.scala](src/main/scala/com/bilalfazlani/DurableKVStore.scala) for implementation.