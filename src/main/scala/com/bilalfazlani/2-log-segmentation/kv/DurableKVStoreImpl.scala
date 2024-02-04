package com.bilalfazlani.logSegmentation
package kv

import zio.*
import log.AppendOnlyLog

private[kv] case class DurableKVStoreImpl[K, V](
    memoryState: ConcurrentMap[K, V],
    fileLog: AppendOnlyLog[KVCommand[K, V]]
) extends DurableKVStore[K, V]:
  def get(key: K): UIO[Option[V]] = memoryState.get(key)

  def set(key: K, value: V): Task[Unit] =
    ZIO.scoped {
      fileLog.append(KVCommand.Set(key, value)) *> memoryState.set(key, value)
    }

  def delete(key: K): Task[Unit] =
    ZIO.scoped {
      fileLog.append(KVCommand.Delete(key)) *> memoryState.delete(
        key
      )
    }
