package com.bilalfazlani.logSnapshots
package kv

import zio.*
import zio.nio.file.*
import zio.json.*
import log.AppendOnlyLog
import log.StateLoader

trait KVReader[-K, +V]:
  def get(key: K): UIO[Option[V]]

trait KVWriter[-K, -V]:
  def set(key: K, value: V): Task[Unit]
  def delete(key: K): Task[Unit]

/** A key value store that persists data to disk. It uses write ahead logging to persist data.
  * Reading data is done from memory.
  */
trait DurableKVStore[K, V] extends KVReader[K, V] with KVWriter[K, V]

object DurableKVStore:
  def live[K: Tag: JsonCodec, V: Tag: JsonCodec](path: Path, segmentSize: Long) =
    ZLayer.make[DurableKVStore[K, V]](
      ZLayer.fromZIO(Semaphore.make(1)),
      StateLoader.live[KVCommand[K, V], Map[K, V]](path),
      StateComputerImpl.live[K, V],
      ConcurrentMap.live[K, V],
      ZLayer.fromFunction(DurableKVStoreImpl.apply[K, V]),
      AppendOnlyLog.jsonFile[KVCommand[K, V]](path, segmentSize)
    )

  def get[K: JsonCodec: Tag, V: JsonCodec: Tag](key: K) =
    ZIO.serviceWithZIO[KVReader[K, V]](_.get(key))

  def set[K: JsonCodec: Tag, V: JsonCodec: Tag](key: K, value: V) =
    ZIO.serviceWithZIO[KVWriter[K, V]](_.set(key, value))

  def delete[K: JsonCodec: Tag](key: K) =
    ZIO.serviceWithZIO[KVWriter[K, Nothing]](_.delete(key))
