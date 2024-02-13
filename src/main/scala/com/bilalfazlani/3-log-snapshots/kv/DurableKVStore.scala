package com.bilalfazlani.logSnapshots
package kv

import zio.*
import zio.json.*
import log.*

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
  def live[K: Tag: JsonCodec: JsonFieldDecoder: JsonFieldEncoder, V: Tag: JsonCodec] =
    ZLayer.fromFunction(DurableKVStoreImpl.apply[K, V])

  def withoutCleanup[K: Tag: JsonCodec: JsonFieldDecoder: JsonFieldEncoder, V: Tag: JsonCodec] =
    ZLayer.makeSome[Scope, DurableKVStore[K, V]](
      ZLayer(Semaphore.make(1)),
      StateLoader.live[KVCommand[K, V], Map[K, V]],
      StateComputerImpl.live[K, V],
      ConcurrentMap.live[K, V],
      AppendOnlyLog.jsonFile[KVCommand[K, V]],
      LowWaterMarkService.fromDisk,
      ZLayer.fromFunction(DurableKVStoreImpl.apply[K, V]),

      // event hub
      ZLayer(Hub.sliding[Event](5))
    )

  def default[K: Tag: JsonCodec: JsonFieldDecoder: JsonFieldEncoder, V: Tag: JsonCodec] =
    ZLayer.makeSome[Scope, DurableKVStore[K, V]](
      ZLayer(Semaphore.make(1)),
      StateLoader.live[KVCommand[K, V], Map[K, V]],
      StateComputerImpl.live[K, V],
      ConcurrentMap.live[K, V],
      AppendOnlyLog.jsonFile[KVCommand[K, V]],
      LowWaterMarkService.fromDisk,
      ZLayer.fromFunction(DurableKVStoreImpl.apply[K, V]),

      // event hub
      ZLayer(Hub.sliding[Event](5)),

      // cleanup
      Pointer.fromDisk[Map[K, V]],
      SnapshotService.start[Map[K, V]],
      DataDiscardService.live
    )

  def get[K: JsonCodec: Tag, V: JsonCodec: Tag](key: K) =
    ZIO.serviceWithZIO[KVReader[K, V]](_.get(key))

  def set[K: JsonCodec: Tag, V: JsonCodec: Tag](key: K, value: V) =
    ZIO.serviceWithZIO[KVWriter[K, V]](_.set(key, value))

  def delete[K: JsonCodec: Tag](key: K) =
    ZIO.serviceWithZIO[KVWriter[K, Nothing]](_.delete(key))
