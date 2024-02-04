package com.bilalfazlani.logSegmentation
package kv

import zio.*
import zio.nio.file.*
import zio.json.*
import log.AppendOnlyLog

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
  def live[K: JsonCodec: Tag, V: JsonCodec: Tag](
      path: Path
  ) =
    ZLayer.makeSome[AppendOnlyLog[KVCommand[K, V]], DurableKVStore[K, V]](
      applyState[K, V],
      ZLayer.fromFunction(DurableKVStoreImpl.apply[K, V])
    )

  def get[K: JsonCodec: Tag, V: JsonCodec: Tag](key: K) =
    ZIO.serviceWithZIO[KVReader[K, V]](_.get(key))

  def set[K: JsonCodec: Tag, V: JsonCodec: Tag](key: K, value: V) =
    ZIO.serviceWithZIO[KVWriter[K, V]](_.set(key, value))

  def delete[K: JsonCodec: Tag](key: K) =
    ZIO.serviceWithZIO[KVWriter[K, Nothing]](_.delete(key))

  private def applyState[
      K: JsonCodec: Tag,
      V: JsonCodec: Tag
  ]: ZLayer[AppendOnlyLog[KVCommand[K, V]], Throwable, MemoryState[K, V]] =
    ZLayer
      .fromZIO(
        ZIO
          .scoped(
            AppendOnlyLog.computeState[KVCommand[K, V], Map[K, V]](Map.empty)((state, command) =>
              command match
                case KVCommand.Set(k, v)   => state + ((k, v))
                case KVCommand.Delete(key) => state - key
            )
          )
          .map(MemoryState.live[K, V](_))
      )
      .flatten
