package com.bilalfazlani.writeAheadLog

import zio.*

trait MemoryState[K, V]:
  def get(key: K): UIO[Option[V]]
  def set(key: K, value: V): UIO[Unit]
  def delete(key: K): UIO[Unit]

object MemoryState:
  def live[K: Tag, V: Tag](
      map: Map[K, V]
  ): ZLayer[Any, Nothing, MemoryState[K, V]] =
    ZLayer.fromZIO(Ref.make(map).map { ref =>
      new MemoryState[K, V]:
        def get(key: K): UIO[Option[V]] = ref.get.map(_.get(key))
        def set(key: K, value: V): UIO[Unit] = ref.update(_ + ((key, value)))
        def delete(key: K): UIO[Unit] = ref.update(_ - key)
    })
