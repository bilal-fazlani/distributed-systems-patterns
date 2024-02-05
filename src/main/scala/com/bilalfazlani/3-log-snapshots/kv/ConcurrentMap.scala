package com.bilalfazlani.logSnapshots
package kv

import zio.*
import log.StateLoader
import log.StateComputer

trait ConcurrentMap[K, V]:
  def get(key: K): UIO[Option[V]]
  def getAll: UIO[Map[K, V]]
  def set(key: K, value: V): UIO[Unit]
  def delete(key: K): UIO[Unit]

object ConcurrentMap:
  def live[K: Tag, V: Tag]: ZLayer[
    StateLoader[Map[K, V]] & StateComputer[KVCommand[K, V], Map[K, V]],
    Throwable,
    ConcurrentMap[K, V]
  ] =
    ZLayer.fromZIO(
      ZIO.scoped(
        for {
          loader <- ZIO.service[StateLoader[Map[K, V]]]
          data <- loader.load
          mapRef <- Ref.make(data)
          stateComputer <- ZIO.service[StateComputer[KVCommand[K, V], Map[K, V]]]
          impl = ConcurrentMapImpl(mapRef, stateComputer).asInstanceOf[ConcurrentMap[K, V]]
        } yield impl
      )
    )

case class ConcurrentMapImpl[K, V](
    map: Ref[Map[K, V]],
    computer: StateComputer[KVCommand[K, V], Map[K, V]]
) extends ConcurrentMap[K, V]:
  def get(key: K): UIO[Option[V]] = map.get.map(_.get(key))

  def getAll: UIO[Map[K, V]] = map.get

  def set(key: K, value: V): UIO[Unit] =
    map.update(m => computer.compute(m, KVCommand.Set(key, value)))
  def delete(key: K): UIO[Unit] =
    map.update(m => computer.compute(m, KVCommand.Delete(key)))
