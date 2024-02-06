package com.bilalfazlani.logSnapshots
package kv

import log.State
import zio.*

case class StateImpl[K, V](map: ConcurrentMap[K, V]) extends log.State[Map[K, V]]:
  def get = map.getAll

object StateImpl:
  def live[K: Tag, V: Tag]: ZLayer[ConcurrentMap[K, V], Nothing, State[Map[K, V]]] = ZLayer.fromFunction(StateImpl.apply[K, V])