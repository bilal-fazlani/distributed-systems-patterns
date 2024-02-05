package com.bilalfazlani.logSnapshots
package kv

import log.StateComputer
import zio.ZLayer
import zio.Tag

class StateComputerImpl[K, V] extends StateComputer[KVCommand[K, V], Map[K, V]] {
  val zero: Map[K, V] = Map.empty

  def compute(state: Map[K, V], command: KVCommand[K, V]): Map[K, V] = command match {
    case KVCommand.Set(key, value) => state + (key -> value)
    case KVCommand.Delete(key)     => state - key
  }
}

object StateComputerImpl {
  def live[K: Tag, V: Tag] = ZLayer.succeed(StateComputerImpl[K, V]())
}
