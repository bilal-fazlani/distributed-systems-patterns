package com.bilalfazlani.logSnapshots
package kv

class StateImpl[K, V](map: ConcurrentMap[K, V]) extends log.State[Map[K, V]]:
  def get = map.getAll
