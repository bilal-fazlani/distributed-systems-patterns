package com.bilalfazlani.durableKVStore

import zio.json.JsonCodec
import zio.json.DeriveJsonCodec

sealed trait KVCommand[K, +V]
object KVCommand:
  case class Set[K, +V](key: K, value: V) extends KVCommand[K, V]
  case class Delete[K](key: K) extends KVCommand[K, Nothing]

  given [K: JsonCodec, V: JsonCodec]: JsonCodec[KVCommand[K, V]] =
    DeriveJsonCodec.gen[KVCommand[K, V]]
