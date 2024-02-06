package com.bilalfazlani.logSnapshots
package kv

import zio.json.JsonCodec
import zio.json.DeriveJsonCodec
import zio.json.JsonFieldDecoder
import zio.json.JsonFieldEncoder

sealed trait KVCommand[K, +V]

object KVCommand:
  case class Set[K, +V](key: K, value: V) extends KVCommand[K, V]
  case class Delete[K](key: K) extends KVCommand[K, Nothing]

  given [K: JsonCodec: JsonFieldDecoder: JsonFieldEncoder, V: JsonCodec]
      : JsonCodec[KVCommand[K, V]] =
    DeriveJsonCodec.gen[KVCommand[K, V]]
