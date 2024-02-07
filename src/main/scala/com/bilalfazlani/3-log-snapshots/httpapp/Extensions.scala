package com.bilalfazlani.logSnapshots
package httpapp

import zio.schema.Schema

extension [A: Schema](a: A)
  /** Convert the given value to json
    */
  def json: String =
    zio.schema.codec.JsonCodec.jsonCodec(Schema[A]).encodeJson(a, None).toString