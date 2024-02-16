package com.bilalfazlani.logSnapshots
package log

import zio.*

extension (layerType: ZLayer.type)
  def fromFunctionZIO[I: Tag, R: Tag, E: Tag, O: Tag](f: I => ZIO[R, E, O]): ZLayer[I & R, E, O] =
    ZLayer.fromFunction(f).flatMap(y => ZLayer.fromZIO(y.get))
