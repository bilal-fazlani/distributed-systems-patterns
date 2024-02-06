package com.bilalfazlani.logSnapshots
package log

import zio.*

trait DataDiscardService

object DataDiscardService:
  def start: ZLayer[Scope, Exception, Unit] = ZLayer.succeed(())

class DataDiscardServiceImpl() extends DataDiscardService