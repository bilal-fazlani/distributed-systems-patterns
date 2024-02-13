package com.bilalfazlani.logSnapshots
package log

import zio.*
import zio.json.*
import com.bilalfazlani.*

case class AppendOnlyLogJsonImpl[LogEntry: JsonCodec](
    sem: Semaphore,
    pointer: Pointer
) extends AppendOnlyLog[LogEntry]:

  def append(entry: LogEntry): ZIO[Scope, Throwable, Unit] =
    for
      config <- ZIO.config(LogConfiguration.config)
      _ <- sem.withPermitScoped
      point <- pointer.inc
      filePath = config.dir / s"log-${point.segment}.txt"
      _ <-
        if point.index == 0L then newFile(filePath, entry.toJson)
        else appendToFile(filePath, entry.toJson)
    yield ()
