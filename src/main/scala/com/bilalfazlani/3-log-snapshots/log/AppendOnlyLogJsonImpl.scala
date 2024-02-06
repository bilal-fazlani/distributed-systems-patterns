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
      incResult <- pointer.inc
      filePath = incResult match {
        case IncResult.SameFile(localLines, segmentOffset) => config.dir / s"log-$segmentOffset.txt"
        case IncResult.NewFile(totalLines)                 => config.dir / s"log-$totalLines.txt"
      }
      _ <-
        if incResult == IncResult.NewFile then newFile(filePath, entry.toJson)
        else appendToFile(filePath, entry.toJson)
    yield ()
