package com.bilalfazlani.logSnapshots
package log

import zio.*
import zio.json.*
import zio.nio.file.Path
import java.io.IOException
import com.bilalfazlani.*

case class AppendOnlyLogJsonImpl[LogEntry: JsonCodec](
    sem: Semaphore,
    state: Ref[Pointer],
    dir: Path,
    segmentSize: Long
) extends AppendOnlyLog[LogEntry]:

  private def inc(state: Ref[Pointer]): UIO[IncResult] = state.modify { s =>
    s.inc(segmentSize)
  }

  def append(entry: LogEntry): ZIO[Scope, IOException, Unit] =
    for
      _ <- sem.withPermitScoped
      incResult <- inc(state)
      filePath = incResult match {
        case IncResult.SameFile(localLines, segmentOffset) => dir / s"log-$segmentOffset.txt"
        case IncResult.NewFile(totalLines)                 => dir / s"log-$totalLines.txt"
      }
      _ <-
        if incResult == IncResult.NewFile then newFile(filePath, entry.toJson)
        else appendToFile(filePath, entry.toJson)
    yield ()
