package com.bilalfazlani.logSegmentation
package log

import zio.*
import zio.json.*
import zio.nio.file.Path
import java.io.IOException
import com.bilalfazlani.*

case class AppendOnlyLogJsonImpl[A: JsonCodec](
    sem: Semaphore,
    state: Ref[State],
    dir: Path,
    segmentSize: Long
) extends AppendOnlyLog[A]:

  private def inc(state: Ref[State]): UIO[IncResult] = state.modify { s =>
    s.inc(segmentSize)
  }

  def append(a: A): ZIO[Scope, IOException, Unit] =
    for
      _ <- sem.withPermitScoped
      incResult <- inc(state)
      filePath = incResult match {
        case IncResult.SameFile(localLines, segmentOffset) => dir / s"log-$segmentOffset.txt"
        case IncResult.NewFile(totalLines)                 => dir / s"log-$totalLines.txt"
      }
      _ <-
        if incResult == IncResult.NewFile then newFile(filePath, a.toJson)
        else appendToFile(filePath, a.toJson)
    yield ()
