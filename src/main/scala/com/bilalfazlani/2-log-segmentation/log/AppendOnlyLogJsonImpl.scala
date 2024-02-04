package com.bilalfazlani.logSegmentation
package log

import zio.*
import zio.json.*
import zio.nio.file.Path
import java.io.IOException
import com.bilalfazlani.*
import zio.stream.ZStream

case class AppendOnlyLogJsonImpl[A: JsonCodec](
    sem: Semaphore,
    state: Ref[State],
    dir: Path,
    segmentSize: Long,
    // stateComputer: StateComputer[A, State],
    // stateLoader: StateLoader[State]
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

  def computeState[State](initial: State)(f: (State, A) => State): ZIO[Scope, Throwable, State] =
    for
      _ <- sem.withPermitScoped
      filePaths <- findFiles(dir).runCollect.map(_.sortBy(_.filename.toString))
      aStream = filePaths
        .map(p =>
          streamLines(p)
            .map(line => JsonDecoder[A].decodeJson(line).left.map(e => Throwable(e)))
            .absolve
        )
        .foldLeft[ZStream[Any, Throwable, A]](ZStream.empty)(_ ++ _)
      state <- aStream.runFold(initial)(f)
    yield state
