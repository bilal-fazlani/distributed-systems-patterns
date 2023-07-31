package com.bilalfazlani.logSegmentation

import zio.*
import zio.json.*
import zio.nio.file.Path
import java.io.IOException
import com.bilalfazlani.*
import zio.stream.ZStream

trait AppendOnlyLog[A]:
  def append(a: A): ZIO[Scope, IOException, Unit]
  def computeState[State](initial: State)(f: (State, A) => State): ZIO[Scope, Throwable, State]

object AppendOnlyLog:
  def jsonFile[A: JsonCodec: Tag](
      dir: Path,
      maxLines: Long
  ): ZLayer[Any, IOException, AppendOnlyLogJsonImpl[A]] =
    (ZLayer.fromZIO(Semaphore.make(1)) ++ ZLayer.fromZIO(
      State.loadFromDisk(dir).flatMap(Ref.make)
    )) >>>
      ZLayer.fromFunction(AppendOnlyLogJsonImpl.apply(_, _, dir, maxLines))

  def append[A: JsonEncoder: Tag](
      a: A
  ): ZIO[AppendOnlyLog[A] & Scope, IOException, Unit] =
    ZIO.serviceWithZIO[AppendOnlyLog[A]](_.append(a))

  def computeState[A: JsonDecoder: Tag, State](
      initial: State
  )(f: (State, A) => State): ZIO[AppendOnlyLog[A] & Scope, Throwable, State] =
    ZIO.serviceWithZIO[AppendOnlyLog[A]](_.computeState(initial)(f))

private case class State private (
    currentLines: Long,
    totalLines: Long,
    segmentOffset: Long
) {
  def inc(maxLines: Long) = {
    if totalLines == 0 || currentLines == maxLines then
      (
        IncResult.NewFile(totalLines),
        copy(
          currentLines = 1,
          totalLines = totalLines + 1,
          segmentOffset = totalLines
        )
      )
    else
      (
        IncResult.SameFile(currentLines + 1, segmentOffset),
        copy(currentLines = currentLines + 1, totalLines = totalLines + 1)
      )
  }
}

private object State:
  def loadFromDisk(dir: Path): IO[IOException, State] =
    for
      fileOffsets <- findFiles(dir)
        .collect { path =>
          path.filename.toString match {
            case s"log-$offset.txt" => offset.toLong
          }
        }
        .runCollect
        .map(_.sorted)
      lastFileOffset = fileOffsets.lastOption.getOrElse(0L)
      lineCount <- getLineCount(dir / s"log-$lastFileOffset.txt")
    yield State(lineCount, lastFileOffset + lineCount, lastFileOffset)

enum IncResult:
  case SameFile(localLines: Long, segmentOffset: Long)
  case NewFile(totalLines: Long)

extension (ref: Ref[State])
  def inc(maxLines: Long): UIO[IncResult] = ref.modify { s =>
    s.inc(maxLines)
  }

private case class AppendOnlyLogJsonImpl[A: JsonCodec](
    sem: Semaphore,
    state: Ref[State],
    dir: Path,
    maxLines: Long
) extends AppendOnlyLog[A]:
  def append(a: A): ZIO[Scope, IOException, Unit] =
    for
      _ <- sem.withPermitScoped
      incResult <- state.inc(maxLines)
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
