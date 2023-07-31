package com.bilalfazlani.logSegmentation

import zio.*
import zio.json.*
import zio.nio.file.Path
import java.io.IOException
import com.bilalfazlani.*

trait AppendOnlyLog[A]:
  def append(a: A): ZIO[Scope, IOException, Unit]
  def readAll: ZIO[Scope, IOException, Seq[A]]

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

  def readAll[A: JsonDecoder: Tag]: ZIO[AppendOnlyLog[A] & Scope, IOException, Seq[A]] =
    ZIO.serviceWithZIO[AppendOnlyLog[A]](_.readAll)

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

  def readAll: ZIO[Scope, IOException, Seq[A]] =
    for
      _ <- sem.withPermitScoped
      filePaths <- findFiles(dir).runCollect.map(_.sortBy(_.filename.toString))
      allLines <- ZIO.collectAll(filePaths.map(readFile)).map(_.flatten)
    yield allLines

  private def readFile(path: Path): IO[IOException, List[A]] =
    readLines(path).map { allLines =>
      val eithers: List[Either[String, A]] =
        allLines.map(line => JsonDecoder[A].decodeJson(line)).toSeq
      eithers
        .foldLeft(Right(List.empty[A]): Either[String, List[A]]) { case (acc, either) =>
          acc.flatMap(list => either.map(list :+ _))
        }
        .left
        .map(e => new IOException(e))
    }.absolve
