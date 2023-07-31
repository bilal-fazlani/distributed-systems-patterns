package com.bilalfazlani.writeAheadLog

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
      path: Path
  ): ZLayer[Any, IOException, AppendOnlyLog[A]] =
    ZLayer.fromZIO(Semaphore.make(1)) >>> ZLayer.fromFunction(
      AppendOnlyLogJsonImpl(path, _)
    )

  def append[A: JsonEncoder: Tag](
      a: A
  ): ZIO[AppendOnlyLog[A] & Scope, IOException, Unit] =
    ZIO.serviceWithZIO[AppendOnlyLog[A]](_.append(a))

  def readAll[A: JsonDecoder: Tag]: ZIO[AppendOnlyLog[A] & Scope, IOException, Seq[A]] =
    ZIO.serviceWithZIO[AppendOnlyLog[A]](_.readAll)

private case class AppendOnlyLogJsonImpl[A: JsonCodec](
    path: Path,
    sem: Semaphore
) extends AppendOnlyLog[A]:
  def append(a: A): ZIO[Scope, IOException, Unit] =
    sem.withPermitScoped *> appendToFile(path, a.toJson)

  def readAll: ZIO[Scope, IOException, Seq[A]] =
    for
      _ <- sem.withPermitScoped
      lines <- readLines(path).map { allLines =>
        val eithers: List[Either[String, A]] =
          allLines.map(line => JsonDecoder[A].decodeJson(line)).toSeq
        eithers
          .foldLeft(Right(List.empty[A]): Either[String, List[A]]) { case (acc, either) =>
            acc.flatMap(list => either.map(list :+ _))
          }
          .left
          .map(e => new IOException(e))
      }.absolve
    yield lines
