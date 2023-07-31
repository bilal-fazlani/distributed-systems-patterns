package com.bilalfazlani.durableKVStore

import zio.*
import zio.json.*
import zio.nio.file.Files
import java.nio.file.StandardOpenOption
import zio.nio.file.Path
import zio.nio.charset.Charset
import java.io.IOException

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

  def readAll[A: JsonDecoder: Tag]
      : ZIO[AppendOnlyLog[A] & Scope, IOException, Seq[A]] =
    ZIO.serviceWithZIO[AppendOnlyLog[A]](_.readAll)

private case class AppendOnlyLogJsonImpl[A: JsonCodec](
    path: Path,
    sem: Semaphore
) extends AppendOnlyLog[A]:
  def append(a: A): ZIO[Scope, IOException, Unit] =
    sem.withPermitScoped *> Files.writeLines(
      path,
      Seq(a.toJson),
      Charset.Standard.utf8,
      Set(StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    )

  def readAll: ZIO[Scope, IOException, Seq[A]] =
    sem.withPermitScoped *>
      ZIO
        .whenZIO(Files.exists(path))(
          Files
            .readAllLines(path, Charset.Standard.utf8)
            .map { allLines =>
              val eithers: List[Either[String, A]] =
                allLines.map(line => JsonDecoder[A].decodeJson(line)).toSeq
              eithers
                .foldLeft(Right(List.empty[A]): Either[String, List[A]]) {
                  case (acc, either) =>
                    acc.flatMap(list => either.map(list :+ _))
                }
                .left
                .map(e => new IOException(e))
            }
            .absolve
        )
        .map(_.getOrElse(Seq.empty[A]))
