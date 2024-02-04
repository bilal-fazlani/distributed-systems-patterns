package com.bilalfazlani.logSegmentation

package log

import zio.*
import zio.json.*
import zio.nio.file.Path
import java.io.IOException
import com.bilalfazlani.*

trait AppendOnlyLog[A]:
  def append(a: A): ZIO[Scope, IOException, Unit]

object AppendOnlyLog:
  def jsonFile[A: JsonCodec: Tag](
      dir: Path,
      maxLines: Long
  ): ZLayer[Semaphore, IOException, AppendOnlyLogJsonImpl[A]] =
    State.fromDisk(dir) >>>
      ZLayer.fromFunction(AppendOnlyLogJsonImpl.apply(_, _, dir, maxLines))

  def append[A: JsonEncoder: Tag](
      a: A
  ): ZIO[AppendOnlyLog[A] & Scope, IOException, Unit] =
    ZIO.serviceWithZIO[AppendOnlyLog[A]](_.append(a))
