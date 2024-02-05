package com.bilalfazlani.logSnapshots

package log

import zio.*
import zio.json.*
import zio.nio.file.Path
import java.io.IOException
import com.bilalfazlani.*

trait AppendOnlyLog[LogEntry]:
  def append(entry: LogEntry): ZIO[Scope, Throwable, Unit]

object AppendOnlyLog:
  def jsonFile[LogEntry: JsonCodec: Tag](
      dir: Path,
      maxLines: Long
  ): ZLayer[Semaphore, IOException, AppendOnlyLogJsonImpl[LogEntry]] =
    Pointer.fromDisk(dir) >>>
      ZLayer.fromFunction(AppendOnlyLogJsonImpl.apply(_, _, dir, maxLines))

  def append[LogEntry: JsonEncoder: Tag](
      entry: LogEntry
  ): ZIO[AppendOnlyLog[LogEntry] & Scope, Throwable, Unit] =
    ZIO.serviceWithZIO[AppendOnlyLog[LogEntry]](_.append(entry))
