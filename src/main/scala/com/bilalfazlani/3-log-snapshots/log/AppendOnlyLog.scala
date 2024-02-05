package com.bilalfazlani.logSnapshots

package log

import zio.*
import zio.json.*
import com.bilalfazlani.*

trait AppendOnlyLog[LogEntry]:
  def append(entry: LogEntry): ZIO[Scope, Throwable, Unit]

object AppendOnlyLog:
  def jsonFile[LogEntry: JsonCodec: Tag]
      : ZLayer[Semaphore, Exception, AppendOnlyLogJsonImpl[LogEntry]] =
    Pointer.fromDisk >>> ZLayer.fromFunction(AppendOnlyLogJsonImpl.apply[LogEntry])

  def append[LogEntry: JsonEncoder: Tag](
      entry: LogEntry
  ): ZIO[AppendOnlyLog[LogEntry] & Scope, Throwable, Unit] =
    ZIO.serviceWithZIO[AppendOnlyLog[LogEntry]](_.append(entry))
