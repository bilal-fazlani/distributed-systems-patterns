package com.bilalfazlani.logSnapshots

package log

import zio.*
import com.bilalfazlani.*
import zio.json.JsonCodec

trait AppendOnlyLog[LogEntry]:
  def append(entry: LogEntry): ZIO[Scope, Throwable, Unit]

object AppendOnlyLog:
  def jsonFile[LogEntry: JsonCodec: Tag] = AppendOnlyLogJsonImpl.live[LogEntry]
