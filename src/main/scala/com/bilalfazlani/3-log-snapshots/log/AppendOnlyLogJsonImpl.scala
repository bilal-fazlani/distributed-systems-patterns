package com.bilalfazlani.logSnapshots
package log

import zio.*
import zio.json.*
import zio.nio.file.Path
import java.nio.file.StandardOpenOption
import zio.nio.channels.FileChannel
import Point.*

case class AppendOnlyLogJsonImpl[LogEntry: JsonCodec](
    sem: Semaphore,
    pointer: Pointer,
    ref: Ref.Synchronized[FileChannel]
) extends AppendOnlyLog[LogEntry]:

  def append(entry: LogEntry) =
    for
      config <- ZIO.config(LogConfiguration.config)
      _ <- sem.withPermitScoped
      point <- pointer.inc
      channel <-
        if point.index == 0L then
          ref.updateAndGetZIO(old =>
            old.close *>
              AppendOnlyLogJsonImpl.open(config.dir / s"log-${point.segment}.txt")
          )
        else ref.get
      _ <- channel.flatMapBlocking(_.writeChunk(Chunk.from(entry.toJson.getBytes)))
    yield ()

object AppendOnlyLogJsonImpl:
  private[this] def open(path: Path): Task[FileChannel] =
    ZIO.attempt(
      FileChannel.fromJava(
        java.nio.channels.FileChannel
          .open(path.toFile.toPath, StandardOpenOption.APPEND, StandardOpenOption.CREATE)
      )
    )

  def live[LogEntry: JsonCodec: Tag] =
    ZLayer
      .fromFunction((pointer: Pointer, sem: Semaphore) =>
        for
          config <- ZIO.config(LogConfiguration.config)
          point <- pointer.get
          fileChannel <- point match
            case Empty                    => open(config.dir / "log-0.txt")
            case NonEmpty(index, segment) => open(config.dir / s"log-${segment.value}.txt")
          ref <- Ref.Synchronized.make(fileChannel)
          service = new AppendOnlyLogJsonImpl[LogEntry](sem, pointer, ref)
        yield service
      )
      .flatMap(x => ZLayer.fromZIO(x.get))
