package com.bilalfazlani.logSnapshots
package log

import zio.*
import com.bilalfazlani.*

trait ReadOnlyStorage:
  def lastSnapshot: Task[Option[Long]]
  def lastWriteOffset: Task[Point]

object ReadOnlyStorageImpl extends ReadOnlyStorage:
  def lastSnapshot: Task[Option[Long]] =
    for
      config <- ZIO.config(LogConfiguration.config)
      files <- findFiles(config.dir).runCollect
      last = files
        .collect(_.filename.toString match {
          case s"snapshot-${Long(ofst)}.json" => ofst
        })
        .sorted
        .lastOption
    yield last

  def lastWritePoint: Task[Point] =
    for
      config <- ZIO.config(LogConfiguration.config)
      lastSegment <- findFiles(config.dir)
        .collect { path =>
          path.filename.toString match {
            case s"log-${Long(offset)}.txt" => offset
          }
        }
        .runCollect
        .map(_.sorted.lastOption)
      p <- lastSegment match {
        case Some(offset) =>
          for
            lineCount <- getLineCount(config.dir / s"log-$offset.txt")
            point = Point.NonEmpty(lineCount - 1, Segment(offset))
          yield point
        case None =>
          for
            lwm <- lastSnapshot
            point = lwm.fold(Point.Empty)(lwm =>
              Point.NonEmpty(config.segmentSize - 1, Segment(lwm - config.segmentSize + 1))
            )
          yield point
      }
    yield p
