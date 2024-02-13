package com.bilalfazlani.logSnapshots
package log

import com.bilalfazlani.*
import zio.*
import zio.json.JsonCodec
import zio.nio.file.Path
import com.bilalfazlani.logSnapshots.log.Point.NonEmpty
import zio.schema.*

trait Pointer:
  def inc: Task[Point.NonEmpty]
  def get: UIO[Point]

case class PointerImpl(pointRef: Ref[Point], notification: Hub[Event]) extends Pointer:
  def inc: Task[Point.NonEmpty] = for {
    segmentSize <- ZIO.config[LogConfiguration](LogConfiguration.config).map(_.segmentSize)
    result <- pointRef.updateAndGet(_.inc(segmentSize))
    _ <- notification.publish(Event.PointerMoved(result))
  } yield result.asInstanceOf[Point.NonEmpty]

  def get: UIO[Point] = pointRef.get

opaque type Segment = Long

object Segment:
  given Schema[Segment] = Schema.primitive[Long]
  def apply(value: Long): Segment = value
  extension (s: Segment) def value: Long = s

sealed trait Point:

  def inc(segmentSize: Long): Point.NonEmpty

object Point:
  case object Empty extends Point:
    def inc(segmentSize: Long): NonEmpty = NonEmpty(0, Segment(0))

  /** @param index
    *   local index of the last written line in segment
    * @param segment
    *   segment number. this is index of the first line in the segment
    */
  case class NonEmpty(index: Long, segment: Segment) extends Point:
    def totalIndex: Long = index + segment.value

    def inc(segmentSize: Long): NonEmpty =
      // new segment
      if index == segmentSize - 1 then NonEmpty(0, Segment(totalIndex + 1))
      // same segment
      else NonEmpty(index + 1, segment)

object Pointer:
  def fromDisk[St: JsonCodec: Tag]: ZLayer[Hub[Event], Exception, Pointer] =
    val f = (notification: Hub[Event]) =>
      ZLayer(for
        config <- ZIO.config(LogConfiguration.config)
        lastOffset <- findFiles(config.dir)
          .collect { path =>
            path.filename.toString match {
              case s"log-${Long(offset)}.txt" => offset
            }
          }
          .runCollect
          .map(_.sorted.lastOption)

        p <- lastOffset match {
          case Some(offset) =>
            for
              lineCount <- getLineCount(config.dir / s"log-$offset.txt")
              point = Point.NonEmpty(lineCount - 1, Segment(offset))
              stateRef: Ref[Point] <- Ref.make(point)
            yield PointerImpl(stateRef, notification)
          case None =>
            Ref.make(Point.Empty).map((x: Ref[Point]) => PointerImpl(x, notification))
        }
      yield p)
    ZLayer.fromFunction(f).flatten
