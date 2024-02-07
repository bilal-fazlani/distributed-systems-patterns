package com.bilalfazlani.logSnapshots
package log

import zio.*
import zio.nio.file.Path
import com.bilalfazlani.*
import zio.json.JsonCodec

trait Pointer:
  def inc: Task[IncResult]

  /** index of the current item in the segment
    */
  def localIndex: UIO[Long]

  /** total index of the item in all segments
    */
  def totalIndex: UIO[Long]

  /** index of the first item in the segment
    */
  def segmentOffset: UIO[Long]

case class PointerImpl(pointRef: Ref[Point], notification: Hub[Event]) extends Pointer:
  def inc: Task[IncResult] = for {
    segmentSize <- ZIO.config[LogConfiguration](LogConfiguration.config).map(_.segmentSize)
    tuple <- pointRef.modify { p =>
      val (result, newPoint) = p.inc(segmentSize)
      ((result, newPoint), newPoint)
    }
    (result, newPoint) = tuple
    _ <- notification.publish(Event.PointerMoved(newPoint)).fork
  } yield result

  def localIndex: UIO[Long] = pointRef.get.map(_.localIndex)
  def totalIndex: UIO[Long] = pointRef.get.map(_.totalIndex)
  def segmentOffset: UIO[Long] = pointRef.get.map(_.segmentOffset)

case class Point(
    localIndex: Long,
    totalIndex: Long,
    segmentOffset: Long
) {
  def inc(segmentSize: Long): (IncResult, Point) = {
    if totalIndex == 0 || localIndex == segmentSize then
      (
        IncResult.NewFile(totalIndex),
        copy(
          localIndex = 1,
          totalIndex = totalIndex + 1,
          segmentOffset = totalIndex
        )
      )
    else
      (
        IncResult.SameFile(localIndex + 1, segmentOffset),
        copy(localIndex = localIndex + 1, totalIndex = totalIndex + 1)
      )
  }
}

enum IncResult:
  case SameFile(localLines: Long, segmentOffset: Long)
  case NewFile(totalLines: Long)

object Pointer:
  def fromDisk[St: JsonCodec: Tag]: ZLayer[Hub[Event], Exception, Pointer] =
    val f = (notification: Hub[Event]) => ZLayer(for
      config <- ZIO.config(LogConfiguration.config)
      fileOffsets <- findFiles(config.dir)
        .collect { path =>
          path.filename.toString match {
            case s"log-${Long(offset)}.txt" => offset
          }
        }
        .runCollect
        .map(_.sorted)
      lastFileOffset = fileOffsets.lastOption.getOrElse(0L)
      lineCount <- getLineCount(config.dir / s"log-$lastFileOffset.txt")
      stateRef <- Ref.make(
        Point(lineCount, lastFileOffset + lineCount, lastFileOffset)
      )
    yield PointerImpl(stateRef, notification))
    ZLayer.fromFunction(f).flatten
