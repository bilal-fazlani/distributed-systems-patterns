package com.bilalfazlani.logSnapshots
package log

import zio.*
import zio.nio.file.Path
import com.bilalfazlani.*

case class Pointer private (
    /** index of the current item in the segment
      */
    localIndex: Long,
    /** total index of the item in all segments
      */
    totalIndex: Long,
    /** index of the first item in the segment
      */
    segmentOffset: Long
) {

  def inc(segmentSize: Long) = {
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
  extension (state: Ref[Pointer])
    def inc(segmentSize: Long): UIO[IncResult] = state.modify { s =>
      s.inc(segmentSize)
    }

  def fromDisk =
    ZLayer(for
      config <- ZIO.config(LogConfiguration.config)
      fileOffsets <- findFiles(config.dir)
        .collect { path =>
          path.filename.toString match {
            case s"log-$offset.txt" => offset.toLong
          }
        }
        .runCollect
        .map(_.sorted)
      lastFileOffset = fileOffsets.lastOption.getOrElse(0L)
      lineCount <- getLineCount(config.dir / s"log-$lastFileOffset.txt")
      stateRef <- Ref.make(
        Pointer(lineCount, lastFileOffset + lineCount, lastFileOffset)
      )
    yield stateRef)
