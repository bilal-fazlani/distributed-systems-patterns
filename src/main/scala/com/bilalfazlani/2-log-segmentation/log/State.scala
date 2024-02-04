package com.bilalfazlani.logSegmentation
package log

import zio.*
import zio.nio.file.Path
import java.io.IOException
import com.bilalfazlani.*

/** Implementation of AppendOnlyLog using json files
  */
case class State private (
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

object State:
  def loadFromDisk(dir: Path): IO[IOException, State] =
    for
      fileOffsets <- findFiles(dir)
        .collect { path =>
          path.filename.toString match {
            case s"log-$offset.txt" => offset.toLong
          }
        }
        .runCollect
        .map(_.sorted)
      lastFileOffset = fileOffsets.lastOption.getOrElse(0L)
      lineCount <- getLineCount(dir / s"log-$lastFileOffset.txt")
    yield State(lineCount, lastFileOffset + lineCount, lastFileOffset)
