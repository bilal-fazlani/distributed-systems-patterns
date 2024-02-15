package com.bilalfazlani.logSnapshots
package log

import SegmentRange.{RangeResult}

case class SegmentRange(start: Long, end: Long) {
  def contains(lwm: Long): RangeResult =
    end match {
      case _: Long if lwm < start  => RangeResult.Before
      case end: Long if lwm < end  => RangeResult.Inside
      case end: Long if lwm == end => RangeResult.End
      case end: Long               => RangeResult.After
    }

  override def toString = s"$start ... $end"
}

object SegmentRange:
  def fromSegmentList(segments: Seq[Long], segmentSize: Long): List[SegmentRange] =
    val ranges = scala.collection.mutable.ListBuffer[SegmentRange]()
    var i = 0
    while (i < segments.length) do {
      val isLast = i == (segments.length - 1)
      ranges += {
        if isLast then SegmentRange(segments(i), segments(i) + segmentSize - 1)
        else SegmentRange(segments(i), segments(i + 1) - 1)
      }
      i += 1
    }
    return ranges.toList

  enum RangeResult:
    case Inside, End, Before, After
