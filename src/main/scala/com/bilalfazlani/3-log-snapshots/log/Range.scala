package com.bilalfazlani.logSnapshots
package log

import SegmentRange.{RangeResult, Infinity}

case class SegmentRange(start: Long, end: Long | Infinity.type) {
  def contains(lwm: Long): RangeResult =
    end match {
      case _: Long if lwm < start  => RangeResult.Before
      case i: Infinity.type        => RangeResult.Inside
      case end: Long if lwm < end  => RangeResult.Inside
      case end: Long if lwm == end => RangeResult.End
      case end: Long if lwm > end  => RangeResult.After
    }
  override def toString = s"$start ... $end"
}

object SegmentRange:
  enum RangeResult:
    case Inside, End, Before, After

  case object Infinity {
    override def toString(): String = "Inf"
  }
  def apply(start: Long): SegmentRange = new SegmentRange(start, Infinity)
