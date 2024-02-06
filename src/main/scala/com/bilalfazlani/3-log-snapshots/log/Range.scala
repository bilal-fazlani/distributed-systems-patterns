package com.bilalfazlani.logSnapshots
package log

import SegmentRange.{RangeResult, Infinity}

case class SegmentRange(start: Long, end: Long | Infinity.type) {
  def contains(snapshot: Long): RangeResult =
    if snapshot < start then RangeResult.Before
    end match {
      case i: Infinity.type =>
        RangeResult.Inside
      case e: Long =>
        if snapshot <= e then RangeResult.Inside else RangeResult.After
    }
  override def toString = s"$start ... $end"
}

object SegmentRange:
  enum RangeResult:
    case Inside, Before, After

  case object Infinity {
    override def toString(): String = "Inf"
  }
  def apply(start: Long): SegmentRange = new SegmentRange(start, Infinity)
