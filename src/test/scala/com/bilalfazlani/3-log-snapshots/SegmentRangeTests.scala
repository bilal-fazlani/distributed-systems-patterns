package com.bilalfazlani.logSnapshots
package log

import zio.test.*
import SegmentRange.*

object SegmentRangeTests extends ZIOSpecDefault:
  val spec = suite("SegmentRangeTests")(
    test("lwm before range") {
      val range = SegmentRange(5, 10)
      val result = range.contains(4)
      assertTrue(result == RangeResult.Before)
    },
    test("lwm inside range") {
      val range = SegmentRange(5, 10)
      val result = range.contains(7)
      assertTrue(result == RangeResult.Inside)
    },
    test("lwm at end of range") {
      val range = SegmentRange(5, 10)
      val result = range.contains(10)
      assertTrue(result == RangeResult.End)
    },
    test("lwm after range") {
      val range = SegmentRange(5, 10)
      val result = range.contains(11)
      assertTrue(result == RangeResult.After)
    },
    test("lwm at infinity") {
      val range = SegmentRange(5, Infinity)
      val result = range.contains(11)
      assertTrue(result == RangeResult.Inside)
    }
  )
