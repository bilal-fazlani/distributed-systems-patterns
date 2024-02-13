package com.bilalfazlani.logSnapshots
package log

import zio.test.*

object PointTest extends ZIOSpecDefault {
  val spec = suite("PointTests")(
    test("incrementing a new pointer") {
      val point = Point.Empty
      val newPoint = point.inc(segmentSize = 3)
      assertTrue(newPoint == Point.NonEmpty(0, Segment(0))) &&
      assertTrue(newPoint.totalIndex == 0)
    },
    test("incrementing an existing pointer in same segment") {
      val point = Point.NonEmpty(index = 0, segment = Segment(0))
      val newPoint = point.inc(segmentSize = 3)
      assertTrue(newPoint == Point.NonEmpty(1, Segment(0))) &&
      assertTrue(newPoint.totalIndex == 1)
    },
    test("incrementing an existing pointer to a new segment") {
      val point = Point.NonEmpty(index = 2, segment = Segment(0))
      val newPoint = point.inc(segmentSize = 3)
      assertTrue(newPoint == Point.NonEmpty(0, Segment(3))) &&
      assertTrue(newPoint.totalIndex == 3)
    },
    test("increase segment") {
      val point = Point.NonEmpty(index = 2, segment = Segment(3))
      val newPoint = point.inc(segmentSize = 3)
      assertTrue(newPoint == Point.NonEmpty(0, Segment(6))) &&
      assertTrue(newPoint.totalIndex == 6)
    }
  )
}
