package com.bilalfazlani.logSnapshots
package log

import zio.test.*

object LowWaterMarkServiceTest extends ZIOSpecDefault {
  val spec = suite("LowWaterMarkService")(
    test("first active segment with greater value") {
      val segments = Array(10L, 13L, 16L)
      val committedSegments = LowWaterMarkServiceImpl.committedSegments(segments, 12)
      assertTrue(committedSegments == Nil)
    },
    test("middle segment with equal value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val committedSegments = LowWaterMarkServiceImpl.committedSegments(segments, 14)
      assertTrue(committedSegments == List(10L))
    },
    test("middle segment with equal value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val committedSegments = LowWaterMarkServiceImpl.committedSegments(segments, 16)
      assertTrue(committedSegments == List(10L, 13L))
    },
    test("last segment with equal value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val committedSegments = LowWaterMarkServiceImpl.committedSegments(segments, 19)
      assertTrue(committedSegments == List(10L, 13L, 16L))
    },
    test("all uncommitted segments") {
      val segments = Array(10L, 13L, 16L, 19L)
      val committedSegments = LowWaterMarkServiceImpl.committedSegments(segments, 5)
      assertTrue(committedSegments == Nil)
    },
    test("last active segment in progress") {
      val segments = Array(10L, 13L, 16L, 19L)
      val committedSegments = LowWaterMarkServiceImpl.committedSegments(segments, 20)
      assertTrue(committedSegments == List(10L, 13L, 16L))
    },
    test("no segments") {
      val segments = Array.empty[Long]
      val committedSegments = LowWaterMarkServiceImpl.committedSegments(segments, 20)
      assertTrue(committedSegments == Nil)
    },
    test("one segment less than snapshot") {
      val segments = Array(10L)
      val committedSegments = LowWaterMarkServiceImpl.committedSegments(segments, 20)
      assertTrue(committedSegments == Nil)
    },
    test("one segment greater than snapshot") {
      val segments = Array(30L)
      val committedSegments = LowWaterMarkServiceImpl.committedSegments(segments, 20)
      assertTrue(committedSegments == Nil)
    },
    test("one segment equal to snapshot") {
      val segments = Array(20L)
      val committedSegments = LowWaterMarkServiceImpl.committedSegments(segments, 20)
      assertTrue(committedSegments == Nil)
    }
  )
}
