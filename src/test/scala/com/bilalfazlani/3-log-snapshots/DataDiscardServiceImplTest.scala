package com.bilalfazlani.logSnapshots
package log

import zio.test.*

object DataDiscardServiceImplTest extends ZIOSpecDefault {
  val spec = suite("DataDiscardServiceImplTest")(
    test("first segment with middle value") {
      val segments = Array(10L, 13L, 16L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 11, 3L)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("first segment with greater value") {
      val segments = Array(10L, 13L, 16L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 12, 3L)
      assertTrue(segmentsToBeDiscarded == List(10))
    },
    test("middle segment with greater value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 14, 3)
      assertTrue(segmentsToBeDiscarded == List(10L))
    },
    test("middle segment with equal value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 16, 3)
      assertTrue(segmentsToBeDiscarded == List(10L, 13L))
    },
    test("last segment with equal value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 19, 3)
      assertTrue(segmentsToBeDiscarded == List(10L, 13L, 16L))
    },
    test("all non-snapshotted segments") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 5, 3L)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("last segment in progress") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 20, 3L)
      assertTrue(segmentsToBeDiscarded == List(10L, 13L, 16L))
    },
    test("no segments") {
      val segments = Array.empty[Long]
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 20, 10L)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("one segment less than snapshot") {
      val segments = Array(10L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 20, 5)
      assertTrue(segmentsToBeDiscarded == List(10L))
    },
    test("one segment containing snapshot") {
      val segments = Array(10L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 12, 5)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("one segment greater than snapshot") {
      val segments = Array(30L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 20, 5)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("one segment equal to snapshot") {
      val segments = Array(20L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 20, 5)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("single zeroth segment") {
      val segments = Array(0L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 0, 5)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("single zeroth segment-2") {
      val segments = Array(0L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 2, 5)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("middle segment with end value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 18, 3)
      assertTrue(segmentsToBeDiscarded == List(10L, 13L, 16L))
    },
    test("last segment with end value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(segments, 21, 3)
      assertTrue(segmentsToBeDiscarded == List(10L, 13L, 16L, 19L))
    }
  )
}
