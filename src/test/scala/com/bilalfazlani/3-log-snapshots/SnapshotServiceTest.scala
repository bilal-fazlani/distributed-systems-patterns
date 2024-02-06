package com.bilalfazlani.logSnapshots
package log

import zio.test.*

object SnapshotServiceTest extends ZIOSpecDefault {
  val spec = suite("SnapshotServiceTest")(
    test("first segment with greater value") {
      val segments = Array(10L, 13L, 16L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 12)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("middle segment with greater value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 14)
      assertTrue(segmentsToBeDiscarded == List(10L))
    },
    test("middle segment with equal value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 16)
      assertTrue(segmentsToBeDiscarded == List(10L, 13L))
    },
    test("last segment with equal value") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 19)
      assertTrue(segmentsToBeDiscarded == List(10L, 13L, 16L))
    },
    test("all non-snapshotted segments") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 5)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("last segment in progress") {
      val segments = Array(10L, 13L, 16L, 19L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 20)
      assertTrue(segmentsToBeDiscarded == List(10L, 13L, 16L))
    },
    test("no segments") {
      val segments = Array.empty[Long]
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 20)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("one segment less than snapshot") {
      val segments = Array(10L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 20)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("one segment greater than snapshot") {
      val segments = Array(30L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 20)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("one segment equal to snapshot") {
      val segments = Array(20L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 20)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("single zeroth segment") {
      val segments = Array(0L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 0)
      assertTrue(segmentsToBeDiscarded == Nil)
    },
    test("single zeroth segment-2") {
      val segments = Array(0L)
      val segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(segments, 2)
      assertTrue(segmentsToBeDiscarded == Nil)
    }
  )
}
