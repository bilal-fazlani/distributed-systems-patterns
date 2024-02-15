package com.bilalfazlani.logSnapshots
package log

import zio.test.*

object StateLoaderTest extends ZIOSpecDefault {
  val spec = suite("StateLoaderTest")(
    test("snapshot in middle of first segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = Some(11L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(10L, 13L, 16L))
    },
    test("snapshot in the end of first segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = Some(12L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(13L, 16L))
    },
    test("snapshot in middle of second segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = Some(14L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(13L, 16L))
    },
    test("snapshot in middle of last segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = Some(17L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(16L))
    },
    test("snapshot at the end of last segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = Some(18L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == Nil)
    },
    test("snapshot beyond last segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = Some(22L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == Nil)
    },
    test("snapshot as first element") {
      val segments = List(10L, 13L, 16L)
      val snapshot = Some(10L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(10L, 13L, 16L))
    },
    test("snapshot as second element") {
      val segments = List(10L, 13L, 16L)
      val snapshot = Some(13L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(13L, 16L))
    },
    test("snapshot as last element") {
      val segments = List(10L, 13L, 16L)
      val snapshot = Some(16L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(16L))
    },
    test("segments are empty") {
      val segments = List.empty[Long]
      val snapshot = Some(10L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == Nil)
    },
    test("snapshot is less than first segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = Some(5L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(10L, 13L, 16L))
    },
    test("single element segment less than snapshot") {
      val segments = List(10L)
      val snapshot = Some(20L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == Nil)
    },
    test("single element segment greater than snapshot") {
      val segments = List(30L)
      val snapshot = Some(20L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(30L))
    },
    test("single element segment equal to snapshot") {
      val segments = List(20L)
      val snapshot = Some(20L)
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(20L))
    },
    test("snapshot is None") {
      val segments = List(10L, 13L, 16L)
      val snapshot = None
      val activeSegments = StateLoader.activeSegments(segments, snapshot, 3)
      assertTrue(activeSegments == List(10L, 13L, 16L))
    }
  )
}
