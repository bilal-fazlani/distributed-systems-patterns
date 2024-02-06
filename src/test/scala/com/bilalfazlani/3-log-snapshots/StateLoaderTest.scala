package com.bilalfazlani.logSnapshots
package log

import zio.test.*

object StateLoaderTest extends ZIOSpecDefault {
  val spec = suite("StateLoaderTest")(
    test("snapshot in middle of first segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = 12L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == List(10L, 13L, 16L))
    },
    test("snapshot in middle of second segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = 14L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == List(13L, 16L))
    },
    test("snapshot in middle of last segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = 20L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == List(16L))
    },
    test("snapshot as first element") {
      val segments = List(10L, 13L, 16L)
      val snapshot = 10L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == List(10L, 13L, 16L))
    },
    test("snapshot as second element") {
      val segments = List(10L, 13L, 16L)
      val snapshot = 13L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == List(13L, 16L))
    },
    test("snapshot as last element") {
      val segments = List(10L, 13L, 16L)
      val snapshot = 16L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == List(16L))
    },
    test("segments are empty") {
      val segments = List.empty[Long]
      val snapshot = 10L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == Nil)
    },
    test("snapshot is less than first segment") {
      val segments = List(10L, 13L, 16L)
      val snapshot = 5L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == List(10L, 13L, 16L))
    },
    test("single element segment less than snapshot") {
      val segments = List(10L)
      val snapshot = 20L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == List(10L))
    },
    test("single element segment greater than snapshot") {
      val segments = List(30L)
      val snapshot = 20L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == List(30L))
    },
    test("single element segment equal to snapshot") {
      val segments = List(20L)
      val snapshot = 20L
      val activeSegments = StateLoader.activeSegments(segments, snapshot)
      assertTrue(activeSegments == List(20L))
    }
  )
}
