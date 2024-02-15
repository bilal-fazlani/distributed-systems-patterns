package com.bilalfazlani.logSnapshots
package log

import zio.*
import com.bilalfazlani.*
import zio.nio.file.Files

trait DataDiscardService:
  def discard(lwm: Long): Task[Unit]

object DataDiscardService:
  def live: ZLayer[Hub[Event], Nothing, DataDiscardService] =
    ZLayer.fromFunction(DataDiscardServiceImpl.apply)

case class DataDiscardServiceImpl(
    eventHub: Hub[Event]
) extends DataDiscardService:
  def discard(lwm: Long): Task[Unit] =
    for {
      config <- ZIO.config(LogConfiguration.config)
      // find any previous snapshots
      previousSnapshots <- findFiles(config.dir).runCollect
        .map(_.collect(_.filename.toString match {
          case s"snapshot-${Long(ofs)}.json" if ofs != lwm => Some(ofs)
          case s"snapshot-${Long(`lwm`)}.json"             => None
        }).flatten)
      _ <-
        ZIO.when(previousSnapshots.nonEmpty)(
          ZIO.logDebug(s"previous snapshots to be discarded: $previousSnapshots")
        )
      // delete any previous snapshots
      _ <- ZIO
        .foreachPar(previousSnapshots)(ofst =>
          Files.deleteIfExists(config.dir / s"snapshot-$ofst.json")
        )
        .unit
        .catchAll(e => ZIO.logError(s"Error deleting previous snapshots: $e"))
      _ <- ZIO.when(previousSnapshots.nonEmpty)(
        ZIO.logInfo(
          s"discarded previous snapshots: ${previousSnapshots.toList
              .map(x => (s"snapshot-$x.json").toString)
              .mkString(", ")}"
        ) *> eventHub.publish(
          Event.DateDiscarded(
            previousSnapshots.toList.map(x => (config.dir / s"snapshot-$x.json").toString)
          )
        )
      )
      // get all segments
      allSegments <- findFiles(config.dir).runCollect
        .map(_.collect(_.filename.toString match {
          case s"log-${Long(ofst)}.txt" => ofst
        }))
        .map(_.toArray.sorted)
      segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(
        allSegments,
        lwm,
        config.segmentSize
      )
      _ <- ZIO.when(segmentsToBeDiscarded.nonEmpty)(
        ZIO.logDebug(s"segments to be discarded: $segmentsToBeDiscarded")
      )
      _ <- ZIO
        .foreachPar(segmentsToBeDiscarded)(ofst =>
          Files.deleteIfExists(config.dir / s"log-$ofst.txt")
        )
        .unit
        .catchAll(e => ZIO.logError(s"Error deleting previous segments: $e"))
      _ <- ZIO.when(segmentsToBeDiscarded.nonEmpty)(
        ZIO.logInfo(
          s"discarded segments: ${segmentsToBeDiscarded.map(x => (s"log-$x.txt").toString).mkString(", ")}"
        ) *> eventHub.publish(
          Event.DateDiscarded(
            segmentsToBeDiscarded.map(x => (config.dir / s"log-$x.txt").toString)
          )
        )
      )
    } yield ()

object DataDiscardServiceImpl:
  /** takes sorted list of segments and and offset to find segments that are already committed
    * @param segments
    * @param offset
    * @return
    *   active segments
    */
  private[log] def segmentsToBeDiscarded(
      segments: Array[Long],
      lwm: Long,
      segmentSize: Long
  ): List[Long] =
    import SegmentRange.RangeResult
    val toBeDiscarded = scala.collection.mutable.ListBuffer[Long]()
    var i = 0
    while (i < segments.length) do {
      val isLast = i == (segments.length - 1)
      val range =
        if isLast then SegmentRange(segments(i), segments(i) + segmentSize - 1)
        else SegmentRange(segments(i), segments(i + 1) - 1)

      range.contains(lwm) match
        case RangeResult.After | RangeResult.End     => toBeDiscarded += segments(i)
        case RangeResult.Inside | RangeResult.Before => return toBeDiscarded.toList

      i += 1
    }
    return toBeDiscarded.toList
