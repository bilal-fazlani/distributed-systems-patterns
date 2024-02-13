package com.bilalfazlani.logSnapshots
package log

import zio.*
import com.bilalfazlani.*
import zio.nio.file.Files

trait DataDiscardService:
  def discard: Task[Unit]

object DataDiscardService:
  def live: ZLayer[LowWaterMarkService, Nothing, DataDiscardService] =
    ZLayer.fromFunction(DataDiscardServiceImpl.apply)

case class DataDiscardServiceImpl(lowWaterMarkService: LowWaterMarkService)
    extends DataDiscardService:
  def discard: Task[Unit] =
    for {
      config <- ZIO.config(LogConfiguration.config)
      offset: Option[Long] <- lowWaterMarkService.lowWaterMark
      _ <- offset.fold(ZIO.unit) { offset =>
        for
          _ <- ZIO.logDebug(s"discard called for offset $offset")
          // find any previous snapshots
          previousSnapshots <- findFiles(config.dir).runCollect
            .map(_.collect(_.filename.toString match {
              case s"snapshot-${Long(ofs)}.json" if ofs != offset => Some(ofs)
              case s"snapshot-${Long(`offset`)}.json"             => None
            }).flatten)
          _ <- ZIO.logDebug(s"previous snapshots to be discarded: $previousSnapshots")
          // delete any previous snapshots
          _ <- ZIO
            .foreachPar(previousSnapshots)(ofst =>
              Files.deleteIfExists(config.dir / s"snapshot-$ofst.json")
            )
            .unit
            .catchAll(e => ZIO.logError(s"Error deleting previous snapshots: $e"))
          _ <- ZIO.when(previousSnapshots.nonEmpty)(ZIO.logInfo(s"discarded previous snapshots"))
          // get all segments
          allSegments <- findFiles(config.dir).runCollect
            .map(_.collect(_.filename.toString match {
              case s"log-${Long(ofst)}.json" => ofst
            }))
            .map(_.toArray.sorted)
          segmentsToBeDiscarded = DataDiscardServiceImpl.segmentsToBeDiscarded(allSegments, offset)
          _ <- ZIO.logDebug(s"segments to be discarded: $segmentsToBeDiscarded")
          _ <- ZIO
            .foreachPar(segmentsToBeDiscarded)(ofst =>
              Files.deleteIfExists(config.dir / s"segment-$ofst.json")
            )
            .unit
            .catchAll(e => ZIO.logError(s"Error deleting previous segments: $e"))
          _ <- ZIO.when(segmentsToBeDiscarded.nonEmpty)(ZIO.logInfo(s"discarded segments"))
        yield ()
      }
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
      snapshot: Long
  ): List[Long] =
    import SegmentRange.RangeResult
    val commiittedSegments = scala.collection.mutable.ListBuffer[Long]()
    var i = 0
    while (i < segments.length) do {
      val isLast = i == (segments.length - 1)
      val range =
        if isLast then SegmentRange(segments(i)) else SegmentRange(segments(i), segments(i + 1) - 1)

      range.contains(snapshot) match
        case RangeResult.After                       => commiittedSegments += segments(i)
        case RangeResult.Inside | RangeResult.Before => return commiittedSegments.toList

      i += 1
    }
    return commiittedSegments.toList
