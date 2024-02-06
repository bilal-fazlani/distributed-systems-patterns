package com.bilalfazlani.logSnapshots
package log

import zio.json.*
import zio.*
import zio.nio.file.Path
import zio.nio.file.Files
import com.bilalfazlani.*

trait SnapshotService

object SnapshotService:
  def start[St: JsonCodec: Tag] =
    ZLayer(for {
      config <- ZIO.config[LogConfiguration](LogConfiguration.config)
      sem <- ZIO.service[Semaphore]
      state <- ZIO.service[State[St]]
      pointer <- ZIO.service[Pointer]
      lowWaterMarkService <- ZIO.service[LowWaterMarkService]
      service = SnapshotServiceImpl[St](lowWaterMarkService, sem, state, pointer)
      schedule = Schedule.fixed(config.snapshotFrequency)
      started <- (service.createSnapshot repeat schedule).forkScoped.unit
    } yield started)

case class SnapshotServiceImpl[St: JsonCodec](
    lowWaterMarkService: LowWaterMarkService,
    sem: Semaphore,
    state: State[St],
    pointer: Pointer
) extends SnapshotService:

  private def stateOffset: ZIO[Any, Nothing, (St, Long)] = ZIO.scoped(sem.withPermit(for {
    state <- state.get
    totalOffset <- pointer.totalIndex
  } yield (state, totalOffset)))

  private[log] def createSnapshot: Task[Unit] =
    for {
      config <- ZIO.config(LogConfiguration.config)
      tuple <- stateOffset
      (state, offset) = tuple
      path = config.dir / s"snapshot-$offset.json"
      tempPath = config.dir / s"snapshot-$offset.json.tmp"

      // create new snapshot and update low water mark atomically
      _ <- lowWaterMarkService.change { oldLwm =>
        for {
          newOffset <- ZIO.when(oldLwm != offset) {
            ZIO.scoped {
              (newFile(tempPath, state.toJson) *> moveFile(tempPath, path)).withFinalizerExit {
                case (_, Exit.Failure(_)) =>
                  Files
                    .deleteIfExists(tempPath)
                    .catchAll(_ => ZIO.logError("Error deleting temp file"))
                case (_, _) => ZIO.unit
              } as offset
            }
          }
        } yield newOffset.getOrElse(oldLwm)
      }
      lwm <- lowWaterMarkService.lowWaterMark
      path = config.dir / s"snapshot-$offset.json"
      tempPath = config.dir / s"snapshot-$offset.json.tmp"
      _ <- ZIO.when(lwm != offset) {
        for {
          // create new snapshot
          _ <- ZIO.scoped {
            (newFile(tempPath, state.toJson) *> moveFile(tempPath, path)).withFinalizerExit {
              case (_, Exit.Failure(_)) =>
                Files
                  .deleteIfExists(tempPath)
                  .catchAll(_ => ZIO.logError("Error deleting temp file"))
              case (_, _) => ZIO.unit
            }
          }
          previousSnapshots <- findFiles(config.dir).runCollect
            .map(_.collect(_.filename.toString match {
              case s"snapshot-${Long(offset)}.json"   => Some(offset)
              case s"snapshot-${Long(`offset`)}.json" => None
            }).flatten)
          // delete previous snapshots
          _ <- ZIO
            .foreachPar(previousSnapshots)(ofst =>
              Files.deleteIfExists(config.dir / s"snapshot-$ofst.json")
            )
            .unit
            .catchAll(e => ZIO.logError(s"Error deleting previous snapshots: $e"))
          // get all segments
          allSegments <- findFiles(config.dir).runCollect
            .map(_.collect(_.filename.toString match {
              case s"segment-${Long(ofst)}.json" if ofst < offset => ofst
            }))
            .map(_.toArray.sorted)
          // get snapshotted segments
          segmentsToBeDiscarded = SnapshotServiceImpl.segmentsToBeDiscarded(allSegments, offset)
          // delete all segments that are snapshotted
          _ <- ZIO
            .foreachPar(segmentsToBeDiscarded)(ofst =>
              Files.deleteIfExists(config.dir / s"segment-$ofst.json")
            )
            .unit
            .catchAll(e => ZIO.logError(s"Error deleting previous segments: $e"))
        } yield ()
      }
    } yield ()

object SnapshotServiceImpl:
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

case class Snapshot[State](state: State, offset: Long)
