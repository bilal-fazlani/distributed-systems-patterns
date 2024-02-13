package com.bilalfazlani.logSnapshots
package log

import zio.json.*
import zio.*
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
      dataDiscardService <- ZIO.service[DataDiscardService]
      lowWaterMarkService <- ZIO.service[LowWaterMarkService]
      service = SnapshotServiceImpl[St](
        lowWaterMarkService,
        dataDiscardService,
        sem,
        state,
        pointer
      )
      schedule = config.snapshotFrequency match
        case SnapshotFrequency.Off        => Schedule.stop
        case SnapshotFrequency.Every(dur) => Schedule.fixed(dur)
      started <- (service.createSnapshot repeat schedule).forkScoped.unit
    } yield started)

case class SnapshotServiceImpl[St: JsonCodec](
    lowWaterMarkService: LowWaterMarkService,
    dataDiscardService: DataDiscardService,
    sem: Semaphore,
    state: State[St],
    pointer: Pointer
) extends SnapshotService:

  private def stateOffset: ZIO[Any, Nothing, (St, Point)] = sem.withPermit(for {
    state <- state.all
    point <- pointer.get
  } yield (state, point))

  private[log] def createSnapshot: Task[Unit] =
    ZIO.logDebug(s"createSnapshot triggered...") *>
      // create new snapshot and update low water mark atomically
      lowWaterMarkService.change { (oldLwm: Option[Long]) =>
        for {
          config <- ZIO.config(LogConfiguration.config)
          tuple <- stateOffset
          (state, point) = tuple
          newLwm <- (oldLwm, point) match {
            // there is no data to snapshot
            case (_, Point.Empty) => ZIO.succeed(oldLwm)
            // there was an old snapshot but no new data after it
            case (Some(x: Long), p @ Point.NonEmpty(index, segment)) if p.totalIndex == x =>
              ZIO.succeed(oldLwm)
            // there is new data to snapshot
            case (_, p @ Point.NonEmpty(index, segment)) =>
              for
                _ <- ZIO.logDebug(s"create snapshot triggered for offset ${p.totalIndex}")
                path = config.dir / s"snapshot-${p.totalIndex}.json"
                tempPath = config.dir / s"snapshot-${p.totalIndex}.json.tmp"
                newLwm <- // create new snapshot
                ZIO.scoped {
                  (newFile(tempPath, state.toJson) *> moveFile(tempPath, path)).withFinalizerExit {
                    case (_, Exit.Failure(_)) =>
                      Files
                        .deleteIfExists(tempPath)
                        .catchAll(_ => ZIO.logError("Error deleting temp file"))
                    case (_, _) => ZIO.logInfo(s"snapshot created at $path")
                  } as p.totalIndex
                } <* dataDiscardService.discard.fork // delete old snapshots and segments in background
              yield Some(newLwm)
          }
        } yield newLwm
      }
