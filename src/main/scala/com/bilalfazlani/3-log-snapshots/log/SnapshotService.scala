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
      schedule = Schedule.fixed(config.snapshotFrequency)
      started <- (service.createSnapshot repeat schedule).forkScoped.unit
    } yield started)

case class SnapshotServiceImpl[St: JsonCodec](
    lowWaterMarkService: LowWaterMarkService,
    dataDiscardService: DataDiscardService,
    sem: Semaphore,
    state: State[St],
    pointer: Pointer
) extends SnapshotService:

  private def stateOffset: ZIO[Any, Nothing, (St, Long)] = ZIO.scoped(sem.withPermit(for {
    state <- state.all
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
        ZIO
          .when(oldLwm != offset) {
            // create new snapshot
            ZIO.scoped {
              (newFile(tempPath, state.toJson) *> moveFile(tempPath, path)).withFinalizerExit {
                case (_, Exit.Failure(_)) =>
                  Files
                    .deleteIfExists(tempPath)
                    .catchAll(_ => ZIO.logError("Error deleting temp file"))
                case (_, _) => ZIO.unit
              } as offset
            } <* dataDiscardService.discard.fork // delete old snapshots and segments in background
          }
          .map(_.getOrElse(oldLwm))
      }
    } yield ()
