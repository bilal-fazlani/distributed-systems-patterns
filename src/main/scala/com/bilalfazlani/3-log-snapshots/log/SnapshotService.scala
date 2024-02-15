package com.bilalfazlani.logSnapshots
package log

import zio.json.*
import zio.*
import com.bilalfazlani.*

trait SnapshotService:
  def createSnapshot: Task[Unit]

object SnapshotService:
  def start[St: JsonCodec: Tag] =
    ZLayer(for {
      config <- ZIO.config[LogConfiguration](LogConfiguration.config)
      sem <- ZIO.service[Semaphore]
      state <- ZIO.service[State[St]]
      pointer <- ZIO.service[Pointer]
      dataDiscardService <- ZIO.service[DataDiscardService]
      lowWaterMarkService <- ZIO.service[LowWaterMarkService]
      service: SnapshotService = SnapshotServiceImpl[St](
        lowWaterMarkService,
        dataDiscardService,
        sem,
        state,
        pointer
      )
      _ <- config.snapshotFrequency.match
        case SnapshotFrequency.Off => ZIO.unit
        case SnapshotFrequency.Every(dur) =>
          val schedule = Schedule.windowed(dur)
          (ZIO.sleep(dur) *> (service.createSnapshot repeat schedule)).forkScoped.unit
    } yield service)

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

  def createSnapshot: Task[Unit] =
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
              _ <- ZIO.logDebug(s"create snapshot triggered at offset ${p.totalIndex}")
              path = config.dir / s"snapshot-${p.totalIndex}.json"
              tempPath = config.dir / s"snapshot-${p.totalIndex}.json.tmp"
              _ <- newFile(tempPath, state.toJson) // create new temp snapshot
              _ <- moveFile(tempPath, path) // move temp snapshot to final location
              _ <- ZIO.logInfo(s"snapshot created at offset ${p.totalIndex}")
              _ <- dataDiscardService.discard(p.totalIndex) // delete old snapshots and segments
            yield Some(p.totalIndex)
        }
      } yield newLwm
    }
