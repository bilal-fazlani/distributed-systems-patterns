package com.bilalfazlani.logSnapshots
package log

import zio.Duration
import zio.nio.file.Path
import zio.Config

enum SnapshotFrequency:
  case Off
  case Every(value: Duration)

object SnapshotFrequency:
  given config: Config[SnapshotFrequency] = Config
    .string("snapshotFrequency")
    .mapAttempt {
      case x if x.toLowerCase == "off" => SnapshotFrequency.Off
    }
    .orElse(Config.duration("snapshotFrequency").map(SnapshotFrequency.Every(_)))

case class LogConfiguration(
    segmentSize: Long,
    dir: Path,
    snapshotFrequency: SnapshotFrequency
)

object LogConfiguration:
  given config: Config[LogConfiguration] =
    val pathConfig = Config.string("dir").map(Path(_))
    val segmentSizeConfig = Config.long("segmentSize")
    val snapshotFrequencyConfig = SnapshotFrequency.config
    (segmentSizeConfig ++ pathConfig ++ snapshotFrequencyConfig).map {
      case (segmentSize, dir, snapshotFrequency) =>
        LogConfiguration(segmentSize, dir, snapshotFrequency)
    }
