package com.bilalfazlani.logSnapshots
package log

import zio.Duration
import zio.nio.file.Path
import zio.Config

case class LogConfiguration(
    segmentSize: Long,
    dir: Path,
    snapshotFrequency: Duration
)

object LogConfiguration:
  given config: Config[LogConfiguration] =
    val pathConfig = Config.string("dir").map(Path(_))
    val segmentSizeConfig = Config.long("segmentSize")
    val snapshotFrequencyConfig = Config.duration("snapshotFrequency")
    (segmentSizeConfig ++ pathConfig ++ snapshotFrequencyConfig).map {
      case (segmentSize, dir, snapshotFrequency) =>
        LogConfiguration(segmentSize, dir, snapshotFrequency)
    }
