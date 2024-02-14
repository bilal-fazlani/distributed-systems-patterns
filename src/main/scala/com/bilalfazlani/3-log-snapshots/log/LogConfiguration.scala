package com.bilalfazlani.logSnapshots
package log

import zio.*
import zio.nio.file.Path

enum SnapshotFrequency:
  case Off
  case Every(value: Duration)

object SnapshotFrequency:
  val config: Config[SnapshotFrequency] = Config
    .string("snapshotFrequency")
    .mapAttempt {
      case x if x.toLowerCase == "off" => SnapshotFrequency.Off
    }
    .orElse(Config.duration("snapshotFrequency").map(SnapshotFrequency.Every(_)))

case class LogConfiguration(
    segmentSize: Long,
    dir: Path,
    snapshotFrequency: SnapshotFrequency,
    logLevel: LogLevel
)

object LogConfiguration:
  def config: Config[LogConfiguration] =
    val pathConfig = Config.string("dir").map(Path(_))
    val segmentSizeConfig = Config.long("segmentSize")
    val snapshotFrequencyConfig = SnapshotFrequency.config
    val logLevelConfig: Config[LogLevel] =
      Config.string("logLevel").mapOrFail {
        case x if LogLevel.levels.exists(_.label.toLowerCase == x.toLowerCase) =>
          Right(LogLevel.levels.find(_.label.equalsIgnoreCase(x)).get)
        case x => Left(Config.Error.InvalidData(Chunk("logLevel"), s"Invalid log level $x"))
      }
    (segmentSizeConfig ++ pathConfig ++ snapshotFrequencyConfig ++ logLevelConfig).map {
      case (segmentSize, dir, snapshotFrequency, logLevel) =>
        LogConfiguration(segmentSize, dir, snapshotFrequency, logLevel)
    }
