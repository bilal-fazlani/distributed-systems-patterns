package com.bilalfazlani.logSnapshots
package log

import zio.*
import com.bilalfazlani.*
import zio.json.*
import zio.stream.ZStream
import zio.nio.file.Path

trait StateLoader[State]:
  def load: ZIO[Scope, Throwable, State]

object StateLoader:
  def live[LogItem: JsonCodec: Tag, State: Tag: JsonCodec] =
    ZLayer.fromFunction(StateLoaderImpl.apply[LogItem, State])

  private[log] def activeSegments(
      segments: List[Long],
      snapshot: Option[Long],
      segmentSize: Long
  ): List[Long] =
    snapshot.fold(segments) { snapshot =>
      val ranges = SegmentRange.fromSegmentList(segments, segmentSize)
      import SegmentRange.RangeResult

      ranges
        .zip(segments)
        .map { case (range @ SegmentRange(start, end), seg) =>
          range.contains(snapshot) match
            case RangeResult.Before | RangeResult.Inside => Some(seg)
            case RangeResult.End | RangeResult.After     => None
        }
        .flatten
    }

case class StateLoaderImpl[Item: JsonCodec, State: JsonCodec](
    stateComputer: StateComputer[Item, State],
    lowWaterMarkService: LowWaterMarkService
) extends StateLoader[State]:

  case class Record(item: Item, offset: Long)

  private def loadSnapshot(lwm: Option[Long], dir: Path): ZIO[Any, Throwable, State] =
    lwm.fold(ZIO.succeed(stateComputer.zero)) { lwm =>
      if lwm == 0 then ZIO.succeed(stateComputer.zero)
      else
        val file = (dir / s"snapshot-$lwm.json").toFile
        val inputStream = ZStream.fromFile(file)
        JsonDecoder[State].decodeJsonStreamInput(inputStream)
    }

  def availableSegments(dir: Path) = findFiles(dir).runCollect.map(
    _.map(_.filename.toString)
      .collect { case s"log-${Long(offset)}.txt" =>
        offset
      }
      .sorted
      .toList
  )

  def computeFinalState(zero: State, stream: ZStream[Any, Throwable, Record]): Task[State] =
    stream.runFold(zero)((state, record) => stateComputer.compute(state, record.item))

  def convertToStream(
      dir: Path,
      segments: List[Long],
      lwm: Option[Long]
  ): ZStream[Any, Throwable, Record] =
    if segments.isEmpty then ZStream.empty
    else
      val startingSegment = segments.head
      def segmentStream(segment: Long) =
        val path = dir / s"log-$segment.txt"
        streamLines(path)
          .map(line => JsonDecoder[Item].decodeJson(line).left.map(e => Throwable(e)))
          .absolve

      segments
        .map(segmentStream)
        .reduce(_ ++ _)
        .mapAccum(startingSegment)((index, a) => (index + 1, (a, index)))
        .map(Record.apply)
        .dropWhile(x => lwm.fold(false)(lwm => x.offset <= lwm))

  def load: ZIO[Any, Throwable, State] =
    for
      config <- ZIO.config(LogConfiguration.config)
      lwm <- lowWaterMarkService.lowWaterMark
      avSegments <- availableSegments(config.dir)
      actSegments = StateLoader.activeSegments(avSegments, lwm, config.segmentSize)
      stream = convertToStream(config.dir, actSegments, lwm)
      zeroState <- loadSnapshot(lwm, config.dir)
      finalState <- computeFinalState(zeroState, stream)
    yield finalState
