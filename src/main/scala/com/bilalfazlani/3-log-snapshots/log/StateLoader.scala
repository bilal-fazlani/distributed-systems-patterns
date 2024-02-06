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

  private[log] def activeSegments(segments: List[Long], snapshot: Long): List[Long] =
    import SegmentRange.RangeResult
    val activeSegments = scala.collection.mutable.ListBuffer[Long]()
    var i = 0

    while (i < segments.length) do {
      val isLast = i == (segments.length - 1)
      val range =
        if isLast then SegmentRange(segments(i)) else SegmentRange(segments(i), segments(i + 1) - 1)

      range.contains(snapshot) match
        case RangeResult.After                       =>
        case RangeResult.Inside | RangeResult.Before => activeSegments += segments(i)

      i += 1
    }

    return activeSegments.toList

case class StateLoaderImpl[Item: JsonCodec, State: JsonCodec](
    stateComputer: StateComputer[Item, State],
    pointerRef: Ref[Pointer],
    lowWaterMarkService: LowWaterMarkService
) extends StateLoader[State]:

  case class Record(item: Item, offset: Long)

  private def loadSnapshot(lwm: Long, dir: Path): ZIO[Any, Throwable, State] =
    if lwm == 0 then ZIO.succeed(stateComputer.zero)
    else
      val file = (dir / s"snapshot-$lwm.json").toFile
      val inputStream = ZStream.fromFile(file)
      JsonDecoder[State].decodeJsonStreamInput(inputStream)

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
      lwm: Long,
      segments: List[Long]
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
        .dropWhile(_.offset <= lwm)

  def load: ZIO[Any, Throwable, State] =
    for
      dir <- ZIO.config(LogConfiguration.config).map(_.dir)
      lwm <- lowWaterMarkService.lowWaterMark
      avSegments <- availableSegments(dir)
      actSegments = StateLoader.activeSegments(avSegments, lwm)
      stream = convertToStream(dir, lwm, actSegments)
      zeroState <- loadSnapshot(lwm, dir)
      finalState <- computeFinalState(zeroState, stream)
    yield finalState
