package com.bilalfazlani.logSnapshots
package log

import zio.json.*
import zio.*
import zio.nio.file.Path
import zio.nio.file.Files
import zio.stream.ZStream
import com.bilalfazlani.*

trait LowWaterMarkService[State: JsonCodec]:
  def createSnapshot: Task[Unit]
  def read: Task[Snapshot[State]]
  def lowWaterMark: Task[Long]
  def start: Task[Unit]

object LowWaterMarkService:
  def live[St: JsonCodec: Tag, Item: Tag](
      path: Path
  ): ZLayer[
    Ref[Pointer] & Semaphore & State[St] & StateComputer[Item, St],
    Nothing,
    LowWaterMarkServiceImpl[St, Item]
  ] =
    ZLayer.fromFunction(LowWaterMarkServiceImpl[St, Item](path, _, _, _, _))

extension (l: Long.type) private[this] def unapply(s: String): Option[Long] = s.toLongOption

class LowWaterMarkServiceImpl[St: JsonCodec, Item](
    dir: Path,
    pointer: Ref[Pointer],
    semaphore: Semaphore,
    state: State[St],
    stateComputer: StateComputer[Item, St]
) extends LowWaterMarkService:

  private def stateOffset = semaphore.withPermit(for {
    state <- state.get
    offset <- pointer.get.map(_.totalIndex)
  } yield (state, offset))

  def createSnapshot: Task[Unit] =
    for {
      tuple <- stateOffset
      (state, offset) = tuple
      lwm <- lowWaterMark
      path = dir / s"snapshot-$offset.json"
      _ <- ZIO.when(lwm != offset) {
        for {
          existingSnapShots <- findFiles(dir).runCollect.map(_.collect(_.filename.toString match {
            case s"snapshot-${Long(offset)}.json"   => Some(offset)
            case s"snapshot-${Long(`offset`)}.json" => None
          }).flatten)
          // create new snapshot
          _ <- newFile(path, state.toJson)
          // delete previous snapshots
          _ <- ZIO
            .foreachPar(existingSnapShots)(ofst =>
              Files.deleteIfExists(dir / s"snapshot-$ofst.json")
            )
            .unit
            .catchAll(e => ZIO.logError(s"Error deleting previous snapshots: $e"))
          // get all segments
          allSegments <- findFiles(dir).runCollect
            .map(_.collect(_.filename.toString match {
              case s"segment-${Long(ofst)}.json" if ofst < offset => ofst
            }))
            .map(_.toArray.sorted)
          // get commiitted which should be deleted
          commmited = LowWaterMarkServiceImpl.committedSegments(allSegments, offset)
          // delete all segments that are committed
          _ <- ZIO
            .foreachPar(allSegments.filter(commmited.contains)) { ofst =>
              Files.deleteIfExists(dir / s"segment-$ofst.json")
            }
            .unit
            .catchAll(e => ZIO.logError(s"Error deleting previous segments: $e"))
        } yield ()
      }
    } yield ()

  def read: Task[Snapshot[St]] =
    for {
      offset <- findFiles(dir).runCollect.map(_.collect(_.filename.toString match {
        case s"snapshot-${Long(offset)}.json" => offset
      }).sorted.lastOption)
      stateOffset <- offset.fold(ZIO.succeed((stateComputer.zero, 0L))) { o =>
        JsonDecoder[St]
          .decodeJsonStreamInput(
            ZStream.fromFile((dir / s"snapshot-$o.json").toFile)
          )
          .map(s => (s, o))
      }
      (state, offset) = stateOffset
    } yield Snapshot(state, offset)

  def lowWaterMark: Task[Long] =
    findFiles(dir).runCollect.map(_.collect(_.filename.toString match {
      case s"snapshot-${Long(ofst)}.json" => ofst
    }).sorted.lastOption.getOrElse(0L))

  def start: Task[Unit] =
    val schedule = Schedule.fixed(1.minute)
    (createSnapshot repeat schedule).fork.unit

case class Snapshot[State](state: State, offset: Long)

object LowWaterMarkServiceImpl:
  /** takes sorted list of segments and and offset to find segments that are already committed
    * @param segments
    * @param offset
    * @return
    *   active segments
    */
  private[log] def committedSegments(
      segments: Array[Long],
      snapshot: Long
  ): List[Long] =
    case object Infinity {
      override def toString(): String = "Inf"
    }

    case class Range(start: Long, end: Long | Infinity.type) {
      def contains = (snapshot: Long) =>
        start <= snapshot && (end match {
          case i: Infinity.type =>
            true
          case e: Long =>
            snapshot <= e
        })

      override def toString = s"$start ... $end"
    }
    val commiittedSegments = scala.collection.mutable.ListBuffer[Long]()
    var i = 0

    while (i < segments.length) do {
      val start = segments(i)
      if snapshot < start then return commiittedSegments.toList

      val isLast = i == (segments.length - 1)
      val end = if isLast then Infinity else segments(i + 1) - 1

      if !Range(start, end).contains(snapshot)
      then commiittedSegments += segments(i)
      else return commiittedSegments.toList

      i += 1
    }

    commiittedSegments.toList
