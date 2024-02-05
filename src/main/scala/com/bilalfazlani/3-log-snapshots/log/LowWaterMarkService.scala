package com.bilalfazlani
package logSnapshots
package log

import zio.json.*
import zio.*
import zio.nio.file.Path
import zio.nio.file.Files
import zio.stream.ZStream

trait LowWaterMarkService[State: JsonCodec]:
  def save(state: State, offset: Long): Task[Unit]
  def read: Task[Snapshot[State]]
  def lowWaterMark: Task[Long]

extension (l: Long.type) private[this] def unapply(s: String): Option[Long] = s.toLongOption

class LowWaterMarkServiceImpl[State: JsonCodec, Item](
    dir: Path,
    stateComputer: StateComputer[Item, State]
) extends LowWaterMarkService:
  def save(state: State, offset: Long): Task[Unit] =
    val path = dir / s"snapshot-$offset.json"
    for {
      existingSnapShots <- findFiles(dir).runCollect.map(_.collect(_.filename.toString match {
        case s"snapshot-${Long(ofst)}.json"     => Some(ofst)
        case s"snapshot-${Long(`offset`)}.json" => None
      }).flatten)
      _ <- newFile(path, state.toJson)
      _ <- ZIO
        .foreachPar(existingSnapShots)(ofst => Files.deleteIfExists(dir / s"snapshot-$ofst.json"))
        .unit
        .catchAll(e => ZIO.logError(s"Error deleting previous snapshots: $e"))
    } yield ()

  def read: Task[Snapshot[State]] =
    for {
      offset <- findFiles(dir).runCollect.map(_.collect(_.filename.toString match {
        case s"snapshot-${Long(offset)}.json" => offset
      }).sorted.lastOption)
      stateOffset <- offset.fold(ZIO.succeed((stateComputer.zero, 0L))) { o =>
        JsonDecoder[State]
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

case class Snapshot[State](state: State, offset: Long)
