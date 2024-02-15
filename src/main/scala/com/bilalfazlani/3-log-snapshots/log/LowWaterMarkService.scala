package com.bilalfazlani.logSnapshots
package log

import zio.*
import com.bilalfazlani.*

trait LowWaterMarkService:
  def lowWaterMark: UIO[Option[Long]]
  def change[R, E](f: Option[Long] => ZIO[R, E, Option[Long]]): ZIO[R, E, Unit]

object LowWaterMarkService:
  val fromDisk = ZLayer(for {
    lastSnapshot <- ZIO.serviceWithZIO[ReadOnlyStorage](_.lastSnapshot)
    ref <- Ref.Synchronized.make(lastSnapshot)
    eventHub <- ZIO.service[Hub[Event]]
    service: LowWaterMarkService = LowWaterMarkServiceImpl(ref, eventHub)
  } yield service)

extension (l: Long.type) private[this] def unapply(s: String): Option[Long] = s.toLongOption

case class LowWaterMarkServiceImpl(
    ref: Ref.Synchronized[Option[Long]],
    eventHub: Hub[Event]
) extends LowWaterMarkService:

  def lowWaterMark: UIO[Option[Long]] = ref.get

  def change[R, E](f: Option[Long] => ZIO[R, E, Option[Long]]) =
    val g = f.andThen(x =>
      x.flatMap {
        case Some(point) => x <* eventHub.publish(Event.LowWaterMarkChanged(point))
        case None        => x
      }
    )
    ref.getAndUpdateZIO(g).unit
