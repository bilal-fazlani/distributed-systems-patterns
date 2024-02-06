package com.bilalfazlani.logSnapshots
package log

import zio.*
import com.bilalfazlani.*

trait LowWaterMarkService:
  def lowWaterMark: UIO[Long]
  def change[R, E](f: Long => ZIO[R, E, Long]): ZIO[R, E, Unit]

object LowWaterMarkService:
  val live = ZLayer(for {
    config <- ZIO.config[LogConfiguration](LogConfiguration.config)
    x <- findFiles(config.dir).runCollect.map(_.collect(_.filename.toString match {
      case s"snapshot-${Long(ofst)}.json" => ofst
    }).sorted.lastOption.getOrElse(0L))
    ref <- Ref.Synchronized.make(x)
    service: LowWaterMarkService = LowWaterMarkServiceImpl(ref)
  } yield service)

extension (l: Long.type) private[this] def unapply(s: String): Option[Long] = s.toLongOption

case class LowWaterMarkServiceImpl(
    ref: Ref.Synchronized[Long]
) extends LowWaterMarkService:

  def lowWaterMark: UIO[Long] = ref.get

  def change[R, E](f: Long => ZIO[R, E, Long]) = ref.getAndUpdateZIO(f).unit
