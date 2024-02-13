package com.bilalfazlani.logSnapshots
package log

import zio.*
import com.bilalfazlani.*

trait LowWaterMarkService:
  def lowWaterMark: UIO[Option[Long]]
  def change[R, E](f: Option[Long] => ZIO[R, E, Option[Long]]): ZIO[R, E, Unit]

object LowWaterMarkService:
  val fromDisk = ZLayer(for {
    config <- ZIO.config(LogConfiguration.config)
    x <- findFiles(config.dir).runCollect.map(_.collect(_.filename.toString match {
      case s"snapshot-${Long(ofst)}.json" => ofst
    }).sorted.lastOption)
    ref <- Ref.Synchronized.make(x)
    service: LowWaterMarkService = LowWaterMarkServiceImpl(ref)
  } yield service)

extension (l: Long.type) private[this] def unapply(s: String): Option[Long] = s.toLongOption

case class LowWaterMarkServiceImpl(
    ref: Ref.Synchronized[Option[Long]]
) extends LowWaterMarkService:

  def lowWaterMark: UIO[Option[Long]] = ref.get

  def change[R, E](f: Option[Long] => ZIO[R, E, Option[Long]]) =
    // val g = f.compose[Option[Long]]()
    for _ <- ref.getAndUpdateZIO(f)
    yield ()
