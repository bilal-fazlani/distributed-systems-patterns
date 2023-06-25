package com.bilalfazlani

import zio.*
import zio.nio.file.*
import zio.json.*
import java.io.IOException

/** A key value store that persists data to disk. It uses write ahead logging to persist data. Reading data is done from
  * memory.
  */
trait DurableKVStore:
  def get(key: String): UIO[Option[String]]
  def set(key: String, value: String): Task[Unit]
  def delete(key: String): Task[Unit]

object DurableKVStore:
  def live(path: Path) = ZLayer.make[DurableKVStore](
    AppendOnlyLog.jsonFile[KVLogCommand](path),
    ZLayer.fromZIO(initiateState),
    ZLayer.fromFunction(DurableKVStoreImpl.apply)
  )

  def get(key: String)                = ZIO.serviceWithZIO[DurableKVStore](_.get(key))
  def set(key: String, value: String) = ZIO.serviceWithZIO[DurableKVStore](_.set(key, value))
  def delete(key: String)             = ZIO.serviceWithZIO[DurableKVStore](_.delete(key))

  private def initiateState: ZIO[AppendOnlyLog[KVLogCommand], IOException, Ref[Map[String, String]]] =
    ZIO
      .scoped(AppendOnlyLog.readAll[KVLogCommand])
      .map(lines =>
        lines.foldLeft(Map.empty[String, String]) {
          case (acc, KVLogCommand.Set(k, v)) => acc + ((k, v))
          case (acc, KVLogCommand.Delete(k)) => acc - k
        }
      )
      .flatMap(map => Ref.make(map))

private case class DurableKVStoreImpl(mapRef: Ref[Map[String, String]], log: AppendOnlyLog[KVLogCommand])
    extends DurableKVStore:
  def get(key: String): UIO[Option[String]] = mapRef.get.map(_.get(key))

  def set(key: String, value: String): Task[Unit] =
    ZIO.scoped {
      log.append(KVLogCommand.Set(key, value)) *> mapRef.update(_ + ((key, value)))
    }

  def delete(key: String): Task[Unit] =
    ZIO.scoped {
      log.append(KVLogCommand.Delete(key)) *> mapRef.update(_ - key)
    }

enum KVLogCommand derives JsonCodec:
  case Set(key: String, value: String)
  case Delete(key: String)
