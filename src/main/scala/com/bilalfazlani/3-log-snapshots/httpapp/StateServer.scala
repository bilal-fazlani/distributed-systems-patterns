package com.bilalfazlani.logSnapshots
package httpapp

import zio.*
import zio.http.*

import log.*
import kv.*
import zio.logging.ConsoleLoggerConfig
import zio.logging.LogFormat
import zio.logging.LogFilter.LogLevelByNameConfig

object StateServer extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.setConfigProvider(
      ConfigProvider.fromMap(
        Map(
          "dir" -> (Path("target") / "log").toString,
          "segmentSize" -> "3",
          "snapshotFrequency" -> "off",
          "logLevel" -> "debug"
        )
      )
    ) >>>
      ZLayer(ZIO.config(LogConfiguration.config)) >>> ZLayer.service[LogConfiguration].flatMap {
        config =>
          Runtime.removeDefaultLoggers ++ zio.logging.consoleLogger(
            ConsoleLoggerConfig(LogFormat.colored, LogLevelByNameConfig(config.get.logLevel))
          )
      }

  val seedData = for {
    kvStore <- ZIO.service[DurableKVStore[String, String]]
    _ <- kvStore.set("a", "1").delay(1.second)
    _ <- kvStore.set("b", "2").delay(1.second)
    _ <- kvStore.set("c", "3").delay(1.second)
    _ <- kvStore.set("a", "4").delay(1.second)
    _ <- kvStore.set("b", "5").delay(1.second)
    _ <- kvStore.set("c", "6").delay(1.second)
  } yield ()

  val program = for {
    app <- ZIO.serviceWith[KVRoutes](_.routes.toHttpApp)
    _ <- Server.serve(app)
    // _ <- seedData.forever
  } yield ()

  override val run =
    program
      .provideSome[Scope](
        Server.defaultWith(_.port(8000)),
        KVRoutes.live,
        ZLayer(Semaphore.make(1)),
        StateLoader.live[KVCommand[String, String], Map[String, String]],
        StateComputerImpl.live[String, String],
        ConcurrentMap.live[String, String],
        AppendOnlyLog.jsonFile[KVCommand[String, String]],
        LowWaterMarkService.fromDisk,
        DurableKVStore.live[String, String],
        Pointer.fromDisk[Map[String, String]],

        // event hub
        ZLayer(Hub.sliding[Event](5)),

        // cleanup
        DataDiscardService.live,
        SnapshotService.start[Map[String, String]]
      )
