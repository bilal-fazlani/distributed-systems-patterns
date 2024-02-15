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
          "segmentSize" -> "5000",
          "snapshotFrequency" -> "10s",
          "logLevel" -> "off"
        )
      )
    ) >>>
      ZLayer(ZIO.config(LogConfiguration.config)) >>> ZLayer.service[LogConfiguration].flatMap {
        config =>
          Runtime.removeDefaultLoggers ++ zio.logging.consoleLogger(
            ConsoleLoggerConfig(LogFormat.colored, LogLevelByNameConfig(config.get.logLevel))
          )
      }

  val keys = "abcdefghijklmnopqrstuvwxyz"

  val randomKv = for {
    key <- (Random.nextIntBetween(0, 25) zip Random.nextIntBetween(0, 25)).map { case (a, b) =>
      s"${keys(a)}${keys(b)}"
    }
    value <- Random.nextIntBounded(100)
  } yield (key.toString, value.toString)

  val seedData =
    ZIO.serviceWithZIO[DurableKVStore[String, String]](kvStore =>
      // randomKv.flatMap(kvStore.set).forever.timeout(10.seconds)
      kvStore.set("A", "B").forever.timeout(1.seconds) *> Console.printLine("done seeding")
    )

  val program = for {
    app <- ZIO.serviceWith[KVRoutes](_.routes.toHttpApp)
    _ <- seedData.forkScoped
    _ <- Server.serve(app)
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
        ReadOnlyStorage.live,

        // event hub
        ZLayer(Hub.sliding[Event](5)),

        // cleanup
        DataDiscardService.live,
        SnapshotService.start[Map[String, String]]
      )
