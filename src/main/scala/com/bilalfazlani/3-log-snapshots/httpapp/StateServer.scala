package com.bilalfazlani.logSnapshots
package httpapp

import zio.*
import zio.http.*

import log.*
import kv.*

object StateServer extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.setConfigProvider(
      ConfigProvider.fromMap(
        Map(
          "dir" -> (Path("target") / "log").toString,
          "segmentSize" -> "3",
          "snapshotFrequency" -> "10s"
        )
      )
    )

  val program = for {
    app <- ZIO.serviceWith[KVRoutes](_.routes.toHttpApp)
    _ <- Server.serve(app)
  } yield ()

  override val run =
    program.provideSome[Scope](
      Server.defaultWith(_.port(8000)),
      KVRoutes.live,
      ZLayer(Semaphore.make(1)),
      StateLoader.live[KVCommand[String, String], Map[String, String]],
      StateComputerImpl.live[String, String],
      ConcurrentMap.live[String, String],
      Pointer.fromDisk[Map[String, String]],
      AppendOnlyLog.jsonFile[KVCommand[String, String]],
      LowWaterMarkService.live,
      DurableKVStore.live[String, String],

      // event hub
      ZLayer(Hub.sliding[Event](5)),

      // cleanup
      DataDiscardService.live,
      SnapshotService.start[Map[String, String]]
    )

  // val dd = for {
  //   h <- Hub.sliding[Event](10)
  //   _ <- h.publish(Event.PointerMoved(Point(0L, 0L, 0L)))
  //   sseStream <- ZStream.fromHubScoped(h)
  // } yield ()

  // val stream: ZStream[Any, Nothing, ServerSentEvent] =
  //   ZStream
  //     .tick(1.second)
  //     .map[Int](_ => 1)
  //     .mapAccum[Int, Int](0)((acc, e) => (acc + e, acc + e))
  //     .map(_.toString)
  //     .map(ServerSentEvent(_))

  // val app: HttpApp[Any] =
  //   Routes(
  //     Method.GET / "text" -> handler(Response.text("Hello World!")),
  //     Method.GET / "sse" -> handler(Response.fromServerSentEvents(stream))
  //   ).toHttpApp
