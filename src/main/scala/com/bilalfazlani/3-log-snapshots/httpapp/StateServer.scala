package com.bilalfazlani.logSnapshots
package httpapp

import zio.*
// import zio.stream.*
import zio.http.*
// import log.*

object StateServer extends ZIOAppDefault:

  // routes for a kv store
  val httpApp = allRoutes.toHttpApp

  override val run =
    Server.serve(httpApp).provide(Server.defaultWith(_.port(8000)))

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
