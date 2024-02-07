package com.bilalfazlani.logSnapshots
package httpapp

import zio.http.*
import zio.http.endpoint.*
import zio.*
import zio.schema.*
import zio.stream.ZStream

case class KeyNotFound(key: String) derives Schema

val mutableMap = collection.mutable.Map.empty[String, String]

val put = Endpoint(Method.PUT / string("key") / string("value")).out[Unit]
val getAll = Endpoint(RoutePattern(Method.GET, Path.root)).out[Map[String, String]]

val getEndpoint =
  Endpoint(Method.GET / string("key"))
    .out[String]
    .outError[KeyNotFound](Status.NotFound)

val getRoute =
  def handler(k: String): ZIO[Any, KeyNotFound, String] =
    ZIO.fromOption(mutableMap.get(k)).mapError(_ => KeyNotFound(k))
  getEndpoint.implement(Handler.fromFunctionZIO[String](handler))

val putRoute = put.implement(Handler.fromFunction(mutableMap.put.apply))
val getAllRoute = getAll.implement(Handler.fromFunction { _ => mutableMap.toMap })

val streamEndpoint = Endpoint(Method.GET / "stream").outStream[ServerSentEvent]
val streamRoute = streamEndpoint.implement(
  Handler.fromFunction(_ =>
    ZStream
      .range(0, 3)
      .tap(_ => ZIO.sleep(1.second))
      .map(i => ServerSentEvent(i.toString, None, None, None))
  )
)

val allRoutes = Routes(streamRoute, getRoute, putRoute, getAllRoute)

enum Event derives Schema:
  case Put(key: String, value: String)
  case Delete(key: String)
