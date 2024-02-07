package com.bilalfazlani.logSnapshots
package httpapp

import zio.http.*
import zio.http.endpoint.*
import zio.*
import zio.schema.*

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

val allRoutes = Routes(getRoute, putRoute, getAllRoute)
