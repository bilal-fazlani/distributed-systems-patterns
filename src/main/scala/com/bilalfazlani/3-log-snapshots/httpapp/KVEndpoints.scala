package com.bilalfazlani.logSnapshots
package httpapp

import zio.http.*
import zio.*
import kv.DurableKVStore
import log.{Event, Point, State}
import zio.http.endpoint.Endpoint
import zio.schema.*
import zio.stream.ZStream
import zio.http.endpoint.openapi.*
import zio.http.codec.PathCodec

case class KeyNotFound(key: String) derives Schema
case class KeyWriteError(message: String) derives Schema
case class KeyDeleteError(message: String) derives Schema

sealed trait EventModel derives Schema
case class DataChanged(point: Point, kvData: Map[String, String]) extends EventModel
case class LowWaterMarkChanged(offset: Long) extends EventModel
case class DataDiscarded(filesDeleted: List[String]) extends EventModel

trait KVRoutes:
  val routes: Routes[Any, Response]

object KVRoutes:
  val live = ZLayer.fromFunction(KVRoutesImpl.apply)

case class KVRoutesImpl(
    kvStore: DurableKVStore[String, String],
    eventHub: Hub[Event],
    state: State[Map[String, String]]
) extends KVRoutes:
  // -------- ENDPOINTS --------
  val put = Endpoint(Method.PUT / "kv" / string("key") / string("value"))
    .out[Unit]
    .outError[KeyWriteError](Status.InternalServerError)

  val delete = Endpoint(Method.DELETE / "kv" / string("key"))
    .out[Unit]
    .outError[KeyDeleteError](Status.InternalServerError)

  val getAll = Endpoint(Method.GET / "kv")
    .out[Map[String, String]]

  val getEndpoint =
    Endpoint(Method.GET / "kv" / string("key"))
      .out[String]
      .outError[KeyNotFound](Status.NotFound)

  val streamEndpoint = Endpoint(Method.GET / "kv-stream")
    .outStream[ServerSentEvent]

  // -------- IMPLEMENTATIONS --------
  val getRoute = getEndpoint.implement(
    Handler.fromFunctionZIO[String](k => kvStore.get(k).someOrFail(KeyNotFound(k)))
  )

  val putRoute =
    put.implement(
      Handler.fromFunctionZIO(kvStore.set(_, _).mapError(e => KeyWriteError(e.getMessage)))
    )

  val deleteRoute = delete.implement(
    Handler.fromFunctionZIO(kvStore.delete(_).mapError(e => KeyDeleteError(e.getMessage)))
  )

  val getAllRoute = getAll.implement(Handler.fromFunctionZIO { _ => state.all })

  val streamRoute = streamEndpoint.implement(
    Handler.fromFunction(_ =>
      ZStream
        .fromHub(eventHub)
        .mapZIO {
          case (e: Event.PointerMoved)        => state.all.map(data => DataChanged(e.point, data))
          case (e: Event.DateDiscarded)       => ZIO.succeed(DataDiscarded(e.filesDeleted))
          case (e: Event.LowWaterMarkChanged) => ZIO.succeed(LowWaterMarkChanged(e.lwm))
        }
        .map(e => ServerSentEvent(e.json, None, None, None))
    )
  )

  // -------- OPENAPI --------
  val openApi = OpenAPIGen.fromEndpoints(
    "Durable Key-Value Store",
    "1.0.0",
    Seq(put, delete, getAll, getEndpoint, streamEndpoint)
  )

  val swaggerUI = SwaggerUI.routes(PathCodec.empty / "docs" / "openapi", openApi)

  // -------- END --------
  override val routes =
    Routes(streamRoute, getRoute, putRoute, deleteRoute, getAllRoute) ++ swaggerUI
