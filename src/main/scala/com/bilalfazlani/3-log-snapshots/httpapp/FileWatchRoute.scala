package com.bilalfazlani.logSnapshots
package httpapp

import zio.*
import zio.nio.file.FileSystem
import zio.nio.file.Path
import java.nio.file.StandardWatchEventKinds
import zio.stream.ZStream
import zio.schema.*
import zio.http.ServerSentEvent
import zio.http.endpoint.Endpoint
import zio.http.Method
import zio.http.Handler
import zio.http.Status
import zio.nio.file.WatchKey
import zio.http.string
import java.net.URLDecoder

case class FileStreamError(message: String) derives Schema

// val path = Path(
//   "/Users/bilal/Projects/distributed-systems-patterns/src/main/scala/com/bilalfazlani/3-log-snapshots/httpapp/"
// )

def watchKeys(p: String): ZIO[Scope, FileStreamError, Chunk[WatchKey]] =
  val path = Path(URLDecoder.decode(p))
  (for {
    watchService <- FileSystem.default.newWatchService
    watchKey <- path.registerTree(
      watchService,
      Seq(StandardWatchEventKinds.ENTRY_MODIFY),
      10,
      Seq.empty
    )
  } yield watchKey)
    .mapError(e => FileStreamError(e.toString))

def fileStream(watchKeys: Chunk[WatchKey]): ZStream[Any, Nothing, ServerSentEvent] =
  ZStream
    .mergeAll(10, 1024)(
      watchKeys
        .map(watchKey => ZIO.scoped(watchKey.pollEventsScoped.map(Chunk.apply)))
        .map(ZStream.repeatZIOChunk)*
    )
    .map(e => FileEvent(e.context.toString, e.kind.name))
    .map(fileEvent => ServerSentEvent(fileEvent.json))

case class FileEvent(path: String, kind: String) derives Schema

extension [A: Schema](a: A)
  /** Convert the given value to json
    */
  def json: String =
    zio.schema.codec.JsonCodec.jsonCodec(Schema[A]).encodeJson(a, None).toString

val fileStreamEndpoint =
  Endpoint(Method.GET / "track" / string("path"))
    .outStream[ServerSentEvent]
    .outError[FileStreamError](Status.InternalServerError)

val fileStreamRoute = fileStreamEndpoint.implement(
  Handler.fromFunctionZIO(path => watchKeys(path).map(fileStream))
)
