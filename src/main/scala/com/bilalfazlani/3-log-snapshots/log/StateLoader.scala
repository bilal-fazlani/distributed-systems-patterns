package com.bilalfazlani.logSnapshots
package log

import zio.*
import com.bilalfazlani.*
import zio.nio.file.Path
import zio.json.*
import zio.stream.ZStream

trait StateLoader[State]:
  def load: ZIO[Scope, Throwable, State]

object StateLoader:
  def live[Item: JsonCodec: Tag, State: Tag](
      path: Path
  ): ZLayer[Semaphore & StateComputer[Item, State], Nothing, StateLoader[State]] =
    ZLayer.makeSome[Semaphore & StateComputer[Item, State], StateLoaderImpl[Item, State]](
      ZLayer.succeed(path),
      ZLayer.fromFunction(StateLoaderImpl.apply[Item, State])
    )

case class StateLoaderImpl[Item: JsonCodec, State](
    stateComputer: StateComputer[Item, State],
    dir: Path,
    sem: Semaphore
) extends StateLoader[State]:
  def load: ZIO[Scope, Throwable, State] =
    for
      _ <- sem.withPermitScoped
      filePaths <- findFiles(dir).runCollect.map(_.sortBy(_.filename.toString))
      aStream = filePaths
        .map(p =>
          streamLines(p)
            .map(line => JsonDecoder[Item].decodeJson(line).left.map(e => Throwable(e)))
            .absolve
        )
        .foldLeft[ZStream[Any, Throwable, Item]](ZStream.empty)(_ ++ _)
      state <- aStream.runFold(stateComputer.zero)(stateComputer.compute)
    yield state
