package com.bilalfazlani.logSnapshots
package log

import zio.*
import com.bilalfazlani.*
import zio.json.*
import zio.stream.ZStream

trait StateLoader[State]:
  def load: ZIO[Scope, Throwable, State]

object StateLoader:
  def live[Item: JsonCodec: Tag, State: Tag]
      : ZLayer[StateComputer[Item, State], Nothing, StateLoader[State]] =
    ZLayer.makeSome[StateComputer[Item, State], StateLoaderImpl[Item, State]](
      ZLayer.fromFunction(StateLoaderImpl.apply[Item, State])
    )

case class StateLoaderImpl[Item: JsonCodec, State](
    stateComputer: StateComputer[Item, State]
) extends StateLoader[State]:
  def load: ZIO[Scope, Throwable, State] =
    for
      dir <- ZIO.config(LogConfiguration.config).map(_.dir)
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
