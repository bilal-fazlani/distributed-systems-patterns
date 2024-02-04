package com.bilalfazlani.logSegmentation

package log

import zio.*
import zio.json.*
import zio.nio.file.Path
import java.io.IOException
import com.bilalfazlani.*

trait AppendOnlyLog[A]:
  def append(a: A): ZIO[Scope, IOException, Unit]
  def computeState[State](initial: State)(f: (State, A) => State): ZIO[Scope, Throwable, State]

object AppendOnlyLog:
  def jsonFile[A: JsonCodec: Tag](
      dir: Path,
      maxLines: Long
  ): ZLayer[Any, IOException, AppendOnlyLogJsonImpl[A]] =
    (ZLayer.fromZIO(Semaphore.make(1)) ++ ZLayer.fromZIO(
      State.loadFromDisk(dir).flatMap(Ref.make)
    )) >>>
      ZLayer.fromFunction(AppendOnlyLogJsonImpl.apply(_, _, dir, maxLines))

  def append[A: JsonEncoder: Tag](
      a: A
  ): ZIO[AppendOnlyLog[A] & Scope, IOException, Unit] =
    ZIO.serviceWithZIO[AppendOnlyLog[A]](_.append(a))

  def computeState[A: JsonDecoder: Tag, State](
      initial: State
  )(f: (State, A) => State): ZIO[AppendOnlyLog[A] & Scope, Throwable, State] =
    ZIO.serviceWithZIO[AppendOnlyLog[A]](_.computeState(initial)(f))

