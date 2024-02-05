package com.bilalfazlani

import zio.ZIO
import zio.nio.charset.Charset
import zio.nio.file.Files
import zio.nio.file.Path
import zio.stream.ZSink
import zio.stream.ZStream

import java.io.IOException
import java.nio.file.StandardOpenOption
import java.nio.file.StandardCopyOption

/** Streams all the files in the given directory
  *
  * @param dir
  * @param predicate
  */
def findFiles(dir: Path): ZStream[Any, IOException, Path] =
  ZStream.whenZIO(Files.isDirectory(dir))(Files.list(dir))

/** gets the number of lines in the file. If the file doesn't exist, it will return 0.
  *
  * @param path
  * @return
  *   line count
  */
def getLineCount(path: Path): ZIO[Any, IOException, Long] =
  Files
    .lines(path, Charset.Standard.utf8)
    .run(ZSink.count)
    .catchSome { case _: java.nio.file.NoSuchFileException =>
      ZIO.succeed(0L)
    }

/** Reads all the lines from the file. If the file doesn't exist, it will return an empty list.
  *
  * @param path
  */
def readLines(path: Path): ZIO[Any, IOException, List[String]] =
  Files.readAllLines(path, Charset.Standard.utf8).catchSome {
    case _: java.nio.file.NoSuchFileException => ZIO.succeed(List.empty)
  }

/** Streams all the lines from the file. If the file doesn't exist, it will return an empty stream.
  *
  * @param path
  */
def streamLines(path: Path): ZStream[Any, IOException, String] =
  Files
    .lines(path, Charset.Standard.utf8)
    .catchSome { case _: java.nio.file.NoSuchFileException =>
      ZStream.empty
    }

/** Creates a file with the given contents or overwrites the file if it already exists. If the
  * parent directory doesn't exist, it will be created.
  * @param path
  * @param contents
  */
def newFile(path: Path, contents: String) =
  path.parent.fold(overriteFile(path, contents))(parent =>
    for
      _ <- Files.createDirectories(parent)
      _ <- overriteFile(path, contents)
    yield ()
  )  

/** Moves the file from the given path to the new path If the new path already exists, it will be
  * replaced If the parent directory of the new path doesn't exist, it will be created.
  * @param from
  * @param to
  */
def moveFile(from: Path, to: Path) =
  Files.move(
    from,
    to,
    StandardCopyOption.ATOMIC_MOVE,
    StandardCopyOption.REPLACE_EXISTING,
  )

/** Appends the given contents to the file. If the file doesn't exist, it will be created. If the
  * parent directory doesn't exist, it will be created.
  *
  * @param path
  * @param contents
  */
def appendToFile(path: Path, contents: String) =
  path.parent.fold(append(path, contents))(parent =>
    for
      _ <- Files.createDirectories(parent)
      _ <- append(path, contents)
    yield ()
  )

private def overriteFile(path: Path, contents: String) =
  for
    _ <- Files.createFile(path).catchSome { case _: java.nio.file.FileAlreadyExistsException =>
      ZIO.unit
    }
    _ <- Files.writeLines(
      path,
      Seq(contents),
      Charset.Standard.utf8,
      Set(StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
    )
  yield ()

private def append(path: Path, contents: String) =
  for _ <- Files.writeLines(
      path,
      Seq(contents),
      Charset.Standard.utf8,
      Set(StandardOpenOption.APPEND, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    )
  yield ()
