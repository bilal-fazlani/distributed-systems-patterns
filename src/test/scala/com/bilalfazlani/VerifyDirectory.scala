package com.bilalfazlani

import zio.nio.file.{Path, Files}
import zio.nio.charset.Charset
import java.io.IOException
import zio.*
import zio.stream.ZStream
import zio.test.Assertion.*
import zio.test.*

private def getFilesPaths(dir: Path): ZIO[Any, IOException, Chunk[Path]] =
  Files.walk(dir, 1).runCollect.map(_.sortBy(_.toString).tail)

def compareFilesContents(file1: Path, file2: Path): ZIO[Any, IOException, TestResult] =
  Files
    .lines(file1, Charset.Standard.utf8)
    .zipAll(Files.lines(file2, Charset.Standard.utf8))("", "")
    .runFold(TestResult.pass)((acc, lines) => acc && assert(lines._1)(equalTo(lines._2)))

def compareFileNamesOfDirectories(dir1: Path, dir2: Path): ZIO[Any, IOException, TestResult] =
  for
    files1 <- getFilesPaths(dir1).map(_.map(_.filename))
    files2 <- getFilesPaths(dir2).map(_.map(_.filename))
  yield assert(files1)(equalTo(files2))

def compareFileContentsOfDirectories(
    dir1: Path,
    dir2: Path
): ZIO[Any, IOException, TestResult] =
  for
    files1 <- getFilesPaths(dir1)
    files2 <- getFilesPaths(dir2)
    files = ZStream.fromChunk(files1.zip(files2))
    result <- files.runFoldZIO(TestResult.pass)((acc, files) =>
      compareFilesContents(files._1, files._2).map(_ && acc)
    )
  yield result

extension (x: TestResult.type) def pass = TestResult(TestArrow.succeed(true))
