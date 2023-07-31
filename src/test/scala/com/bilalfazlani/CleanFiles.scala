package com.bilalfazlani

import zio.nio.file.Path
import zio.test.TestAspect
import zio.nio.file.Files
import zio.ZIO

def cleanFiles(path: Path) = TestAspect.beforeAll(
  ZIO.whenZIO(Files.isDirectory(path)) {
    Files
      .walk(path, 4)
      .filterZIO(p => Files.isRegularFile(p))
      .runForeach(Files.delete)
  }
)
