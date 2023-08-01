package com.bilalfazlani.logSegmentation

import zio.test.*
import zio.nio.file.Path
import zio.*
import com.bilalfazlani.cleanFiles
import com.bilalfazlani.compareFileNamesOfDirectories
import com.bilalfazlani.compareFileContentsOfDirectories

object DurableKVStoreTest extends ZIOSpecDefault {
  val spec = suite("DurableKVStore with log segmentation")(
    test("after reaching maxline, should roll files") {
      val path = Path("target") / "test-output" / "log-segmentation" / "roll-test"
      val effect1 =
        (DurableKVStore.set("name", "A") *>
          DurableKVStore.delete("name") *>
          DurableKVStore.set("name", "C") *>
          DurableKVStore.set("name", "D") *>
          DurableKVStore.set("name", "E") *>
          DurableKVStore.set("name", "F") *>
          DurableKVStore.set("name", "G") *>
          DurableKVStore.set("name", "H") *>
          DurableKVStore.set("name", "I") *>
          DurableKVStore.set("name", "J"))

      val test = effect1.provide(
        DurableKVStore.live[String, String](path),
        AppendOnlyLog.jsonFile[KVCommand[String, String]](path, 3)
      ) *> effect1.provide(
        DurableKVStore.live[String, String](path),
        AppendOnlyLog.jsonFile[KVCommand[String, String]](path, 3)
      )
      for
        _ <- test
        fileNamesMatch <- compareFileNamesOfDirectories(
          path,
          Path(
            "src"
          ) / "test" / "scala" / "com" / "bilalfazlani" / "2-log-segmentation" / "roll-test"
        )
        contentsMatch <- compareFileContentsOfDirectories(
          path,
          Path(
            "src"
          ) / "test" / "scala" / "com" / "bilalfazlani" / "2-log-segmentation" / "roll-test"
        )
      yield assertTrue(fileNamesMatch) && assertTrue(contentsMatch)
    }
  ) @@ cleanFiles(Path("target") / "test-output" / "log-segmentation")
}
