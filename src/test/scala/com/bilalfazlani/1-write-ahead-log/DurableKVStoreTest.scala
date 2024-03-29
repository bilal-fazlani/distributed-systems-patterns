package com.bilalfazlani.writeAheadLog

import zio.test.*
import zio.nio.file.Path
import com.bilalfazlani.cleanFiles
import com.bilalfazlani.compareFilesContents

object DurableKVStoreTest extends ZIOSpecDefault {
  val spec = suite("DurableKVStore using write ahead log")(
    test("set twice and get a value") {
      val path = Path("target") / "test-output" / "write-ahead-log" / "set-twice-and-get.txt"
      val setName =
        (DurableKVStore
          .set("name", "A") *> DurableKVStore.set("name", "B"))
          .provide(
            DurableKVStore.live[String, String](path),
            AppendOnlyLog.jsonFile[KVCommand[String, String]](path)
          )

      // providing layers multiple times ensures file is getting read multiple times

      val getName = DurableKVStore
        .get[String, String]("name")
        .provide(
          DurableKVStore.live[String, String](path),
          AppendOnlyLog.jsonFile[KVCommand[String, String]](path)
        )

      for {
        _ <- setName
        name <- getName
        logFileContentsMatched <- compareFilesContents(
          path,
          Path("src") / "test" / "scala" / "com" / "bilalfazlani" / "1-write-ahead-log" / "set-twice-and-get.txt"
        )
      } yield assertTrue(name.contains("B")) && logFileContentsMatched
    },
    test("set and delete a value") {
      val path = Path("target") / "test-output" / "write-ahead-log" / "set-and-delete.txt"
      val setName =
        (DurableKVStore
          .set("name", "A") *> DurableKVStore
          .delete("name"))
          .provide(
            DurableKVStore.live[String, String](path),
            AppendOnlyLog.jsonFile[KVCommand[String, String]](path)
          )

      val getName = DurableKVStore
        .get[String, String]("name")
        .provide(
          DurableKVStore.live[String, String](path),
          AppendOnlyLog.jsonFile[KVCommand[String, String]](path)
        )

      for {
        _ <- setName
        name <- getName
        logFileContentsMatched <- compareFilesContents(
          path,
          Path(
            "src"
          ) / "test" / "scala" / "com" / "bilalfazlani" / "1-write-ahead-log" / "set-and-delete.txt"
        )
      } yield assertTrue(name.isEmpty) && logFileContentsMatched
    }
  ) @@ cleanFiles(Path("target") / "test-output" / "write-ahead-log")
}
