package com.bilalfazlani.writeAheadLog

import zio.test.*
import zio.nio.file.Path
import com.bilalfazlani.cleanFiles

object DurableKVStoreTest extends ZIOSpecDefault {
  val spec = suite("DurableKVStore")(
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
      } yield assertTrue(name.contains("B"))
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
      } yield assertTrue(name.isEmpty)
    }
  ) @@ cleanFiles(Path("target") / "test-output" / "write-ahead-log")
}
