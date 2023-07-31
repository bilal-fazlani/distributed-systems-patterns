package com.bilalfazlani.durableKVStore

import zio.test.*
import zio.nio.file.Path

object DurableKVStoreTest extends ZIOSpecDefault {

  val spec = suite("DurableKVStore")(
    test("set and get a value") {
      val path = Path("set-get-value-test.txt")
      val setName =
        (DurableKVStore
          .set[String, String]("name", "A") *> DurableKVStore.set("name", "B"))
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
    test("delete a value") {
      val path = Path("delete-value-test.txt")
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
  )
}
