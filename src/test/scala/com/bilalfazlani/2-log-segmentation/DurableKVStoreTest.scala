package com.bilalfazlani.logSegmentation

import zio.test.*
import zio.nio.file.Path
import zio.*
import com.bilalfazlani.cleanFiles

object DurableKVStoreTest extends ZIOSpecDefault {
  val spec = suite("DurableKVStore")(
    test("set twice and get a value") {
      val path = Path("target") / "test-output" / "log-segmentation" / "set-twice-and-get"
      val setName =
        (DurableKVStore.set("name", "A") *> DurableKVStore.set("name", "B"))
          .provide(
            DurableKVStore.live[String, String](path),
            AppendOnlyLog.jsonFile[KVCommand[String, String]](path, 10)
          )

      // providing layers multiple times ensures file is getting read multiple times

      val getName = DurableKVStore
        .get[String, String]("name")
        .provide(
          DurableKVStore.live[String, String](path),
          AppendOnlyLog.jsonFile[KVCommand[String, String]](path, 10)
        )

      for {
        _ <- setName
        name <- getName
      } yield assertTrue(name.contains("B"))
    },
    test("set and delete a value") {
      val path = Path("target") / "test-output" / "log-segmentation" / "set-and-delete"
      val setName =
        (DurableKVStore
          .set("name", "A") *> DurableKVStore
          .delete("name"))
          .provide(
            DurableKVStore.live[String, String](path),
            AppendOnlyLog.jsonFile[KVCommand[String, String]](path, 10)
          )

      val getName = DurableKVStore
        .get[String, String]("name")
        .provide(
          DurableKVStore.live[String, String](path),
          AppendOnlyLog.jsonFile[KVCommand[String, String]](path, 10)
        )

      for {
        _ <- setName
        name <- getName
      } yield assertTrue(name.isEmpty)
    },
    test("after reaching maxline, should roll files") {
      val path = Path("target") / "test-output" / "log-segmentation" / "roll-test"
      val effect1 =
        (DurableKVStore.set("name", "A") *>
          DurableKVStore.set("name", "B") *>
          DurableKVStore.set("name", "C") *>
          DurableKVStore.set("name", "D") *>
          DurableKVStore.set("name", "E") *>
          DurableKVStore.set("name", "F") *>
          DurableKVStore.set("name", "G") *>
          DurableKVStore.set("name", "H") *>
          DurableKVStore.set("name", "I") *>
          DurableKVStore.set("name", "J"))

      effect1.provide(
        DurableKVStore.live[String, String](path),
        AppendOnlyLog.jsonFile[KVCommand[String, String]](path, 3)
      ) *> effect1.provide(
        DurableKVStore.live[String, String](path),
        AppendOnlyLog.jsonFile[KVCommand[String, String]](path, 3)
      ) *> assertCompletes
    }
  ) @@ cleanFiles(Path("target") / "test-output" / "log-segmentation")
}
