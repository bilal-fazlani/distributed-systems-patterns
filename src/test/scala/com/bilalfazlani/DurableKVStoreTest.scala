package com.bilalfazlani

import zio.test.*
import zio.nio.file.Path

object DurableKVStoreTest extends ZIOSpecDefault {
  val spec = suite("DurableKVStore")(
    test("set and get a value") {
      val setName =
        (DurableKVStore.set("name", "A") *> DurableKVStore.set("name", "B"))
          .provide(DurableKVStore.live(Path("testkv.txt")))

      // providing layers multiple times ensures file is getting read multiple times

      val getName = DurableKVStore
        .get("name")
        .provide(DurableKVStore.live(Path("testkv.txt")))

      for {
        _ <- setName
        name <- getName
      } yield assertTrue(name.contains("B"))
    }
  )
}
