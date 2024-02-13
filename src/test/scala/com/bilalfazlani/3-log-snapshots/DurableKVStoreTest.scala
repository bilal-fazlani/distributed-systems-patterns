package com.bilalfazlani.logSnapshots
package kv

import zio.test.*
import zio.nio.file.Path
import zio.*
import com.bilalfazlani.*

object DurableKVStoreTest extends ZIOSpecDefault {

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] =
    Runtime.setConfigProvider(
      ConfigProvider.fromMap(
        Map(
          "dir" -> (Path("target") / "test-output" / "log-snapshots" / "snapshot-test").toString,
          "segmentSize" -> "3",
          "snapshotFrequency" -> "15s"
        )
      )
    ) >>> zio.test.testEnvironment

  val spec = suite("DurableKVStore with log snapshots")(
    // this test creates a durable kv store based on a append only log.
    // the append only log is set to configure the max 3 records in a file
    // the test writes 10 records to the store and verifies that the files are rolled
    test("after reaching snapshot frequency, should create a snapshot") {
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
        DurableKVStore.withoutCleanup[String, String],
        Scope.default
      ) *> effect1.provide(
        DurableKVStore.withoutCleanup[String, String],
        Scope.default
      )
      for
        _ <- test
        fileNamesMatch <- compareFileNamesOfDirectories(
          Path("target") / "test-output" / "log-snapshots" / "snapshot-test",
          Path(
            "src"
          ) / "test" / "scala" / "com" / "bilalfazlani" / "3-log-snapshots" / "snapshot-test"
        )
        contentsMatch <- compareFileContentsOfDirectories(
          Path("target") / "test-output" / "log-snapshots" / "snapshot-test",
          Path(
            "src"
          ) / "test" / "scala" / "com" / "bilalfazlani" / "3-log-snapshots" / "snapshot-test"
        )
      yield fileNamesMatch && contentsMatch
    }
  ) @@ cleanFiles(Path("target") / "test-output" / "log-snapshots")
}
