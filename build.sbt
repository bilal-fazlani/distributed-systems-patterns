val scala3Version = "3.3.1"

lazy val root = project
  .in(file("."))
  .settings(
    name := "distributed-system-patterns",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    scalacOptions += "-Wunused:all",
    resolvers ++= Resolver.sonatypeOssRepos("snapshots"),
    libraryDependencies ++=
      Seq(
        Libs.zio,
        Libs.zioStreams,
        Libs.zioNio,
        Libs.zioLogging,
        Libs.zioJson,
        Libs.zioHTTP,
        Libs.zioTest % Test,
        Libs.zioTestSbt % Test
      )
  )
