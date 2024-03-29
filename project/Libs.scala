import sbt._

object Libs {
  lazy val zioVersion = "2.0.18"
  private val ZIO = "dev.zio"

  lazy val zio = ZIO %% "zio" % zioVersion
  lazy val zioNio = ZIO %% "zio-nio" % "2.0.2"
  lazy val zioLogging = ZIO %% "zio-logging" % "2.2.0"
  lazy val zioStreams = ZIO %% "zio-streams" % zioVersion
  lazy val zioJson = ZIO %% "zio-json" % "0.6.2"
  lazy val zioHTTP = ZIO %% "zio-http" % "3.0.0-RC4+60-aecd87cb-SNAPSHOT"
  // TESTING
  lazy val zioTest = ZIO %% "zio-test" % zioVersion
  lazy val zioTestSbt = ZIO %% "zio-test-sbt" % zioVersion
}
