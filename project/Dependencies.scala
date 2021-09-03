import sbt._

object Dependencies {

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.8" % Test
  lazy val scallop   = "org.rogach" %% "scallop" % "4.0.4"
  lazy val spray     = "io.spray" %%  "spray-json" % "1.3.6"

  lazy val dependencies: Seq[ModuleID] = Seq(
    scalaTest,
    scallop,
    spray
  )

}
