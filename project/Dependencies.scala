import sbt._

object Dependencies {

  lazy val scalaTest = "org.scalatest"    %% "scalatest"  % "3.2.8" % Test
  lazy val spray     = "io.spray"         %% "spray-json" % "1.3.6"
  lazy val scopt     = "com.github.scopt" %% "scopt"      % "4.0.1"
  lazy val cats      = "org.typelevel"    %% "cats-core"  % "2.3.0"

  lazy val dependencies: Seq[ModuleID] = Seq(
    scalaTest,
    spray,
    scopt,
    cats
  )

}
