import sbt._

object Dependencies {

  lazy val scalaTest = "org.scalatest"    %% "scalatest"  % "3.2.9"
  lazy val scalaMock = "org.scalamock"    %% "scalamock"  % "5.1.0"
  lazy val spray     = "io.spray"         %% "spray-json" % "1.3.6"
  lazy val scopt     = "com.github.scopt" %% "scopt"      % "4.0.1"
  lazy val cats      = "org.typelevel"    %% "cats-core"  % "2.3.0"

  lazy val hadoop    = "org.apache.hadoop" % "hadoop-client" % "2.10.1"

  lazy val dependencies: Seq[ModuleID] = Seq(
    Seq(
      scalaTest,
      scalaMock
    ).map(_ % Test),
    Seq(
      spray,
      scopt,
      cats,
      hadoop
    )
  ).flatten

}
