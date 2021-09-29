import sbt._

object Dependencies {

  val jacksonVersion = "2.12.5"

  lazy val scalaTest = "org.scalatest"    %% "scalatest" % "3.2.9" % Test
  lazy val scalaMock = "org.scalamock"    %% "scalamock" % "5.1.0" % Test
  lazy val scopt     = "com.github.scopt" %% "scopt"     % "4.0.1"
  lazy val cats      = "org.typelevel"    %% "cats-core" % "2.3.0"

  lazy val jacksonScala = "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
  lazy val jackson      = "com.fasterxml.jackson.core"    % "jackson-databind"     % jacksonVersion

  lazy val hadoop = "org.apache.hadoop" % "hadoop-client" % "2.10.1"

  lazy val dependencies: Seq[ModuleID] = Seq(
    scalaTest,
    scalaMock,
    scopt,
    cats,
    hadoop,
    jackson,
    jacksonScala
  )

}
