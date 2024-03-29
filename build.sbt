/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Dependencies._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

ThisBuild / scalaVersion     := "2.13.6"
ThisBuild / organization     := "za.co.absa"
ThisBuild / versionScheme    := Some("early-semver")
ThisBuild / githubOwner      := "AbsaOSS"
ThisBuild / githubRepository := "spark-metadata-tool"

Test / parallelExecution := false

val mergeStrategy: Def.SettingsDefinition = assembly / assemblyMergeStrategy := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.concat
  case other: Any                              => MergeStrategy.defaultMergeStrategy(other)
}

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("publishSigned"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-metadata-tool",
    libraryDependencies ++= dependencies,
    semanticdbEnabled := true,                        // enable SemanticDB
    semanticdbVersion := scalafixSemanticdb.revision, // use Scalafix compatible version
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    scalacOptions ++= compilerOptions,
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
    releaseVersionBump   := sbtrelease.Version.Bump.Minor,
    assembly / mainClass := Some("za.co.absa.spark_metadata_tool.Application"),
    assembly / test      := {},
    mergeStrategy,
    assembly / artifact := {
      val art = (assembly / artifact).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(assembly / artifact, assembly),
    patSettings
  )
  .enablePlugins(AutomateHeaderPlugin)

val patSettings = githubTokenSource := TokenSource.Or(
  TokenSource.Environment("GITHUB_TOKEN"), // Required for publishing
  TokenSource.Or(
    TokenSource.Environment("SHELL"),      // Used to bypass PAT eager resolution for local development
    TokenSource.GitConfig("github.token")  // Used on windows with IntelliJ due to known issue: https://github.com/djspiewak/sbt-github-packages/issues/26
  )
)

val compilerOptions = Seq(
  "-target:jvm-1.8",
  "-explaintypes",
  "-feature",
  "-language:existentials",
  "-language:experimental.macros",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xcheckinit",
  "-Xfatal-warnings",
  "-Xlint:adapted-args",
  "-Xlint:constant",
  "-Xlint:delayedinit-select",
  "-Xlint:deprecation",
  "-Xlint:doc-detached",
  "-Xlint:inaccessible",
  "-Xlint:infer-any",
  "-Xlint:missing-interpolator",
  "-Xlint:nullary-unit",
  "-Xlint:option-implicit",
  "-Xlint:package-object-classes",
  "-Xlint:poly-implicit-overload",
  "-Xlint:private-shadow",
  "-Xlint:stars-align",
  "-Xlint:strict-unsealed-patmat",
  "-Xlint:type-parameter-shadow",
  "-Xlint:-byname-implicit",
  "-Yrangepos",
  "-Wunused:nowarn",
  "-Wdead-code",
  "-Wextra-implicit",
  "-Wnumeric-widen",
  "-Wunused:implicits",
  "-Wunused:explicits",
  "-Wunused:imports",
  "-Wunused:locals",
  "-Wunused:params",
  "-Wunused:patvars",
  "-Wunused:privates",
  "-Wvalue-discard"
)

// JaCoCo code coverage
Test / jacocoReportSettings := JacocoReportSettings(
  title = s"spark-metadata-tool Jacoco Report - scala:${scalaVersion.value}",
  formats = Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML)
)

// exclude example
Test / jacocoExcludes := Seq(
//    "za.co.absa.spark_metadata_tool.model.S3*", // class and related objects
//    "za.co.absa.spark_metadata_tool.model.AppConfig" // class only
)
