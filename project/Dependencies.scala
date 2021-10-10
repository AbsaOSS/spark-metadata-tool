/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._

object Dependencies {

  val circeVersion = "0.14.1"

  lazy val aws          = "software.amazon.awssdk" % "s3"            % "2.17.55"
  lazy val cats         = "org.typelevel"         %% "cats-core"     % "2.3.0"
  lazy val scalaTest    = "org.scalatest"         %% "scalatest"     % "3.2.9" % Test
  lazy val scalaMock    = "org.scalamock"         %% "scalamock"     % "5.1.0" % Test
  lazy val scopt        = "com.github.scopt"      %% "scopt"         % "4.0.1"
  lazy val circeCore    = "io.circe"              %% "circe-core"    % circeVersion
  lazy val circeGeneric = "io.circe"              %% "circe-generic" % circeVersion
  lazy val circeParser  = "io.circe"              %% "circe-parser"  % circeVersion

  lazy val hadoop = ("org.apache.hadoop" % "hadoop-common" % "2.10.1")
    .exclude("asm", "asm")
    .exclude("org.mortbay.jetty", "servlet-api")

  lazy val dependencies: Seq[ModuleID] = Seq(
    aws,
    cats,
    circeCore,
    circeGeneric,
    circeParser,
    hadoop,
    scalaTest,
    scalaMock,
    scopt
  )

}
