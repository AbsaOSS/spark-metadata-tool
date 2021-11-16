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
  val log4jVersion = "2.14.1"

  lazy val aws          = "software.amazon.awssdk"   % "s3"            % "2.17.55"
  lazy val cats         = "org.typelevel"           %% "cats-core"     % "2.3.0"
  lazy val circeCore    = "io.circe"                %% "circe-core"    % circeVersion
  lazy val circeGeneric = "io.circe"                %% "circe-generic" % circeVersion
  lazy val circeParser  = "io.circe"                %% "circe-parser"  % circeVersion
  lazy val log4j        = "org.apache.logging.log4j" % "log4j-core"    % log4jVersion
  lazy val log4jApi     = "org.apache.logging.log4j" % "log4j-api"     % log4jVersion
  lazy val log4s        = "org.log4s"               %% "log4s"         % "1.8.2"
  lazy val scalaTest    = "org.scalatest"           %% "scalatest"     % "3.2.9" % Test
  lazy val scalaMock    = "org.scalamock"           %% "scalamock"     % "5.1.0" % Test
  lazy val scopt        = "com.github.scopt"        %% "scopt"         % "4.0.1"

  lazy val hadoopCommon = ("org.apache.hadoop" % "hadoop-common" % "2.10.1")
    .exclude("asm", "asm")
    .exclude("org.mortbay.jetty", "servlet-api")
  lazy val hadoopHdfs = ("org.apache.hadoop" % "hadoop-hdfs" % "2.10.1")
    .exclude("asm", "asm")
    .exclude("org.mortbay.jetty", "servlet-api")
  lazy val hadoopMiniCluster = ("org.apache.hadoop" % "hadoop-minicluster" % "2.10.1" % Test)

  lazy val dependencies: Seq[ModuleID] = Seq(
    aws,
    cats,
    circeCore,
    circeGeneric,
    circeParser,
    hadoopCommon,
    hadoopHdfs,
    hadoopMiniCluster,
    log4j,
    log4jApi,
    log4s,
    scalaTest,
    scalaMock,
    scopt
  )

}
