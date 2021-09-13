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

package za.co.absa.spark_metadata_tool

import cats.implicits._
import za.co.absa.spark_metadata_tool.io.UnixFileManager
import za.co.absa.spark_metadata_tool.model.AppError
import za.co.absa.spark_metadata_tool.model.AppConfig
import za.co.absa.spark_metadata_tool.io.FileManager
import org.apache.hadoop.fs.Path

object Application extends App {

  val metadataDir = "_spark_metadata"

  //TODO: proper error handling
  run(args).leftMap(err => throw new RuntimeException(err.toString()))

  def run(args: Array[String]): Either[AppError, Unit] = for {
    (conf, io, tool) <- init(args)
    files            <- io.listFiles(s"${conf.path}/$metadataDir").map(_.map(p => new Path(p)))
    maybeKey         <- tool.getFirstPartitionKey(new Path(conf.path))
    _                <- files.traverse(path => fixFile(path, tool, new Path(conf.path), maybeKey))
  } yield ()

  //TODO: better type than tuple?
  def init(args: Array[String]): Either[AppError, (AppConfig, FileManager, MetadataTool)] = for {
    config <- ArgumentParser.createConfig(args)
    io      = UnixFileManager //TODO: resolve according to fs in config
  } yield (config, io, new MetadataTool(io))

  def fixFile(
    path: Path,
    metaTool: MetadataTool,
    newBasePath: Path,
    firstPartitionKey: Option[String]
  ): Either[AppError, Unit] = for {
    parsed <- metaTool.loadFile(path)
    fixed  <- metaTool.fixPaths(parsed, newBasePath, firstPartitionKey)
    _      <- metaTool.saveFile(path, fixed)
  } yield ()

  println("done") //FIXME: PEM
}
