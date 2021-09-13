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

import org.apache.hadoop.fs.Path
import scopt.OParser
import za.co.absa.spark_metadata_tool.model.AppConfig
import za.co.absa.spark_metadata_tool.model.AppError
import za.co.absa.spark_metadata_tool.model.Hdfs
import za.co.absa.spark_metadata_tool.model.S3
import za.co.absa.spark_metadata_tool.model.TargetFilesystem
import za.co.absa.spark_metadata_tool.model.Unix
import za.co.absa.spark_metadata_tool.model.UnknownError

object ArgumentParser {
  implicit val filesystemRead: scopt.Read[TargetFilesystem] = scopt.Read.reads {
    case "unix" => Unix
    case "hdfs" => Hdfs
    case "s3"   => S3
    case fs     => throw new NoSuchElementException(s"Unrecognized filesystem $fs")
  }

  implicit val hadoopPathRead: scopt.Read[Path] = scopt.Read.reads {
    case s if !s.isEmpty => new Path(s)
    case s               => throw new NoSuchElementException(s"$s is not a valid path")
  }

  val builder = OParser.builder[AppConfig]

  val parser = {
    import builder._
    OParser.sequence(
      programName("spark-metadata-tool"),
      head("spark-metadata-tool", "0.1.0-SNAPSHOT PLACEHOLDER"),
      opt[Path]('p', "path")
        .required()
        .action((x, c) => c.copy(path = x))
        .text("path text"),
      opt[TargetFilesystem]('f', "filesystem")
        .required()
        .action((x, c) => c.copy(filesystem = x))
        .text("unix/hdfs/s3")
    )
  }

  def createConfig(args: Array[String]): Either[AppError, AppConfig] = {
    val parseResult = OParser.parse(
      parser,
      args,
      AppConfig(
        new Path("default"),
        Unix
      )
    )

    parseResult.toRight(UnknownError("Unknown error when parsing arguments"))
  }

}
