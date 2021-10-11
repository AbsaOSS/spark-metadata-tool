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

package za.co.absa.spark_metadata_tool

import cats.implicits._
import org.apache.hadoop.fs.Path
import scopt.OParser
import za.co.absa.spark_metadata_tool.model.AppConfig
import za.co.absa.spark_metadata_tool.model.AppError
import za.co.absa.spark_metadata_tool.model.Hdfs
import za.co.absa.spark_metadata_tool.model.S3
import za.co.absa.spark_metadata_tool.model.TargetFilesystem
import za.co.absa.spark_metadata_tool.model.Unix
import za.co.absa.spark_metadata_tool.model.UnknownError
import za.co.absa.spark_metadata_tool.model.UnknownFileSystemError

object ArgumentParser {

  implicit val hadoopPathRead: scopt.Read[Path] = scopt.Read.reads {
    case s if s.nonEmpty => new Path(s)
    case s               => throw new NoSuchElementException(s"$s is not a valid path")
  }

  private val builder = OParser.builder[AppConfig]

  private val parser = {
    import builder._
    OParser.sequence(
      programName("spark-metadata-tool"),
      head("spark-metadata-tool", "0.1.0-SNAPSHOT PLACEHOLDER"),
      opt[Path]('p', "path")
        .required()
        .action((x, c) => c.copy(path = x))
        .text("path text"),
      opt[Unit]("keep-backup")
        .action((_, c) => c.copy(keepBackup = true))
        .text("keep backup")
    )
  }

  def createConfig(args: Array[String]): Either[AppError, AppConfig] = {
    val parseResult = OParser.parse(
      parser,
      args,
      AppConfig(
        path = new Path("default"),
        filesystem = Unix,
        keepBackup = false
      )
    )

    parseResult.fold(Left(UnknownError("Unknown error when parsing arguments")): Either[AppError, AppConfig]) { conf =>
      val fs = getFsFromPath(conf.path.toString)
      fs.map(fs => conf.copy(filesystem = fs))
    }
  }

  private def getFsFromPath(path: String): Either[UnknownFileSystemError, TargetFilesystem] = path match {
    case _ if path.startsWith("/")       => Unix.asRight
    case _ if path.startsWith("hdfs://") => Hdfs.asRight
    case _ if path.startsWith("s3://")   => S3.asRight
    case _ =>
      UnknownFileSystemError(
        s"Couldn't extract filesystem from path $path"
      ).asLeft
  }

}
