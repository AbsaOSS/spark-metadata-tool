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
import org.apache.log4j.FileAppender
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.PatternLayout
import org.log4s.Logger
import scopt.OParser
import za.co.absa.spark_metadata_tool.LoggingImplicits._
import za.co.absa.spark_metadata_tool.model.AppConfig
import za.co.absa.spark_metadata_tool.model.AppError
import za.co.absa.spark_metadata_tool.model.ArgumentParserError
import za.co.absa.spark_metadata_tool.model.Hdfs
import za.co.absa.spark_metadata_tool.model.InitializationError
import za.co.absa.spark_metadata_tool.model.S3
import za.co.absa.spark_metadata_tool.model.TargetFilesystem
import za.co.absa.spark_metadata_tool.model.Unix
import za.co.absa.spark_metadata_tool.model.UnknownFileSystemError

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try
import scala.util.chaining._

object ArgumentParser {
  implicit private val logger: Logger = org.log4s.getLogger

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
        .text("keep backup"),
      opt[Unit]('v', "verbose")
        .action((_, c) => c.copy(verbose = true))
        .text("verbose"),
      opt[Unit]("log-to-file")
        .action((_, c) => c.copy(logToFile = true))
        .text("logtofile"),
      opt[Unit]("dry-run")
        .action((_, c) => c.copy(dryRun = true))
        .text("dry run")
    )
  }

  def createConfig(args: Array[String]): Either[AppError, AppConfig] = {
    val parseResult = OParser.parse(
      parser,
      args,
      AppConfig(
        path = new Path("default"),
        filesystem = Unix,
        keepBackup = false,
        verbose = false,
        logToFile = false,
        dryRun = false
      )
    )

    parseResult
      .fold(Left(ArgumentParserError("Couldn't parse provided arguments")): Either[AppError, AppConfig]) { conf =>
        for {
          _  <- initLogging(conf.verbose, conf.logToFile).tap(_.logDebug("Initialized logging"))
          fs <- getFsFromPath(conf.path.toString).tap(_.logValueDebug("Derived filesystem from path"))
        } yield conf.copy(
          filesystem = fs
        )
      }
      .tap(_.logValueDebug("Initialized application config"))
  }

  def initLogging(verbose: Boolean, logToFile: Boolean): Either[AppError, Unit] = Try {

    if (verbose) {
      LogManager.getRootLogger.setLevel(Level.DEBUG)
      LogManager.getLogger("org.apache.http").setLevel(Level.INFO)
      LogManager.getLogger("software.amazon.awssdk").setLevel(Level.INFO)
    }

    if (logToFile) {
      val fa = new FileAppender()
      val ts = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
      fa.setName("FileAppender")
      fa.setFile(s"metadatatool-$ts.log")
      fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"))
      fa.setThreshold(Level.DEBUG)
      fa.setAppend(true)
      fa.activateOptions()
      LogManager.getRootLogger.addAppender(fa)
    }
  }.toEither.leftMap(err => InitializationError(s"Failed to init logging: ${err.getMessage}", err.some))

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
