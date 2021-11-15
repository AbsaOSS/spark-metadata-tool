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
import org.log4s.Logger
import software.amazon.awssdk.services.s3.S3Client
import za.co.absa.spark_metadata_tool.LoggingImplicits._
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.io.S3FileManager
import za.co.absa.spark_metadata_tool.io.UnixFileManager
import za.co.absa.spark_metadata_tool.model.AppConfig
import za.co.absa.spark_metadata_tool.model.AppError
import za.co.absa.spark_metadata_tool.model.AppErrorWithThrowable
import za.co.absa.spark_metadata_tool.model.FixPaths
import za.co.absa.spark_metadata_tool.model.Hdfs
import za.co.absa.spark_metadata_tool.model.InitializationError
import za.co.absa.spark_metadata_tool.model.Merge
import za.co.absa.spark_metadata_tool.model.NotFoundError
import za.co.absa.spark_metadata_tool.model.S3
import za.co.absa.spark_metadata_tool.model.TargetFilesystem
import za.co.absa.spark_metadata_tool.model.Unix
import za.co.absa.spark_metadata_tool.model.UnknownError

import scala.util.Try
import scala.util.chaining._

object Application extends App {
  implicit private val logger: Logger = org.log4s.getLogger

  run(args).leftMap {
    case err: AppErrorWithThrowable => err.ex.fold(logger.error(err.toString))(e => logger.error(e)(err.msg))
    case err: AppError              => logger.error(err.toString)
  }

  def run(args: Array[String]): Either[AppError, Unit] = for {
    (conf, io, tool) <- init(args).tap(_.logInfo("Initialized application"))
    _ <- conf.mode match {
           case FixPaths => fixPaths(conf, io, tool)
           case Merge    => mergeMetadataFiles(conf, io, tool)
         }
    backupPath = new Path(s"${conf.path}/$BackupDir")
    _         <- if (conf.keepBackup) ().asRight else tool.deleteBackup(backupPath, conf.dryRun)
  } yield ()

  private def fixPaths(config: AppConfig, io: FileManager, tool: MetadataTool): Either[AppError, Unit] = {
    val metaPath = new Path(s"${config.path}/$SparkMetadataDir")

    for {
      filesToFix <- io.listFiles(metaPath).tap(_.logInfo(s"Checked ${metaPath.toString} for Spark metadata files"))
      key <- tool
               .tap(_ => logger.debug("Trying to determine first partition key"))
               .getFirstPartitionKey(config.path)
      _ <-
        filesToFix
          .traverse(path => fixFile(path, tool, config.path, config.dryRun, key))
          .tap(_.logInfo("Fixed all files"))
    } yield ()
  }

  private def mergeMetadataFiles(config: AppConfig, io: FileManager, tool: MetadataTool): Either[AppError, Unit] = {
    val newMeta = new Path(s"${config.path}/$SparkMetadataDir")

    for {
      newFiles <-
        io.listFiles(newMeta)
          .tap(_.logInfo(s"Checked the new metadata directory '${newMeta.toString}' for Spark metadata files"))
      filteredNewFiles <- tool.filterLastCompact(newFiles)
      targetFile <- filteredNewFiles.headOption
                      .toRight(NotFoundError(s"No files in target metadata folder"))
                      .tap(_.logValueInfo(s"Found target file to write merge changes"))
      oldPath <-
        config.oldPath.toRight(UnknownError(s"Path to the old data directory was not set for run mode ${config.mode}"))
      oldMeta = new Path(s"$oldPath/$SparkMetadataDir")
      oldFiles <-
        io.listFiles(oldMeta)
          .tap(_.logInfo(s"Checked the old metadata directory '${oldMeta.toString}' for Spark metadata files"))
      toMerge <-
        tool.filterLastCompact(oldFiles).tap(_.logValueInfo(s"Old files to be merged into the new metadata folder"))
      _      <- tool.backupFile(targetFile.path, config.dryRun)
      merged <- tool.merge(toMerge, targetFile)
      _      <- tool.saveFile(targetFile.path, merged, config.dryRun)
    } yield ()
  }

  private def init(args: Array[String]): Either[AppError, (AppConfig, FileManager, MetadataTool)] = for {
    config   <- ArgumentParser.createConfig(args)
    s3Client <- initS3(config.filesystem)
    io       <- initFileManager(config.filesystem, s3Client)
  } yield (config, io, new MetadataTool(io))

  private def fixFile(
    path: Path,
    metaTool: MetadataTool,
    newBasePath: Path,
    dryRun: Boolean,
    firstPartitionKey: Option[String]
  ): Either[AppError, Unit] = (for {
    _      <- logger.info(s"Processing file $path").asRight
    parsed <- metaTool.loadFile(path)
    _      <- metaTool.backupFile(path, dryRun)
    fixed <-
      metaTool.fixPaths(parsed, newBasePath, firstPartitionKey).tap(_.logDebug(s"Fixed paths in file ${path.toString}"))
    _ <- metaTool.saveFile(path, fixed, dryRun)
  } yield ()).tap(_.logInfo(s"Done processing file ${path.toString}"))

  def initS3(fs: TargetFilesystem): Either[AppError, Option[S3Client]] = fs match {
    case S3 =>
      Try {
        S3Client.builder().build().some
      }.toEither.leftMap(err => InitializationError("Failed to initialize S3 Client", err.some))
    case _ => None.asRight
  }

  private def initFileManager(fs: TargetFilesystem, s3Client: Option[S3Client]): Either[AppError, FileManager] =
    ((fs, s3Client) match {
      case (Unix, _) => UnixFileManager.asRight
      case (Hdfs, _) =>
        model
          .NotImplementedError("HDFS support not implemented yet")
          .asLeft // https://github.com/AbsaOSS/spark-metadata-tool/issues/5
      case (S3, Some(client)) => S3FileManager(client).asRight
      case (S3, None)         => NotFoundError("No S3 Client provided for S3 filesystem").asLeft
    }).tap(fm => logger.debug(s"Initialized file manager : $fm"))

}
