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
import za.co.absa.spark_metadata_tool.model.Hdfs
import za.co.absa.spark_metadata_tool.model.S3
import za.co.absa.spark_metadata_tool.model.Unix

import scala.util.chaining._

object Application extends App {
  implicit private val logger: Logger = org.log4s.getLogger

  //TODO: proper error handling
  run(args).leftMap(err => logger.error(err.toString))

  def run(args: Array[String]): Either[AppError, Unit] = for {
    (conf, io, tool) <- init(args).tap(_.logInfo("Initialized application"))
    metaPath          = new Path(s"${conf.path}/$SparkMetadataDir")
    filesToFix       <- io.listFiles(metaPath).tap(_.logInfo(s"Checked ${metaPath.toString} for Spark metadata files"))
    key <- tool
             .tap(_ => logger.debug("Trying to determine first partition key"))
             .getFirstPartitionKey(conf.path)
    _ <-
      filesToFix
        .traverse(path => fixFile(path, tool, conf.path, key))
        .tap(_.logInfo("Fixed all files"))
    backupPath = new Path(s"${conf.path}/$BackupDir")
    _ <- if (conf.keepBackup) { ().asRight }
         else { deleteBackup(backupPath, io).tap(_.logInfo(s"Deleted backup from $backupPath")) }
  } yield ()

  private def init(args: Array[String]): Either[AppError, (AppConfig, FileManager, MetadataTool)] = for {
    config <- ArgumentParser.createConfig(args)
    io      = initFileManager(config)
  } yield (config, io, new MetadataTool(io))

  private def deleteBackup(backupDir: Path, io: FileManager): Either[AppError, Unit] = for {
    backupFiles <- io.listFiles(backupDir).tap(_.logDebug(s"Checked ${backupDir.toString} for backup files to delete"))
    _           <- io.delete(backupFiles).tap(_.logDebug(s"Deleted backup files in ${backupDir.toString}"))
    _           <- io.delete(Seq(backupDir)).tap(_.logDebug(s"Deleted backup directory : ${backupDir.toString}"))
  } yield ()

  private def fixFile(
    path: Path,
    metaTool: MetadataTool,
    newBasePath: Path,
    firstPartitionKey: Option[String]
  ): Either[AppError, Unit] = (for {
    parsed <- metaTool.loadFile(path)
    _      <- metaTool.backupFile(path)
    fixed <-
      metaTool.fixPaths(parsed, newBasePath, firstPartitionKey).tap(_.logDebug(s"Fixed paths in file ${path.toString}"))
    _ <- metaTool.saveFile(path, fixed)
  } yield ()).tap(_.logInfo(s"Done processing file ${path.toString}"))

  //TODO: implement remaining file managers
  private def initFileManager(config: AppConfig): FileManager = (config.filesystem match {
    case Unix => UnixFileManager
    case Hdfs => throw new NotImplementedError
    case S3   => S3FileManager(S3Client.builder().build())
  }).tap(fm => logger.debug(s"Initialized file manager : $fm"))

}
