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
import _root_.io.circe.generic.auto._
import _root_.io.circe.syntax._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.log4s.Logger
import software.amazon.awssdk.services.s3.S3Client
import za.co.absa.spark_metadata_tool.LoggingImplicits._
import za.co.absa.spark_metadata_tool.io.{FileManager, HdfsFileManager, S3FileManager, UnixFileManager}
import za.co.absa.spark_metadata_tool.model.{
  Action,
  AppConfig,
  AppError,
  AppErrorWithThrowable,
  CompareMetadataWithData,
  CreateMetadata,
  FixPaths,
  Hdfs,
  InitializationError,
  IoError,
  JsonLine,
  Merge,
  NotFoundError,
  S3,
  SinkFileStatus,
  StringLine,
  TargetFilesystem,
  Unix,
  UnknownError
}

import scala.util.Try
import scala.util.chaining._
import scala.util.matching.Regex

object Application extends App {
  implicit private val logger: Logger = org.log4s.getLogger
  private val dataFileRe: Regex = ("^part-(\\d{4,})-" + // Part number
    "[0-9a-fA-F]{8}-" + // first 8 characters of uuid
    "[0-9a-fA-F]{4}-" + // 4 characters of uuid separated by -
    "[0-9a-fA-F]{4}-" +
    "[0-9a-fA-F]{4}-" +
    "[0-9a-fA-F]{12}" +           // last 12 hexadecimal characters of uuid
    "(?:\\.snappy)?\\.parquet").r // extension, that may not be compressed by snappy

  run(args).leftMap {
    case err: AppErrorWithThrowable => err.ex.fold(logger.error(err.toString))(e => logger.error(e)(err.msg))
    case err: AppError              => logger.error(err.toString)
  }

  def run(args: Array[String]): Either[AppError, Unit] = for {
    (conf, io, tool) <- init(args).tap(_.logInfo("Initialized application"))
    _ <- conf.mode match {
           case FixPaths                => fixPaths(conf, io, tool)
           case Merge                   => mergeMetadataFiles(conf, io, tool)
           case CompareMetadataWithData => compareMetadataWithData(conf, io, tool)
           case m: CreateMetadata       => createMetadata(conf, io, tool, m)
         }
    backupPath = new Path(s"${conf.path}/$BackupDir")
    _ <- conf.mode match {
           case FixPaths | Merge if !conf.keepBackup => tool.deleteBackup(backupPath, conf.dryRun)
           case _                                    => ().asRight
         }
  } yield ()

  private def fixPaths(config: AppConfig, io: FileManager, tool: MetadataTool): Either[AppError, Unit] = {
    val metaPath = new Path(s"${config.path}/$SparkMetadataDir")

    for {
      allFiles           <- io.listFiles(metaPath).tap(_.logInfo(s"Checked ${metaPath.toString} for Spark metadata files"))
      metadataFilesToFix <- tool.filterMetadataFiles(allFiles)
      key <- tool
               .tap(_ => logger.debug("Trying to determine first partition key"))
               .getFirstPartitionKey(config.path)
      _ <-
        metadataFilesToFix
          .traverse(path => fixFile(path, tool, config.path, config.dryRun, key))
          .tap(_.logInfo("Fixed all files"))
    } yield ()
  }

  private def mergeMetadataFiles(config: AppConfig, io: FileManager, tool: MetadataTool): Either[AppError, Unit] = {
    val newMeta = new Path(s"${config.path}/$SparkMetadataDir")

    for {
      allNewFiles <-
        io.listFiles(newMeta)
          .tap(_.logInfo(s"Checked the new metadata directory '${newMeta.toString}' for Spark metadata files"))
      newMetadataFiles <- tool.filterMetadataFiles(allNewFiles)
      filteredNewFiles <- tool.filterLastCompact(newMetadataFiles)
      targetFile <- filteredNewFiles.headOption
                      .toRight(NotFoundError(s"No files in target metadata folder"))
                      .tap(_.logValueInfo(s"Found target file to write merge changes"))
      oldPath <-
        config.oldPath.toRight(UnknownError(s"Path to the old data directory was not set for run mode ${config.mode}"))
      oldMeta = new Path(s"$oldPath/$SparkMetadataDir")
      allOldFiles <-
        io.listFiles(oldMeta)
          .tap(_.logInfo(s"Checked the old metadata directory '${oldMeta.toString}' for Spark metadata files"))
      oldMetadataFiles <- tool.filterMetadataFiles(allOldFiles)
      toMerge <-
        tool
          .filterLastCompact(oldMetadataFiles)
          .tap(_.logValueInfo(s"Old files to be merged into the new metadata folder"))
      _      <- tool.backupFile(targetFile.path, config.dryRun)
      merged <- tool.merge(toMerge, targetFile)
      _      <- tool.saveFile(targetFile.path, merged, config.dryRun)
    } yield ()
  }

  private def compareMetadataWithData(
    config: AppConfig,
    io: FileManager,
    tool: MetadataTool
  ): Either[AppError, Unit] = {
    val dataPath = config.path
    val metaPath = new Path(s"$dataPath/$SparkMetadataDir")

    for {
      metaPaths       <- io.listFiles(metaPath)
      usedMetaFiles   <- tool.filterLastCompact(metaPaths)
      metaRecords     <- usedMetaFiles.flatTraverse(metaFile => tool.getMetaRecords(metaFile.path))
      filesInInputDir <- tool.listFilesRecursively(dataPath)
      dataFiles       <- filesInInputDir.filter(_.toString.endsWith(".parquet")).asRight
    } yield {
      val add                 = "add"
      val delete              = "delete"
      val metaRecordsByAction = metaRecords.groupMap(_.action)(_.path)

      val deleteRecords = metaRecordsByAction.getOrElse(delete, Seq.empty)
      val addRecords    = metaRecordsByAction.getOrElse(add, Seq.empty).filter(record => !deleteRecords.contains(record))
      val otherRecords  = metaRecordsByAction.filter(record => record._1 != add && record._1 != delete).values.flatten

      val notDeletedData = deleteRecords.filter(dataFiles.contains)
      val missingData    = addRecords.diff(dataFiles)
      val unknownData    = dataFiles.diff(addRecords)
      val noDataIssueDetected =
        notDeletedData.isEmpty && missingData.isEmpty && unknownData.isEmpty

      if (noDataIssueDetected) {
        logger.info("No issue detected in data and metadata")
      } else {
        logger.error("Data issue detected")
      }
      printDetectedDataIssues(notDeletedData, missingData, unknownData, otherRecords)
    }
  }

  private def createMetadata(
    config: AppConfig,
    io: FileManager,
    tool: MetadataTool,
    createMetadata: CreateMetadata
  ): Either[AppError, Unit] = {
    val metadataDir = new Path(config.path, SparkMetadataDir)
    for {
      _            <- io.makeDir(metadataDir)
      allDataFiles <- io.listFiles(config.path).map(_.flatMap(p => dataFilePartNumber(p).map((p, _))).sortBy(_._2))
      maxUBatchNum =
        math.min(createMetadata.maxMicroBatchNumber, allDataFiles.lastOption.map(_._2).getOrElse(0)) // TODO check this!
      lastCompaction = maxUBatchNum - (maxUBatchNum % createMetadata.compactionNumber) - 1
      dataFiles      = allDataFiles.filter(_._2 <= maxUBatchNum)
      _             <- checkContainsAllDataFiles(dataFiles)
      statuses <- dataFiles
                    .map(df => io.getFileStatus(df._1).map(s => (s, df._2)))
                    .sequence
                    .map(_.map(s => (SinkFileStatus.from(s._1, Action.Add), s._2)))
      compactionStatusLines =
        statuses.filter(_._2 < lastCompaction).map(_._1.asJson).map(JsonLine).toList.prepended(StringLine("v1"))
      _ <- tool.saveFile(new Path(metadataDir, s"$lastCompaction.compact"), compactionStatusLines, config.dryRun)
      nonCompactedLines =
        statuses.filter(_._2 >= lastCompaction).map(s => (Seq(StringLine("v1"), JsonLine(s._1.asJson)), s._2.toString))
      _ <- nonCompactedLines.map(s => tool.saveFile(new Path(metadataDir, s._2), s._1, config.dryRun)).sequence
    } yield ()
  }

  private[spark_metadata_tool] def dataFilePartNumber(path: Path): Option[Int] =
    dataFileRe
      .findFirstMatchIn(path.getName)
      .map(_.group(1))
      .map(_.toInt)

  private[spark_metadata_tool] def checkContainsAllDataFiles(dataFiles: Seq[(Path, Int)]): Either[IoError, Unit] =
    if (dataFiles.isEmpty) {
      IoError("directory doesn't seem to contain any datafiles", None).asLeft
    } else {
      val diff: Set[Int] = (0 to dataFiles.last._2).toSet -- dataFiles.map(_._2)
      if (diff.nonEmpty) {
        IoError(s"Data directory is missing parts: ${diff.mkString(",")}", None).asLeft
      } else {
        Right(())
      }
    }

  private def stripExtensions(pth: Path): String =
    pth.getName.takeWhile(_ == '.')

  private def printDetectedDataIssues(
    notDeletedData: Iterable[Path],
    missingData: Iterable[Path],
    unknownData: Iterable[Path],
    otherRecords: Iterable[Path]
  ): Unit = {
    if (notDeletedData.nonEmpty) {
      logger.error(s"Data that should have been deleted. START")
      notDeletedData.foreach(r => logger.error(r.toString))
      logger.error(s"Data that should have been deleted. END.")
    }
    if (missingData.nonEmpty) {
      logger.error(s"Missing data. START")
      missingData.foreach(r => logger.error(r.toString))
      logger.error(s"Missing data. END")
    }
    if (unknownData.nonEmpty) {
      logger.error(s"Unknown data. START")
      unknownData.foreach(r => logger.error(r.toString))
      logger.error(s"Unknown data. END")
    }
    if (otherRecords.nonEmpty) {
      logger.error(s"Unknown records in metadata. START")
      otherRecords.foreach(r => logger.error(r.toString))
      logger.error(s"Unknown records in metadata. END")
    }
  }

  private def init(args: Array[String]): Either[AppError, (AppConfig, FileManager, MetadataTool)] = for {
    config <- ArgumentParser.createConfig(args)
    io     <- initFileManager(config.filesystem)
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
    _      <- metaTool.verifyMetadataFileContent(path.toString, parsed)
    _      <- metaTool.backupFile(path, dryRun)
    fixed <-
      metaTool.fixPaths(parsed, newBasePath, firstPartitionKey).tap(_.logDebug(s"Fixed paths in file ${path.toString}"))
    _ <- metaTool.saveFile(path, fixed, dryRun)
  } yield ()).tap(_.logInfo(s"Done processing file ${path.toString}"))

  def initS3(): Either[AppError, S3Client] = Try {
    S3Client.builder().build()
  }.toEither.leftMap(err => InitializationError("Failed to initialize S3 Client", err.some))

  def initHdfs(): Either[AppError, FileSystem] = Try {
    val hadoopConfDir       = sys.env("HADOOP_CONF_DIR")
    val coreSiteXmlPath     = s"$hadoopConfDir/core-site.xml"
    val hdfsSiteXmlPath     = s"$hadoopConfDir/hdfs-site.xml"
    val hadoopConfiguration = new Configuration()
    hadoopConfiguration.addResource(new Path(coreSiteXmlPath))
    hadoopConfiguration.addResource(new Path(hdfsSiteXmlPath))
    UserGroupInformation.setConfiguration(hadoopConfiguration)

    FileSystem.get(hadoopConfiguration)
  }.toEither.leftMap(err => InitializationError("Failed to initialize Hdfs file system", err.some))

  private def initFileManager(fs: TargetFilesystem): Either[AppError, FileManager] =
    (fs match {
      case Unix => UnixFileManager.asRight
      case Hdfs => initHdfs().map(hdfs => HdfsFileManager(hdfs))
      case S3   => initS3().map(client => S3FileManager(client))
    }).tap(fm => logger.debug(s"Initialized file manager : $fm"))

}
