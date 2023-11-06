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

import _root_.io.circe.parser._
import _root_.io.circe.syntax._
import _root_.io.circe.generic.auto._
import cats.implicits._
import org.apache.hadoop.fs.Path
import org.log4s.Logger
import za.co.absa.spark_metadata_tool.LoggingImplicits._
import za.co.absa.spark_metadata_tool.PathDerivation._
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.model.AppError
import za.co.absa.spark_metadata_tool.model.FileLine
import za.co.absa.spark_metadata_tool.model.JsonError
import za.co.absa.spark_metadata_tool.model.JsonLine
import za.co.absa.spark_metadata_tool.model.MetadataFile
import za.co.absa.spark_metadata_tool.model.NotFoundError
import za.co.absa.spark_metadata_tool.model.StringLine
import za.co.absa.spark_metadata_tool.model.IoError
import za.co.absa.spark_metadata_tool.model.ParsingError
import za.co.absa.spark_metadata_tool.model.MetadataRecord
import za.co.absa.spark_metadata_tool.model.SinkFileStatus

import scala.util.Try
import scala.util.chaining._

class MetadataTool(io: FileManager) {
  private val compactFileSuffix       = "compact"
  implicit private val logger: Logger = org.log4s.getLogger

  /** Loads Spark Structured Streaming metadata file from specified path and parses its contents.
    *
    * Parses each line as either a String or JsObject and wraps the results into a Seq of FileLine
    *
    * @param path
    *   location of the file
    * @return
    *   sequence of parsed lines or an error
    */
  def loadFile(path: Path): Either[AppError, Seq[FileLine]] = for {
    lines  <- io.readAllLines(path).tap(_.logDebug(s"Loaded file $path"))
    parsed <- lines.map(parseLine).tap(_ => logger.debug(s"Parsed contents of file $path")).asRight
  } yield parsed

  /** Fixes paths to output files in all relevant lines.
    *
    * If the name of first partition key is provided, it splits the old paths on the key name and replaces the old base
    * path with provided new one. If a path that does not contain the key is found, returns error.
    *
    * If no partition key is provided, it assumes paths do not contain any partitions, in which case each old path is
    * fully replaced by the new base path.
    *
    * If any JSON object doesn't contain a "path" key, error is returned.
    *
    * Regular lines contained in StringLine are left untouched.
    *
    * @param data
    *   parsed lines of a single Spark structured streaming metadata file to be fixed
    * @param newBasePath
    *   path specifying current location of the data, used to fix old paths
    * @param firstPartitionKey
    *   name of the first partition key
    * @return
    *   sequence of lines with fixed paths or an error
    */
  def fixPaths(
    data: Seq[FileLine],
    newBasePath: Path,
    firstPartitionKey: Option[String]
  ): Either[AppError, Seq[FileLine]] = data.traverse(line => processLine(line, newBasePath, firstPartitionKey))

  def saveMetadata(
    dir: Path,
    statuses: Seq[SinkFileStatus],
    maxMicroBatchNum: Int,
    compactionPeriod: Int,
    dryRun: Boolean
  ): Either[AppError, Unit] = {
    val lastCompaction = Compaction.lastCompaction(maxMicroBatchNum, compactionPeriod)
    val compactedSize  = Compaction.compactedSize(statuses.length, maxMicroBatchNum, compactionPeriod)
    for {
      _ <- compactedMetadata(statuses, compactedSize, lastCompaction).map { case (lastCompaction, compacted) =>
             saveCompactedMetadata(dir, lastCompaction, compacted, dryRun)
           }.getOrElse(Right(()))
      _ <- saveLooseMetadata(dir, looseMetadata(statuses, compactedSize, lastCompaction), dryRun)
    } yield ()
  }

  private def compactedMetadata(
    statuses: Seq[SinkFileStatus],
    compactedSize: Int,
    lastCompaction: Option[Int]
  ): Option[(Int, Seq[SinkFileStatus])] =
    lastCompaction.map((_, if (compactedSize > 0) statuses.take(compactedSize) else Seq.empty))

  private def looseMetadata(
    statuses: Seq[SinkFileStatus],
    compactedSize: Int,
    lastCompacted: Option[Int]
  ): Seq[(Int, Iterable[SinkFileStatus])] =
    if (compactedSize > 0) {
      lastCompacted.map { lc =>
        statuses.drop(compactedSize).mapWithIndex((status, idx) => (lc + 1 + idx, Seq(status)))
      }.getOrElse {
        val zeroth = (0, statuses.take(compactedSize + 1))
        zeroth +: statuses.drop(compactedSize + 1).mapWithIndex((s, idx) => (idx + 1, Seq(s)))
      }
    } else {
      val lc = lastCompacted.getOrElse(-1)
      (Seq.fill(-compactedSize)(None) ++: statuses.map(Some(_))).mapWithIndex((s, idx) => (lc + 1 + idx, s))
    }

  def saveCompactedMetadata(
    dir: Path,
    compactionNum: Int,
    metadata: Seq[SinkFileStatus],
    dryRun: Boolean
  ): Either[AppError, Unit] =
    saveFile(
      new Path(dir, s"$compactionNum.$compactFileSuffix"),
      formatMetadata(metadata),
      dryRun
    )

  def saveLooseMetadata(
    dir: Path,
    metadata: Seq[(Int, Iterable[SinkFileStatus])],
    dryRun: Boolean
  ): Either[AppError, Unit] =
    metadata.map { case (batchNum, statuses) =>
      saveFile(new Path(dir, batchNum.toString), formatMetadata(statuses), dryRun)
    }.sequence
      .map(_ => ())

  private def formatMetadata(metadata: Iterable[SinkFileStatus]): Seq[FileLine] =
    StringLine("v1") +: metadata.map(_.asJson).map(JsonLine).toSeq

  /** Saves the data into a file on provided path. If the file already exists, its contents will be overwritten.
    *
    * @param path
    *   where to save the file
    * @param data
    *   sequence of lines to be written
    * @return
    *   Unit or an error
    */
  def saveFile(path: Path, data: Seq[FileLine], dryRun: Boolean): Either[AppError, Unit] = (
    if (dryRun) {
      val linesToShow = 10
      logger.info(s"Saving file ${path.toString} (showing first $linesToShow lines) :")
      logger.info(data.take(linesToShow).mkString("\n"))
      ().asRight
    } else {
      io.write(path, data.map(_.toString))
    }
  ).tap(_.logDebug(s"Saved file $path"))

  /** Finds name of the top level partition, if it exists.
    *
    * Loads example path from random metadata file and compares it with names of directories in root folder. If root
    * directory contains folder with name in form 'key=value' and such pair is found withing the loaded path, it is
    * considered to be the first partition.
    *
    * @param rootDirPath
    *   path to root directory
    * @return
    *   name of the first partition, None if it doesn't exist, error on failure
    */
  def getFirstPartitionKey(rootDirPath: Path): Either[AppError, Option[String]] = for {
    dirs                <- io.listDirectories(rootDirPath)
    partitionCandidates <- dirs.map(_.getName).filter(_.contains("=")).asRight
    pathToMeta           = new Path(s"${rootDirPath.toString}/$SparkMetadataDir")
    testPath            <- getPathFromMetaFile(pathToMeta)
    firstPartition      <- partitionCandidates.find(dir => testPath.toString.contains(dir)).asRight
    key <- firstPartition
             .flatMap(_.split("=").headOption)
             .tap {
               case None      => logger.debug("First partition key not found, assuming unpartitioned data")
               case Some(key) => logger.debug(s"Found first partition key : $key")
             }
             .asRight
  } yield key

  /** List all files in directory recursively.
    *
    * @param rootDirPath
    *   path to root directory
    * @return
    *   all files in directory and all subdirectories recursively
    */
  def listFilesRecursively(rootDirPath: Path): Either[AppError, Seq[Path]] =
    for {
      dirs          <- io.listDirectories(rootDirPath)
      files         <- io.listFiles(rootDirPath)
      filesInSubDir <- dirs.flatTraverse(dir => listFilesRecursively(dir))
    } yield {
      files ++ filesInSubDir
    }

  /** List content of directory recursively.
   *
   * @param rootDirPath
   * path to root directory
   * @return
   * content of directory and all subdirectories recursively
   */
  def listDirectoryRecursively(rootDirPath: Path): Either[AppError, Seq[Path]] =
    for {
      dirs <- io.listDirectories(rootDirPath)
      files <- io.listFiles(rootDirPath)
      filesInSubDir <- dirs.flatTraverse(dir => listDirectoryRecursively(dir))
    } yield {
      dirs ++ files ++ filesInSubDir
    }

  /** Get metadata records from a metadata file.
    *
    * @param metadataFilePath
    *   path to metadata file
    * @return
    *   metadata records from metadata file
    */
  def getMetaRecords(metadataFilePath: Path): Either[AppError, Seq[MetadataRecord]] =
    for {
      loadedFile <- loadFile(metadataFilePath)
      _          <- verifyMetadataFileContent(metadataFilePath.toString, loadedFile)
      metaRecords <- loadedFile.traverse {
                       case l: JsonLine =>
                         val path = l.cursor
                           .get[Path]("path")
                           .leftMap(_ => NotFoundError(s"Couldn't find path in JSON line ${l.toString}"))

                         val action = l.cursor
                           .get[String]("action")
                           .leftMap(_ => NotFoundError(s"Couldn't find action in JSON line ${l.toString}"))
                         for {
                           p <- path
                           a <- action
                         } yield Some(MetadataRecord(p, a))
                       case _ => Right(None)
                     }
    } yield {
      metaRecords.flatten
    }

  def backupFile(path: Path, dryRun: Boolean): Either[AppError, Unit] = (
    if (dryRun) {
      ().asRight
    } else {
      io.copy(path, new Path(path.toString.replaceFirst(SparkMetadataDir, BackupDir)))
    }
  ).tap(_.logDebug(s"Created backup of file $path"))

  def deleteBackup(backupDir: Path, dryRun: Boolean): Either[AppError, Unit] =
    if (dryRun) {
      logger.info(s"Checked ${backupDir.toString} for backup files to delete")
      logger.info(s"Deleted backup files in ${backupDir.toString}")
      logger.info(s"Deleted backup directory : ${backupDir.toString}").asRight
    } else {
      for {
        backupFiles <-
          io.listFiles(backupDir).tap(_.logDebug(s"Checked ${backupDir.toString} for backup files to delete"))
        _ <- io.delete(backupFiles).tap(_.logDebug(s"Deleted backup files in ${backupDir.toString}"))
        _ <- io.delete(Seq(backupDir)).tap(_.logDebug(s"Deleted backup directory : ${backupDir.toString}"))
      } yield ()
    }

  def merge(oldFiles: Seq[MetadataFile], targetFile: MetadataFile): Either[AppError, Seq[FileLine]] = for {
    targetFileContent <- loadFile(targetFile.path)
    _                 <- verifyMetadataFileContent(targetFile.path.toString, targetFileContent)
    version <- targetFileContent.headOption
                 .tap(v => logger.debug(s"Version value from the target file $targetFile: $v"))
                 .toRight(NotFoundError(s"No content in file ${targetFile.path}"))
    toAppend <- targetFileContent
                  .drop(1)
                  .tap(c => logger.debug(s"Will append ${c.size} lines from the target file $targetFile"))
                  .asRight
    oldFilesContent <- oldFiles.sorted.traverse(f => loadFile(f.path))
    _ <- oldFiles.zip(oldFilesContent).traverse { case (file, content) =>
           verifyMetadataFileContent(file.path.toString, content)
         }
    toPrepend <- oldFilesContent
                   .flatMap(_.drop(1))
                   .tap(c => logger.debug(s"Will merge ${c.size} lines from the old metadata files"))
                   .asRight
  } yield Seq(version) ++ toPrepend ++ toAppend

  def filterLastCompact(files: Seq[Path]): Either[AppError, Seq[MetadataFile]] = (for {
    parsedFiles <- files.traverse(MetadataFile.fromPath)
    sortedFiles  = parsedFiles.sorted
    lastCompact =
      sortedFiles.findLast(_.compact).tap(file => logger.debug(s"Last .compact file: ${file.map(_.toString)}"))
  } yield lastCompact.fold(sortedFiles)(lc => sortedFiles.dropWhile(_ != lc)))
    .tap(_.logValueDebug(s"Last .compact file (if present) and following files"))

  private def getPathFromMetaFile(path: Path): Either[AppError, Path] = for {
    files     <- io.listFiles(path)
    metaFiles <- filterMetadataFiles(files)
    // avoid loading .compact files due to their potential size
    file <- metaFiles
              .find(!_.getName.endsWith(compactFileSuffix))
              .toRight(NotFoundError(s"Couldn't find standard metadata file to load in $metaFiles"))
    parsed <- loadFile(file)
    extracted <- parsed.collectFirst { case l: JsonLine =>
                   l.cursor
                     .get[Path]("path")
                     .leftMap(_ => NotFoundError(s"Couldn't find path in JSON line ${l.toString}"))
                 }.toRight(NotFoundError(s"Couldn't find any JSON line in file $file"))
    path <- extracted.tap(_.logValueDebug(s"Extracted following path from file $file"))
  } yield path

  private def parseLine(line: String): FileLine = parse(line).fold(_ => StringLine(line), json => JsonLine(json))

  private def processLine(
    line: FileLine,
    newBasePath: Path,
    firstPartitionKey: Option[String]
  ): Either[AppError, FileLine] = line match {
    case l: StringLine => l.asRight
    case l: JsonLine   => fixJsonLine(l, newBasePath, firstPartitionKey)
  }

  private def fixJsonLine(
    line: JsonLine,
    newBasePath: Path,
    firstPartitionKey: Option[String]
  ): Either[AppError, JsonLine] = for {
    oldPath <- line.cursor
                 .get[Path]("path")
                 .leftMap(_ => NotFoundError(s"Couldn't find path in JSON line ${line.toString}"))
    newPath <- fixPath(
                 oldPath,
                 newBasePath,
                 firstPartitionKey
               )
    fixedLine <-
      line.cursor
        .downField("path")
        .set(newPath.toString.asJson)
        .top
        .toRight(JsonError(s"Error when trying to set the `path` field in line $line with cursor ${line.cursor}"))
  } yield JsonLine(fixedLine)

  private def fixPath(oldPath: Path, newBasePath: Path, firstPartitionKey: Option[String]): Either[AppError, Path] =
    firstPartitionKey.fold(new Path(s"${newBasePath.toString}/${oldPath.getName}").asRight: Either[AppError, Path]) {
      key =>
        val old   = oldPath.toString
        val fixed = old.replaceFirst(s".*/$key=", s"$newBasePath/$key=")
        if (fixed == old && !old.startsWith(newBasePath.toString)) {
          NotFoundError(
            s"Failed to fix path $oldPath! Couldn't split as partition key $key was not found in the path."
          ).asLeft
        } else {
          new Path(fixed).asRight
        }
    }

  def filterMetadataFiles(files: Seq[Path]): Either[IoError, Seq[Path]] = {
    val (metadataFiles, otherFiles) = files.partition { path =>
      val (prefix, suffix) = path.getName.span(_ != '.')
      val isPrefixNumber   = Try(prefix.toLong).map(_ => true).getOrElse(false)
      val isCompactOrEmpty = suffix.isEmpty || suffix == s".$compactFileSuffix"
      isPrefixNumber && isCompactOrEmpty
    }

    if (otherFiles.nonEmpty)
      logger.info(s"Ignored non metadata files: ${otherFiles.map(_.toString)}")

    metadataFiles.asRight[IoError]
  }

  def verifyMetadataFileContent(path: String, lines: Seq[FileLine]): Either[AppError, Unit] = {
    def verifyJsonContent(lines: Seq[FileLine]): Boolean = lines.forall {
      case line: JsonLine => line.cursor.keys.exists(_.toSeq.contains("path"))
      case _              => false
    }
    lines match {
      case (firstLine: StringLine) :: (rest: Seq[FileLine])
          if firstLine.value == "v1" && rest.nonEmpty && verifyJsonContent(rest) =>
        Right((): Unit)
      case _ => Left(ParsingError(s"File $path did not match expected format", None))
    }
  }

}
