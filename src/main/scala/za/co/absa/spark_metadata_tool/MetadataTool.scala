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
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.model.AppError
import za.co.absa.spark_metadata_tool.model.FileLine
import za.co.absa.spark_metadata_tool.model.JsonLine
import za.co.absa.spark_metadata_tool.model.NotFoundError
import za.co.absa.spark_metadata_tool.model.StringLine

import scala.collection.immutable.ListMap
import scala.util.Try

class MetadataTool(io: FileManager) {

  private implicit val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

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
    lines  <- io.readAllLines(path)
    parsed <- lines.map(parseLine).asRight
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

  /** Saves the data into a file on provided path. If the file already exists, its contents will be overwritten.
    *
    * @param path
    *   where to save the file
    * @param data
    *   sequence of lines to be written
    * @return
    *   Unit or an error
    */
  def saveFile(path: Path, data: Seq[FileLine]): Either[AppError, Unit] = io.write(path, data.map(FileLine.write))

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
    key                 <- firstPartition.flatMap(_.split("=").headOption).asRight
  } yield key

  def backupFile(path: Path): Either[AppError, Unit] =
    io.copy(path, new Path(path.toString.replaceFirst(SparkMetadataDir, BackupDir)))

  private def getPathFromMetaFile(path: Path): Either[AppError, Path] = for {
    metaFiles <- io.listFiles(path)
    // avoid loading .compact files due to their potential size
    file <- metaFiles
              .find(!_.getName.endsWith("compact"))
              .toRight(NotFoundError(s"Couldn't find standard metadata file to load in $metaFiles"))
    parsed <- loadFile(file)
    pathString <- parsed.collectFirst { case l: JsonLine =>
                    l.fields.get("path").toRight(NotFoundError(s"Couldn't find path in JSON line ${FileLine.write(l)}"))
                  }.toRight(NotFoundError(s"Couldn't find any JSON line in file $file"))
    path <- pathString.map(new Path(_))
  } yield path

  private def parseLine(line: String): FileLine =
    Try(mapper.readValue(line, classOf[ListMap[String, String]])).fold(_ => StringLine(line), json => JsonLine(json))

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
    oldPath <-
      line.fields
        .get("path")
        .fold(
          NotFoundError(s"Couldn't find path in JSON line ${FileLine.write(line)}").asLeft: Either[NotFoundError, Path]
        )(
          new Path(_).asRight
        )
    newPath <- fixPath(
                 oldPath,
                 newBasePath,
                 firstPartitionKey
               )
    fixedLine: ListMap[String, String] = line.fields.map { case (k, v) =>
                                           if (k == "path") (k, newPath.toString) else (k, v)
                                         }
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

}
