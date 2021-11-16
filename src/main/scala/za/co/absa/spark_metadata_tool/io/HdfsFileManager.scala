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

package za.co.absa.spark_metadata_tool.io

import org.log4s.Logger
import za.co.absa.spark_metadata_tool.model.IoError
import org.apache.hadoop.fs.{FileSystem, Path}
import cats.implicits._
import za.co.absa.spark_metadata_tool.LoggingImplicits._

import java.io.PrintWriter
import scala.io.Source
import scala.util.chaining._
import scala.util.{Try, Using}

case class HdfsFileManager(hdfs: FileSystem) extends FileManager {
  implicit private val logger: Logger = org.log4s.getLogger

  override def listFiles(path: Path): Either[IoError, Seq[Path]] =
    listDirectory(path)
      .map(_.filter(path => hdfs.isFile(path)))
      .tap(_.logValueDebug(s"Listed files in ${path.toString}"))

  override def listDirectories(path: Path): Either[IoError, Seq[Path]] =
    listDirectory(path)
      .map(_.filter(path => hdfs.isDirectory(path)))
      .tap(_.logValueDebug(s"Listed directories in ${path.toString}"))

  override def readAllLines(path: Path): Either[IoError, Seq[String]] =
    Using(
      Source.fromInputStream(hdfs.open(path))
    )(_.getLines().toSeq).fold(err => Left(IoError(err.getMessage, err.some)), lines => Right(lines))

  // overwrites by default
  override def write(path: Path, lines: Seq[String]): Either[IoError, Unit] =
    Using(new PrintWriter(hdfs.create(path, true))) { writer =>
      writer.write(lines.mkString("\n"))
    }.fold(err => Left(IoError(err.getMessage, err.some)), _ => Right(()))

  override def copy(origin: Path, destination: Path): Either[IoError, Unit] = Try {

    val bkpDir = destination.getParent
    val from = origin
    val to = destination

    if (hdfs.exists(bkpDir)) {
      hdfs.mkdirs(bkpDir)
    }

    org.apache.hadoop.fs.FileUtil.copy(
      hdfs,
      from,
      hdfs,
      to,
      false,
      hdfs.getConf
    )
  }.toEither.map(_ => ()).leftMap(err => IoError(err.getMessage, err.some))

  def delete(paths: Seq[Path]): Either[IoError, Unit] = Try {
    paths.foreach(p => hdfs.delete(p, false))
  }.toEither.map(_ => ()).leftMap(err => IoError(err.getMessage, err.some))

  private def listDirectory(path: Path): Either[IoError, Seq[Path]] = {
    checkDirectoryExists(path) match {
      case Right(true) =>
        Try {
          hdfs.listStatus(path).map(fileStatus => fileStatus.getPath)
        }.fold(
          err => IoError(err.getMessage, err.some).asLeft,
          files => files.toSeq.asRight
        )
      case Right(false) => IoError(s"$path does not exist or is not a directory", None).asLeft
      case Left(error) => error.asLeft
    }
  }

  private def checkDirectoryExists(directory: Path): Either[IoError, Boolean] = Try {
    hdfs.exists(directory) && hdfs.isDirectory(directory)
  }.fold(err => Left(IoError(err.getMessage, err.some)), res => Right(res))
}
