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

import cats.implicits._
import org.apache.hadoop.fs.Path
import org.log4s.Logger
import za.co.absa.spark_metadata_tool.LoggingImplicits._
import za.co.absa.spark_metadata_tool.model.IoError

import java.io.File
import java.io.FileOutputStream
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import scala.io.Source
import scala.util.Try
import scala.util.Using
import scala.util.chaining._

case object UnixFileManager extends FileManager {
  implicit private val logger: Logger = org.log4s.getLogger

  override def listFiles(path: Path): Either[IoError, Seq[Path]] =
    listDirectory(path)
      .map(_.filter(_.isFile).map(f => new Path(f.getAbsolutePath)))
      .tap(_.logValueDebug(s"Listed files in ${path.toString}"))

  override def listDirectories(path: Path): Either[IoError, Seq[Path]] =
    listDirectory(path)
      .map(_.filter(_.isDirectory).map(d => new Path(d.getAbsolutePath)))
      .tap(_.logValueDebug(s"Listed directories in ${path.toString}"))

  override def readAllLines(path: Path): Either[IoError, Seq[String]] = Using(Source.fromFile(path.toString)) { src =>
    src.getLines().toSeq
  }.fold(err => Left(IoError(err.getMessage, err.some)), lines => Right(lines))

  // overwrites by default
  override def write(path: Path, lines: Seq[String]): Either[IoError, Unit] =
    Using(new PrintWriter(new FileOutputStream(new File(path.toString)))) { writer =>
      writer.write(lines.mkString("\n"))
    }.fold(err => Left(IoError(err.getMessage, err.some)), _ => Right(()))

  override def copy(origin: Path, destination: Path): Either[IoError, Unit] = Try {

    val bkpDir = Paths.get(withScheme(destination.getParent).toUri)
    val from   = Paths.get(withScheme(origin).toUri)
    val to     = Paths.get(withScheme(destination).toUri)

    if (Files.notExists(bkpDir)) {
      Files.createDirectories(bkpDir)
    }

    Files.copy(from, to)
  }.toEither.map(_ => ()).leftMap(err => IoError(err.getMessage, err.some))

  def delete(paths: Seq[Path]): Either[IoError, Unit] = Try {
    paths.foreach(p => Files.delete(Paths.get(withScheme(p).toUri)))
  }.toEither.map(_ => ()).leftMap(err => IoError(err.getMessage, err.some))

  private def listDirectory(path: Path): Either[IoError, Seq[File]] = {
    val dir = new File(path.toString)

    checkDirectoryExists(dir) match {
      case Right(true) =>
        Try(dir.listFiles()).fold(
          err => IoError(err.getMessage, err.some).asLeft,
          files => files.toSeq.asRight
        )
      case Right(false) => IoError(s"$path does not exist or is not a directory", None).asLeft
      case Left(error)  => error.asLeft
    }
  }

  private def checkDirectoryExists(directory: File): Either[IoError, Boolean] = Try {
    directory.exists && directory.isDirectory
  }.fold(err => Left(IoError(err.getMessage, err.some)), res => Right(res))

  private def withScheme(path: Path): Path = new Path(s"file:///${path.toString}")
}
