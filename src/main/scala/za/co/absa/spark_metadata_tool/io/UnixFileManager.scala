/*
 * Copyright 2018 ABSA Group Limited
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

package za.co.absa.spark_metadata_tool.io

import cats.implicits._
import za.co.absa.spark_metadata_tool.model.IoError

import java.io.File
import java.io.FileOutputStream
import java.io.PrintWriter
import scala.io.Source
import scala.util.Try
import scala.util.Using

case object UnixFileManager extends FileManager {

  //TODO: check if safe(listFiles might need to be option)
  override def listFiles(path: String): Either[IoError, Seq[String]] = {
    val dir = new File(path)

    checkDirectoryExists(dir) match {
      case Right(true)  => dir.listFiles().filter(_.isFile).toSeq.map(_.getAbsolutePath).asRight
      case Right(false) => IoError(s"$path does not exist or is not a directory").asLeft
      case Left(error)  => error.asLeft
    }
  }

  //TODO: merge with listFiles method
  override def listDirs(path: String): Either[IoError, Seq[String]] = {
    val dir = new File(path)

    checkDirectoryExists(dir) match {
      case Right(true)  => dir.listFiles().filter(_.isDirectory()).toSeq.map(_.getName()).asRight
      case Right(false) => IoError(s"$path does not exist or is not a directory").asLeft
      case Left(error)  => error.asLeft
    }
  }

  override def delete(path: String): Either[IoError, Unit]           = ???
  override def move(from: String, to: String): Either[IoError, Unit] = ???

  override def readAllLines(path: String): Either[IoError, Seq[String]] = Using(Source.fromFile(path)) { src =>
    src.getLines().toSeq
  }.fold(err => Left(IoError(err.getMessage())), lines => Right(lines))

  // overwrites by default
  override def write(path: String, lines: Seq[String]): Either[IoError, Unit] =
    Using(new PrintWriter(new FileOutputStream(new File(path)))) { writer =>
      writer.write(lines.mkString("\n"))
    }.fold(err => Left(IoError(err.getMessage())), _ => Right(()))

  private def checkDirectoryExists(directory: File): Either[IoError, Boolean] = Try {
    directory.exists && directory.isDirectory
  }.fold(err => Left(IoError(err.getMessage())), res => Right(res))
}
