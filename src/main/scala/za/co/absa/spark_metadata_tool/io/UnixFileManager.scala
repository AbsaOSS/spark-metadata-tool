package za.co.absa.spark_metadata_tool.io

import za.co.absa.spark_metadata_tool.model.IoError
import scala.io.Source
import scala.util.Using
import java.io.PrintWriter
import java.io.FileOutputStream
import java.io.File
import scala.util.Try

case object UnixFileManager extends FileManager {

  //TODO: check if safe(listFiles might need to be option)
  override def listFiles(path: String): Either[IoError, Seq[String]] = {
    val dir = new File(path)

    checkDirectoryExists(dir) match {
      case Right(true)  => Right(dir.listFiles().filter(_.isFile).toSeq.map(_.getAbsolutePath))
      case Right(false) => Left(IoError(s"$path does not exist or is not a directory"))
      case Left(error)  => Left(error)
    }
  }

  override def delete(path: String): Either[IoError, Unit]           = ???
  override def move(from: String, to: String): Either[IoError, Unit] = ???

  override def readAllLines(path: String): Either[IoError, Seq[String]] = Using(Source.fromFile(path)) { src =>
    src.getLines().toSeq
  }.fold(err => Left(IoError(err.getMessage())), lines => Right(lines))

  //overwrites by default
  override def write(path: String, lines: Seq[String]): Either[IoError, Unit] =
    Using(new PrintWriter(new FileOutputStream(new File(path)))) { writer =>
      writer.write(lines.mkString("\n"))
    }.fold(err => Left(IoError(err.getMessage())), _ => Right(()))

  private def checkDirectoryExists(directory: File): Either[IoError, Boolean] = Try {
    directory.exists && directory.isDirectory
  }.fold(err => Left(IoError(err.getMessage())), res => Right(res))
}
