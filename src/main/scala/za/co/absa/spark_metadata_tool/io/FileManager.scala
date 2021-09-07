package za.co.absa.spark_metadata_tool.io

import za.co.absa.spark_metadata_tool.model.IoError

//TODO: refactor list* into single parametrized method
trait FileManager {
  def listFiles(path: String): Either[IoError, Seq[String]]
  def listDirs(path: String): Either[IoError, Seq[String]]
  def readAllLines(path: String): Either[IoError, Seq[String]]
  def write(path: String, lines: Seq[String]): Either[IoError, Unit]
  def delete(path: String): Either[IoError, Unit]
  def move(from: String, to: String): Either[IoError, Unit]
}
