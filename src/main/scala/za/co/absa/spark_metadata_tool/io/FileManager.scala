package za.co.absa.spark_metadata_tool.io

import za.co.absa.spark_metadata_tool.model.IoError

trait FileManager {
  def listFiles(path: String): Either[IoError, Seq[String]]
  def readAllLines(path: String): Either[IoError, Seq[String]]
  def write(path: String, lines: Seq[String]): Either[IoError, Unit]
  def delete(path: String): Either[IoError, Unit]
  def move(from: String, to: String): Either[IoError, Unit]
}
