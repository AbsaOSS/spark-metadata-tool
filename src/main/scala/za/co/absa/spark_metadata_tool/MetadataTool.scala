package za.co.absa.spark_metadata_tool

import spray.json._
import DefaultJsonProtocol._
import za.co.absa.spark_metadata_tool.model.FileLine
import za.co.absa.spark_metadata_tool.model.StringLine
import za.co.absa.spark_metadata_tool.model.JsonLine
import scala.util.Try
import za.co.absa.spark_metadata_tool.model.AppConfig
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.model.AppError
import cats.implicits._
import za.co.absa.spark_metadata_tool.model.NotFoundError

class MetadataTool(io: FileManager, config: AppConfig) {

  val metadataDir = "_spark_metadata"

  def fixMetadataFiles: Either[AppError, Unit] = for {
    files <- io.listFiles(s"${config.path}/$metadataDir")
    key   <- getFirstPartitionKey
    _     <- files.traverse(file => processFile(file, key)) //TODO: backups and rollback on failure
  } yield ()

  private def processFile(path: String, firstPartitionKey: String): Either[AppError, Unit] = for {
    lines <- parseFile(path)
    fixed <- lines.traverse(line => processLine(line, firstPartitionKey))
    _     <- io.write(path, fixed.map(_.toString))
  } yield ()

  private def parseFile(path: String): Either[AppError, Seq[FileLine]] = for {
    lines       <- io.readAllLines(path)
    parsedLines <- Right(lines.map(parseLine(_)))
  } yield parsedLines

  private def parseLine(line: String): FileLine =
    Try(line.parseJson).fold(_ => StringLine(line), json => JsonLine(json))

  private def processLine(line: FileLine, firstPartitionKey: String): Either[AppError, FileLine] = line match {
    case StringLine(line) => StringLine(line).asRight
    case JsonLine(line) =>
      for {
        oldPath  <- line.asJsObject.fields.get("path").toRight(NotFoundError(s"Couldn't find key 'path' in $line"))
        newPath  <- fixPath(oldPath.convertTo[String], firstPartitionKey, config.path).toJson.asRight
        fixedLine = line.asJsObject.copy(fields = line.asJsObject.fields ++ Map(("path", newPath)))
      } yield JsonLine(fixedLine)
  }

  private def fixPath(path: String, key: String, newPath: String): String =
    path.replaceFirst(s".*/$key=", s"${config.filesystem.pathPrefix}${stripLeadingSlash(newPath)}/$key=")

  private def getFirstPartitionKey: Either[AppError, String] = for {
    dirs          <- io.listDirs(config.path)
    partitionDirs <- dirs.filterNot(_ == metadataDir).asRight
    key <- partitionDirs
             .map(_.split("="))
             .filter(_.length > 1)
             .headOption
             .flatMap(_.headOption)
             .toRight(NotFoundError("Couldn't find first partition key"))
  } yield key

  //FIXME: this needs to be handled better for consistency, ideally in arguement parsing, need to strip filesystem prefixes and trailing slashes as well
  private def stripLeadingSlash(path: String): String = if (path.startsWith("/")) path.drop(1) else path

}
