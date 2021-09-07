package za.co.absa.spark_metadata_tool

import scopt.OParser
import za.co.absa.spark_metadata_tool.model.AppConfig
import za.co.absa.spark_metadata_tool.model.TargetFilesystem
import za.co.absa.spark_metadata_tool.model.Unix
import za.co.absa.spark_metadata_tool.model.S3
import za.co.absa.spark_metadata_tool.model.Hdfs
import za.co.absa.spark_metadata_tool.model.AppError
import za.co.absa.spark_metadata_tool.model.UnknownError

object ArgumentParser {
  implicit val filesystemRead: scopt.Read[TargetFilesystem] = scopt.Read.reads(fs =>
    fs match {
      case "unix" => Unix
      case "hdfs" => Hdfs
      case "s3"   => S3
      case _      => throw new NoSuchElementException(s"Unrecognized filesystem $fs")
    }
  )

  val builder = OParser.builder[AppConfig]

  val parser = {
    import builder._
    OParser.sequence(
      programName("spark-metadata-tool"),
      head("spark-metadata-tool", "0.1.0-SNAPSHOT PLACEHOLDER"),
      opt[String]('p', "path")
        .required()
        .action((x, c) => c.copy(path = x))
        .text("path text"),
      opt[TargetFilesystem]('f', "filesystem")
        .required()
        .action((x, c) => c.copy(filesystem = x))
        .text("unix/hdfs/s3")
    )
  }

  def createConfig(args: Array[String]): Either[AppError, AppConfig] = {
    val parseResult = OParser.parse(
      parser,
      args,
      AppConfig(
        "",
        Unix
      )
    )

    parseResult.fold(Left(UnknownError("Unknown error when parsing arguments")): Either[AppError, AppConfig])(conf =>
      Right(conf)
    )
  }

}
