package za.co.absa.spark_metadata_tool

import scopt.OParser
import za.co.absa.spark_metadata_tool.model.AppConfig
import za.co.absa.spark_metadata_tool.model.TargetFilesystem
import za.co.absa.spark_metadata_tool.model.AppError
import za.co.absa.spark_metadata_tool.model.UnknownError
import za.co.absa.spark_metadata_tool.model.UnknownFileSystemError

object ArgumentParser {
  val builder = OParser.builder[AppConfig]

  val parser = {
    import builder._
    OParser.sequence(
      programName("spark-metadata-tool"),
      head("spark-metadata-tool", "0.1.0-SNAPSHOT PLACEHOLDER"),
      opt[String]('p', "path")
        .required()
        .action((x, c) => c.copy(path = x))
        .text("path text")
    )
  }

  def createConfig(args: Array[String]): Either[AppError, AppConfig] = {
    val parseResult = OParser.parse(
      parser,
      args,
      AppConfig(
        "",
        TargetFilesystem.Unix
      )
    )

    parseResult.fold(Left(UnknownError("Unknown error when parsing arguments")): Either[AppError, AppConfig]) { conf =>
      val maybeFs = ArgumentParser.getFsFromPath(conf.path)
      maybeFs.map(fs => conf.copy(filesystem = fs))
    }
  }

  private def getFsFromPath(path: String): Either[UnknownFileSystemError, TargetFilesystem] = path match {
    case _ if path.startsWith("/")       => Right(TargetFilesystem.Unix)
    case _ if path.startsWith("hdfs://") => Right(TargetFilesystem.Hdfs)
    case _ if path.startsWith("s3://")   => Right(TargetFilesystem.S3)
    case _                               => Left(UnknownFileSystemError(s"Couldn't extract filesystem from path $path"))
  }
}
