package za.co.absa.spark_metadata_tool

import za.co.absa.spark_metadata_tool.io.UnixFileManager
import cats.implicits._
import za.co.absa.spark_metadata_tool.model.AppError

object Application extends App {

  //TODO: proper error handling
  run.leftMap(err => throw new RuntimeException(err.toString()))

  def run: Either[AppError, Unit] = for {
    config <- ArgumentParser.createConfig(args)
    tool    = new MetadataTool(UnixFileManager, config)
    _      <- tool.fixMetadataFiles
  } yield ()

  println("done") //FIXME: PEM
}
