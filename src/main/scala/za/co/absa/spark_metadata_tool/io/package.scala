package za.co.absa.spark_metadata_tool

import za.co.absa.spark_metadata_tool.model.IoError
import cats.syntax.either._
import cats.syntax.option._

package object io {

  private[io] def catchAsIoError[R](resource: => R): Either[IoError, R] =
    Either.catchNonFatal(resource).leftMap(err => IoError(err.getMessage, err.some))
}
