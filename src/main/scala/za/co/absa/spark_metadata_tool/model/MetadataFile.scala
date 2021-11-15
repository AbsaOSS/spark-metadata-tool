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

package za.co.absa.spark_metadata_tool.model

import cats.implicits._
import org.apache.hadoop.fs.Path

import scala.util.Try

case class MetadataFile(
  numericName: Int,
  compact: Boolean,
  path: Path
) {
  override def toString: String = path.toString
}

object MetadataFile {
  def fromPath(path: Path): Either[AppError, MetadataFile] = {
    val splitName = path.getName.split('.')
    for {
      parsedName <-
        Try(splitName.headOption.map(_.toInt)).toEither.leftMap(err =>
          ParsingError(
            s"Couldn't parse name `${splitName.headOption}` of the file ${path.getName}, expected numeric value",
            err.some
          )
        )
      numericName <-
        parsedName.toRight(
          ParsingError(s"Couldn't parse filename ${path.getName}, split on `.` returned an empty array", None)
        )
      compact = splitName.size > 1
    } yield MetadataFile(numericName, compact, path)

  }

  implicit def orderingByName[A <: MetadataFile]: Ordering[A] =
    Ordering.by(e => (e.numericName, e.compact))

}
