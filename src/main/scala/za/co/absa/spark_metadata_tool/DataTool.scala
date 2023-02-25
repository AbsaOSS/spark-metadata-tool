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

package za.co.absa.spark_metadata_tool

import org.apache.hadoop.fs.{FileStatus, Path}
import org.log4s.Logger
import za.co.absa.spark_metadata_tool.LoggingImplicits.EitherOps
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.model.IoError

import scala.util.chaining.scalaUtilChainingOps

class DataTool(io: FileManager) {
  import za.co.absa.spark_metadata_tool.DataTool._

  def listDataFileStatuses(path: Path): Either[IoError, Seq[FileStatus]] =
    for {
      statuses <- io.walkFileStatuses(path, dataFileFilter).tap(_.logValueDebug("Listed file statuses"))
      _        <- Either.cond(statuses.nonEmpty, (), IoError(s"No data files found in $path", None))
    } yield statuses
}

object DataTool {
  private implicit val logger: Logger = org.log4s.getLogger

  private[spark_metadata_tool] def dataFileFilter(filePath: Path): Boolean =
    filePath.toUri.getPath.endsWith(".parquet") && isVisible(filePath)

  private def isVisible(filePath: Path): Boolean =
    !hiddenRe.pattern.matcher(filePath.toUri.getPath).find()

  private val hiddenRe = "/[_.]".r
}
