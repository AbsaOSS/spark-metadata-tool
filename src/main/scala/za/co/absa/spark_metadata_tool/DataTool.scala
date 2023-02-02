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

import org.apache.hadoop.fs.Path
import org.log4s.Logger
import za.co.absa.spark_metadata_tool.LoggingImplicits.EitherOps
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.model.IoError

import scala.util.chaining.scalaUtilChainingOps

class DataTool(io: FileManager) {
  import za.co.absa.spark_metadata_tool.DataTool._

  def listDataFilesUpToPart(path: Path, maxPartNumber: Int): Either[IoError, Seq[Path]] =
    listDataFiles(path).map(_.take(maxPartNumber))

  def listDataFiles(path: Path): Either[IoError, Seq[Path]] =
    for {
      dataFiles <- io.walkFiles(path, dataFileFilter)
                     .tap(_.logValueDebug("Listing data"))
      _ <- Either.cond(dataFiles.nonEmpty, (), IoError(s"No data files found in $path", None))
    } yield dataFiles
}

object DataTool {
  private implicit val logger: Logger = org.log4s.getLogger

  private[spark_metadata_tool] def dataFileFilter(filePath: Path): Boolean =
    filePath.toUri.getPath.endsWith(".parquet") && isVisible(filePath)

  private def isVisible(filePath: Path): Boolean =
    !hiddenRe.pattern.matcher(filePath.toUri.getPath).find()

  private val hiddenRe = "/[_.]".r
}
