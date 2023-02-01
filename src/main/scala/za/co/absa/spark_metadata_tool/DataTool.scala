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
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.model.IoError

import scala.util.matching.Regex

class DataTool(io: FileManager) {
  import za.co.absa.spark_metadata_tool.DataTool._

  def listDataFilesUpToPart(path: Path, maxPartNumber: Int): Either[IoError, Seq[Path]] =
    listDataFiles(path).map(_.take(maxPartNumber))

  def listDataFiles(path: Path): Either[IoError, Seq[Path]] =
    for {
      dataFiles <- io.walkFiles(path, mkDataFileFilter(path)).map(_.sortBy(_.toUri))
      _         <- Either.cond(dataFiles.nonEmpty, (), IoError(s"No data files found in $path", None))
    } yield dataFiles
}

object DataTool {

  private[spark_metadata_tool] def mkDataFileFilter(basePath: Path): Path => Boolean = {
    val isDataFile = mkPartitionedFileFilter(basePath)
    (filePath: Path) => isVisible(filePath) && isDataFile(filePath)
  }
  private def isVisible(filePath: Path): Boolean =
    !hiddenRe.matches(filePath.toString)

  private val hiddenRe = "^_|^\\.|/_|/\\.".r

  private def mkPartitionedFileFilter(basePath: Path): Path => Boolean = {
    val partFilter = mkDataFileRe(basePath)
    (filePath: Path) => partFilter.matches(filePath.toString)
  }

  private def mkDataFileRe(basePath: Path): Regex =
    new Regex(s"${Regex.quote(basePath.toString)}$partitionRe/$dataFileRe")

  private val partitionRe = "(?:/[a-zA-Z0-9_.+-]+=[a-zA-Z0-9_.+-]+)*"  // partitioning e.g. gender=Male

  private val dataFileRe: String = "part-\\d{5,}-" + // Part number
    "[0-9a-fA-F]{8}-" + // first 8 characters of uuid
    "[0-9a-fA-F]{4}-" + // 4 characters of uuid separated by -
    "[0-9a-fA-F]{4}-" +
    "[0-9a-fA-F]{4}-" +
    "[0-9a-fA-F]{12}" +        // last 12 hexadecimal characters of uuid
    "(?:[.-]c[0-9]{3,})?" +    // merged file counter .c000 or -c000
    "(?:\\.snappy)?\\.parquet" // extension, that may not be compressed by snappy
}
