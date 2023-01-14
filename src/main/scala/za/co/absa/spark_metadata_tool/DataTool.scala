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

object DataTool {
  private val dataFileRe: Regex = ("^part-(\\d{4,})-" + // Part number
    "[0-9a-fA-F]{8}-" + // first 8 characters of uuid
    "[0-9a-fA-F]{4}-" + // 4 characters of uuid separated by -
    "[0-9a-fA-F]{4}-" +
    "[0-9a-fA-F]{4}-" +
    "[0-9a-fA-F]{12}" +           // last 12 hexadecimal characters of uuid
    "(?:\\.snappy)?\\.parquet").r // extension, that may not be compressed by snappy
}

class DataTool(io: FileManager) {
  import za.co.absa.spark_metadata_tool.DataTool._

  def listDataFilesUpToPart(path: Path, maxPartNumber: Long): Either[IoError, Seq[(Int, Path)]] =
    for {
      dataFiles  <- listDataFiles(path)
      upToMaxPart = dataFiles.filter(_._1 <= maxPartNumber)
      _          <- validateNonEmpty(upToMaxPart, path)
      _          <- checkInexactNumberOfDataFiles(upToMaxPart, path)
    } yield upToMaxPart

  def listDataFiles(path: Path): Either[IoError, Seq[(Int, Path)]] =
    io.listFiles(path).map(_.flatMap(p => dataFilePartNumber(p).map((_, p))).sortBy(_._1))

  private def validateNonEmpty(dataFiles: Seq[(Int, Path)], dir: Path): Either[IoError, Unit] =
    if (dataFiles.isEmpty) {
      Left(IoError(s"No data files found in $dir!", None))
    } else {
      Right(())
    }

  private def checkInexactNumberOfDataFiles(dataFiles: Seq[(Int, Path)], dir: Path): Either[IoError, Unit] = {
    val occurrences = partFileOccurrences(dataFiles)
    if (occurrences.contains(0)) {
      val missing    = occurrences.zipWithIndex.filter(_._1 == 0).map(_._2)
      Left(IoError(s"Missing [${missing.mkString(", ")}] part files in  data directory: $dir", None))
    } else if (occurrences.exists(_ > 1)) {
      val duplicities = occurrences.zipWithIndex.filter(_._1 > 1).map(_._2)
      Left(IoError(s"Directory $dir contains duplicated part files: [${duplicities.mkString(", ")}]", None))
    } else {
      Right(())
    }
  }

  private def partFileOccurrences(dataFiles: Seq[(Int, Path)]): Array[Int] =
    dataFiles.foldLeft(new Array[Int](dataFiles.last._1 + 1)) { case (sieve, (idx, _)) =>
      sieve(idx) += 1
      sieve
    }

  private def dataFilePartNumber(path: Path): Option[Int] =
    dataFileRe
      .findFirstMatchIn(path.getName)
      .map(_.group(1))
      .map(_.toInt)
}
