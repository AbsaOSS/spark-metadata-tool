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

  def mkDataFileFilter(basePath: Path): Path => Boolean = {
    val partFilter = mkPartitionedFileFilter(basePath)
    (filePath: Path) => filterVisible(filePath) && partFilter(filePath)
  }
  private def filterVisible(filePath: Path): Boolean =
    !hiddenRe.matches(filePath.toString)

  private val hiddenRe = "^_|^\\./_|/\\.".r

  private def mkPartitionedFileFilter(basePath: Path): Path => Boolean = {
    val partFilter = mkDataFileRe(basePath)
    (filePath: Path) => partFilter.matches(filePath.toString)
  }

  private def mkDataFileRe(basePath: Path): Regex =
    new Regex(s"${Regex.quote(basePath.toString)}$partitionRe/$dataFileRe", "partition", "num")

  private val dataFileRe: String = "part-(?<num>\\d{4,})-" + // Part number
    "[0-9a-fA-F]{8}-" + // first 8 characters of uuid
    "[0-9a-fA-F]{4}-" + // 4 characters of uuid separated by -
    "[0-9a-fA-F]{4}-" +
    "[0-9a-fA-F]{4}-" +
    "[0-9a-fA-F]{12}" +         // last 12 hexadecimal characters of uuid
    "(?:\\.snappy)?\\.parquet" // extension, that may not be compressed by snappy

  private val partitionRe = "(?<partition>(?:/[a-zA-Z0-9_.+-]+=[a-zA-Z0-9_.+-]+)*)"

  private def asNonEmptyArray[A](seq: Array[A]): Option[Array[A]] =
    if (seq.isEmpty) None else Some(seq)

  private[spark_metadata_tool] def validateDataFiles(dataFiles: Seq[Path], basePath: Path): Either[IoError, Unit] = {
    val dataFileRe = mkDataFileRe(basePath)
    dataFiles.flatMap { path =>
      dataFileRe.findFirstMatchIn(path.toString).map(mat => (mat.group("partition"), mat.group("num").toInt))
    }
      .groupMap(_._1)(_._2)
      .map { case (group, seq) =>
        checkParts(group, seq)
      }
      .foldLeft(Right[IoError, Unit](()).withLeft) {
        case (Left(aggErr), Left(err)) =>
          Left(IoError(aggErr.msg + err.msg, None))
        case (agg, Right(_)) => agg
        case (_, result)     => result
      }
  }

  private def checkParts(group: String, seq: Seq[Int]): Either[IoError, Unit] =
    if (seq.isEmpty) {
      Left(IoError(s"No data files for partition $group.", None))
    } else {
      val sieve = new Array[Int](seq.max + 1)
      seq.foldLeft(sieve) { (hist, part) =>
        hist(part) += 1
        hist
      }
      val missing     = sieve.zipWithIndex.filter(_._1 == 0).map(_._2)
      val duplicities = sieve.zipWithIndex.filter(_._1 > 1).map(_._2)
      if (missing.isEmpty && duplicities.isEmpty) {
        Right(())
      } else {
        val missingMsg = asNonEmptyArray(missing)
          .map(parts => s"$group missing part files: ${parts.mkString(", ")}.")
        val duplicitiesMsg = asNonEmptyArray(duplicities)
          .map(parts => s"Detected duplicities in $group for part files: ${parts.mkString(", ")}.")
        Left(IoError((missingMsg ++ duplicitiesMsg).mkString(" "), None))
      }
    }
}

class DataTool(io: FileManager) {
  import za.co.absa.spark_metadata_tool.DataTool._

  def listDataFilesUpToPart(path: Path, maxPartNumber: Int): Either[IoError, Seq[Path]] =
    for {
      dataFiles <- listDataFiles(path).map(_.take(maxPartNumber))
      _         <- Either.cond(dataFiles.nonEmpty, (), IoError(s"No data files found in $path", None))
    } yield dataFiles

  def listDataFiles(path: Path): Either[IoError, Seq[Path]] =
    for {
      datafiles <- io.walkFiles(path, mkDataFileFilter(path)).map(_.sortBy(_.toUri))
      _         <- validateDataFiles(datafiles, path)
    } yield datafiles
}
