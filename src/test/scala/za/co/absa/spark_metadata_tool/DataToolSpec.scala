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
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.model.IoError

import java.io.IOException
import java.time.{Duration, Instant}

class DataToolSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory {

  private val fileManager = mock[FileManager]

  private val dataTool = new DataTool(fileManager)

  private val baseDir = new Path("s3://bucket/tmp/superheroes")

  private val DefaultBlockReplication = 1
  private val DefaultBlockSize: Long  = 64 * 1024 * 1024
  private val TMinus10: Instant       = Instant.now().minus(Duration.ofMinutes(10))

  "listDataFileStatuses" should "list statuses of all data files in directory tree" in {
    val expected = Seq(
      mkStatus("part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      mkStatus("gender=Male/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      mkStatus("part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      mkStatus("gender=Female/part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
    )
    (fileManager.walkFileStatuses(_: Path, _: Path => Boolean)).expects(baseDir, *).returning(Right(expected))

    dataTool.listDataFileStatuses(baseDir) should equal(Right(expected))
  }

  it should "fail when no data files are found" in {
    (fileManager.walkFileStatuses(_: Path, _: Path => Boolean)).expects(baseDir, *).returning(Right(Seq()))

    dataTool.listDataFileStatuses(baseDir) should equal(Left(IoError(s"No data files found in $baseDir", None)))

  }

  it should "fail on file system exception" in {
    val exception = new IOException("io error")
    (fileManager
      .walkFileStatuses(_: Path, _: Path => Boolean))
      .expects(baseDir, *)
      .returning(Left(IoError("io error", Some(exception))))

    dataTool.listDataFileStatuses(baseDir) should equal(Left(IoError("io error", Some(exception))))
  }

  "dataFileFilter" should "filter only visible parquet files" in {
    val expected = Seq(
      mkPath("part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      mkPath("gender=Female/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      mkPath("gender=Female/subdir/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      mkPath("gender=Male/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      mkPath("subdir/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
    )
    val invalid = Seq(
      mkPath("_SUCCESS"),
      mkPath("._SUCCESS"),
      mkPath("_SUCCESS.crc"),
      mkPath("part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet.crc"),
      mkPath("gender=Male/_part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.snappy.parquet"),
      mkPath("gender=Male/.part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      mkPath("_gender=Male/part-00003-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.parquet"),
      mkPath(".gender=Unknown/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      mkPath("hidden/_subdir/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
    )

    (expected ++ invalid).filter(DataTool.dataFileFilter) should equal(expected)
  }

  private def mkStatus(relPath: String, size: Long = 512, lastModified: Instant = TMinus10): FileStatus =
    new FileStatus(
      size,
      false,
      DefaultBlockReplication,
      DefaultBlockSize,
      lastModified.toEpochMilli,
      mkPath(relPath)
    )

  private def mkPath(relPath: String): Path =
    new Path(baseDir, relPath)
}
