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
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.model.IoError

import java.io.IOException

class DataToolSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory {

  private val fileManager = mock[FileManager]

  private val dataTool = new DataTool(fileManager)

  private val baseDir = new Path("s3:///bucket/tmp/superheroes")

  "listDatafiles" should "list all data files in directory tree" in {
    val expected = Seq(
      new Path(baseDir, "part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      new Path(baseDir, "gender=Male/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      new Path(baseDir, "part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      new Path(baseDir, "gender=Female/part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
    )
    (fileManager.walkFiles(_: Path, _: Path => Boolean)).expects(baseDir, *).returning(Right(expected))

    dataTool.listDataFiles(baseDir) should equal(Right(expected))
  }

  it should "fail when no data files are found" in {
    (fileManager.walkFiles(_: Path, _: Path => Boolean)).expects(baseDir, *).returning(Right(Seq()))

    dataTool.listDataFiles(baseDir) should equal(Left(IoError(s"No data files found in $baseDir", None)))

  }

  it should "fail on file system exception" in {
    val exception = new IOException("io error")
    (fileManager
      .walkFiles(_: Path, _: Path => Boolean))
      .expects(baseDir, *)
      .returning(Left(IoError("io error", Some(exception))))

    dataTool.listDataFiles(baseDir) should equal(Left(IoError("io error", Some(exception))))
  }

  "listDatafilesUpToPart" should "takes only first 5 files" in {
    (fileManager
      .walkFiles(_: Path, _: Path => Boolean))
      .expects(baseDir, *)
      .returning(
        Right(
          Seq(
            new Path(baseDir, "gender=Female/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
            new Path(baseDir, "gender=Female/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
            new Path(baseDir, "gender=Female/part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
            new Path(baseDir, "gender=Male/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
            new Path(baseDir, "gender=Male/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.snappy.parquet"),
            new Path(baseDir, "gender=Male/part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
            new Path(baseDir, "gender=Male/part-00003-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.parquet"),
            new Path(baseDir, "gender=Unknown/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
            new Path(baseDir, "gender=Unknown/part-00003-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
            new Path(baseDir, "gender=Unknown/part-00007-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          )
        )
      )

    val maxPartNumber = 5
    dataTool.listDataFilesUpToPart(baseDir, maxPartNumber) should equal(
      Right(
        Seq(
          new Path(baseDir, "gender=Female/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
          new Path(baseDir, "gender=Female/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
          new Path(baseDir, "gender=Female/part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
          new Path(baseDir, "gender=Male/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
          new Path(baseDir, "gender=Male/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.snappy.parquet")
        )
      )
    )
  }

  "dataFileFilter" should "filter only visible parquet files" in {
    val expected = Seq(
      new Path(baseDir, "part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      new Path(baseDir, "gender=Female/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      new Path(baseDir, "gender=Female/subdir/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      new Path(baseDir, "gender=Male/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      new Path(baseDir, "subdir/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
    )
    val invalid = Seq(
      new Path(baseDir, "_SUCCESS"),
      new Path(baseDir, "._SUCCESS"),
      new Path(baseDir, "_SUCCESS.crc"),
      new Path(baseDir, "part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet.crc"),
      new Path(baseDir, "gender=Male/_part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.snappy.parquet"),
      new Path(baseDir, "gender=Male/.part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      new Path(baseDir, "_gender=Male/part-00003-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.parquet"),
      new Path(baseDir, ".gender=Unknown/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
      new Path(baseDir, "hidden/_subdir/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"),
    )

    (expected ++ invalid).filter(DataTool.dataFileFilter) should equal(expected)
  }
}
