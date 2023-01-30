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
import za.co.absa.spark_metadata_tool.model.IoError

import java.util.UUID

class DataToolSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory {

  "listDatafiles" should "list all data files in directory" in {

  }

  it should "list only properly formatted files" in {

  }

  it should "list files in ascending order" in {

  }

  "listDatafilesUpToPart" should "list only datafiles up to part number" in {

  }

  it should "list data files in ascending order" in {

  }

  it should "fail when some of required files are missing" in {

  }

  it should "fail when present with part duplicities" in {

  }

  "validateDataFiles" should "return Right(()) when correct sequence of datafiles is provided" in {
    val basePath = new Path("s3://bucket/tmp/superheroes")
    val dataFiles = Seq(
      new Path(basePath, s"gender=Male/part-0000-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Male/part-0001-$swiftUUIDStr.parquet"),
      new Path(basePath, s"gender=Male/part-0002-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Female/part-0000-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Female/part-0001-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Unknown/strength_quantile_gt0.95=true/part-0000-$swiftUUIDStr.parquet"),
      new Path(basePath, s"gender=Unknown/strength_quantile_gt0.95=false/part-0000-$swiftUUIDStr.parquet")
    )
    DataTool.validateDataFiles(dataFiles, basePath) should equal(Right(()))
  }

  it should "return Right(()) for unpartitioned data" in {
    val basePath = new Path("s3://bucket/tmp/superheroes")
    val dataFiles = Seq(
      new Path(basePath, s"part-0000-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"part-0001-$swiftUUIDStr.parquet"),
      new Path(basePath, s"part-0002-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"part-0003-$swiftUUIDStr.parquet")
    )
    DataTool.validateDataFiles(dataFiles, basePath) should equal(Right(()))
  }

  it should "fail when some part files are missing" in {
    val basePath = new Path("s3://bucket/tmp/superheroes")
    val dataFiles = Seq(
      new Path(basePath, s"gender=Male/part-0000-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Male/part-0001-$swiftUUIDStr.parquet"),
      new Path(basePath, s"gender=Male/part-0002-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Female/part-0000-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Female/part-0001-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Unknown/strength_quantile_gt0.95=true/part-0001-$swiftUUIDStr.parquet"),
      new Path(basePath, s"gender=Unknown/strength_quantile_gt0.95=false/part-0000-$swiftUUIDStr.parquet")
    )
    DataTool.validateDataFiles(dataFiles, basePath) should equal(Left(IoError("/gender=Unknown/strength_quantile_gt0.95=true missing part files: 0.", None)))
  }

  it should "fail when duplicities are introduced in data" in {
    val basePath = new Path("s3://bucket/tmp/superheroes")
    val dataFiles = Seq(
      new Path(basePath, s"gender=Male/part-0000-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Male/part-0001-$swiftUUIDStr.parquet"),
      new Path(basePath, s"gender=Male/part-0002-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Female/part-0000-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Female/part-0000-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Female/part-0001-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Unknown/strength_quantile_gt0.95=true/part-0000-$swiftUUIDStr.parquet"),
      new Path(basePath, s"gender=Unknown/strength_quantile_gt0.95=false/part-0000-$swiftUUIDStr.parquet")
    )
    DataTool.validateDataFiles(dataFiles, basePath) should equal(Left(IoError("Detected duplicities in /gender=Female for part files: 0.", None)))
  }

  it should "fail when duplicities and missing files are in data" in {
    val basePath = new Path("s3://bucket/tmp/superheroes")
    val dataFiles = Seq(
      new Path(basePath, s"gender=Male/part-0000-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Male/part-0002-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Male/part-0002-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Female/part-0000-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Female/part-0001-$swiftUUIDStr.snappy.parquet"),
      new Path(basePath, s"gender=Unknown/strength_quantile_gt0.95=true/part-0000-$swiftUUIDStr.parquet"),
      new Path(basePath, s"gender=Unknown/strength_quantile_gt0.95=false/part-0000-$swiftUUIDStr.parquet")
    )
    DataTool.validateDataFiles(dataFiles, basePath) should equal(Left(IoError("/gender=Male missing part files: 1. Detected duplicities in /gender=Male for part files: 2.", None)))
  }

  private def swiftUUIDStr: String =
    UUID.randomUUID().toString
}
