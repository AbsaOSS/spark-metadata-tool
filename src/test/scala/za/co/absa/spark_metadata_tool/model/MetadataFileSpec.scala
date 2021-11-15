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
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark_metadata_tool.model.MetadataFileSpec._

class MetadataFileSpec extends AnyFlatSpec with Matchers with EitherValues {

  "fromPath" should "parse path to regular metadata file" in {

    val path = new Path("hdfs://path/to/rooot/_spark_metadata/13")

    val res = MetadataFile.fromPath(path)

    res.value.numericName shouldBe 13
    res.value.compact shouldBe false
    res.value.path shouldBe path
  }

  it should "parse path to .compact metadata file" in {

    val path = new Path("hdfs://path/to/rooot/_spark_metadata/4.compact")

    val res = MetadataFile.fromPath(path)

    res.value.numericName shouldBe 4
    res.value.compact shouldBe true
    res.value.path shouldBe path
  }

  it should "return an error for invalid filename" in {

    val path = new Path("hdfs://path/to/rooot/_spark_metadata/foo.compact")

    val expected = ParsingError(
      s"Couldn't parse name `Some(foo)` of the file foo.compact, expected numeric value",
      None
    )

    val res = MetadataFile.fromPath(path)

    res.left.value.msg shouldBe expected.msg
  }

  "Ordering" should "return regular metadata files in correct order" in {
    val paths    = regularPaths
    val expected = regularFiles

    val res = paths.traverse(MetadataFile.fromPath).map(_.sorted)

    res.value should contain theSameElementsInOrderAs expected
  }

  it should "return .compact metadata files in correct order" in {
    val paths    = compactPaths
    val expected = compactFiles

    val res = paths.traverse(MetadataFile.fromPath).map(_.sorted)

    res.value should contain theSameElementsInOrderAs expected
  }

  it should "put .compact metadata file after the regular file with the same number" in {
    val paths = Seq(
      new Path("hdfs://path/to/rooot/_spark_metadata/3.compact"),
      new Path("hdfs://path/to/rooot/_spark_metadata/3")
    )
    val expected = Seq(
      MetadataFile(3, false, new Path("hdfs://path/to/rooot/_spark_metadata/3")),
      MetadataFile(3, true, new Path("hdfs://path/to/rooot/_spark_metadata/3.compact"))
    )

    val res = paths.traverse(MetadataFile.fromPath).map(_.sorted)

    res.value should contain theSameElementsInOrderAs expected
  }

  it should "return all metadata files in correct order" in {
    val paths    = combinedPaths
    val expected = combinedFiles

    val res = paths.traverse(MetadataFile.fromPath).map(_.sorted)

    res.value should contain theSameElementsInOrderAs expected
  }

}

object MetadataFileSpec {
  val regularPaths = Seq(
    new Path("hdfs://path/to/rooot/_spark_metadata/0"),
    new Path("hdfs://path/to/rooot/_spark_metadata/3"),
    new Path("hdfs://path/to/rooot/_spark_metadata/2"),
    new Path("hdfs://path/to/rooot/_spark_metadata/1"),
    new Path("hdfs://path/to/rooot/_spark_metadata/4")
  )

  val regularFiles = Seq(
    MetadataFile(0, false, new Path("hdfs://path/to/rooot/_spark_metadata/0")),
    MetadataFile(1, false, new Path("hdfs://path/to/rooot/_spark_metadata/1")),
    MetadataFile(2, false, new Path("hdfs://path/to/rooot/_spark_metadata/2")),
    MetadataFile(3, false, new Path("hdfs://path/to/rooot/_spark_metadata/3")),
    MetadataFile(4, false, new Path("hdfs://path/to/rooot/_spark_metadata/4"))
  )

  val compactPaths = Seq(
    new Path("hdfs://path/to/rooot/_spark_metadata/7.compact"),
    new Path("hdfs://path/to/rooot/_spark_metadata/5.compact"),
    new Path("hdfs://path/to/rooot/_spark_metadata/9.compact"),
    new Path("hdfs://path/to/rooot/_spark_metadata/1.compact"),
    new Path("hdfs://path/to/rooot/_spark_metadata/3.compact")
  )

  val compactFiles = Seq(
    MetadataFile(1, true, new Path("hdfs://path/to/rooot/_spark_metadata/1.compact")),
    MetadataFile(3, true, new Path("hdfs://path/to/rooot/_spark_metadata/3.compact")),
    MetadataFile(5, true, new Path("hdfs://path/to/rooot/_spark_metadata/5.compact")),
    MetadataFile(7, true, new Path("hdfs://path/to/rooot/_spark_metadata/7.compact")),
    MetadataFile(9, true, new Path("hdfs://path/to/rooot/_spark_metadata/9.compact"))
  )

  val combinedPaths = Seq(
    new Path("hdfs://path/to/rooot/_spark_metadata/0"),
    new Path("hdfs://path/to/rooot/_spark_metadata/3.compact"),
    new Path("hdfs://path/to/rooot/_spark_metadata/3"),
    new Path("hdfs://path/to/rooot/_spark_metadata/2"),
    new Path("hdfs://path/to/rooot/_spark_metadata/10"),
    new Path("hdfs://path/to/rooot/_spark_metadata/1"),
    new Path("hdfs://path/to/rooot/_spark_metadata/9.compact"),
    new Path("hdfs://path/to/rooot/_spark_metadata/4"),
    new Path("hdfs://path/to/rooot/_spark_metadata/7"),
    new Path("hdfs://path/to/rooot/_spark_metadata/9"),
    new Path("hdfs://path/to/rooot/_spark_metadata/11"),
    new Path("hdfs://path/to/rooot/_spark_metadata/6"),
    new Path("hdfs://path/to/rooot/_spark_metadata/8"),
    new Path("hdfs://path/to/rooot/_spark_metadata/5"),
    new Path("hdfs://path/to/rooot/_spark_metadata/6.compact")
  )

  val combinedFiles = Seq(
    MetadataFile(0, false, new Path("hdfs://path/to/rooot/_spark_metadata/0")),
    MetadataFile(1, false, new Path("hdfs://path/to/rooot/_spark_metadata/1")),
    MetadataFile(2, false, new Path("hdfs://path/to/rooot/_spark_metadata/2")),
    MetadataFile(3, false, new Path("hdfs://path/to/rooot/_spark_metadata/3")),
    MetadataFile(3, true, new Path("hdfs://path/to/rooot/_spark_metadata/3.compact")),
    MetadataFile(4, false, new Path("hdfs://path/to/rooot/_spark_metadata/4")),
    MetadataFile(5, false, new Path("hdfs://path/to/rooot/_spark_metadata/5")),
    MetadataFile(6, false, new Path("hdfs://path/to/rooot/_spark_metadata/6")),
    MetadataFile(6, true, new Path("hdfs://path/to/rooot/_spark_metadata/6.compact")),
    MetadataFile(7, false, new Path("hdfs://path/to/rooot/_spark_metadata/7")),
    MetadataFile(8, false, new Path("hdfs://path/to/rooot/_spark_metadata/8")),
    MetadataFile(9, false, new Path("hdfs://path/to/rooot/_spark_metadata/9")),
    MetadataFile(9, true, new Path("hdfs://path/to/rooot/_spark_metadata/9.compact")),
    MetadataFile(10, false, new Path("hdfs://path/to/rooot/_spark_metadata/10")),
    MetadataFile(11, false, new Path("hdfs://path/to/rooot/_spark_metadata/11"))
  )
}
