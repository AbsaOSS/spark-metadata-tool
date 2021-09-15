/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark_metadata_tool.io

import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.S3Object

import scala.jdk.CollectionConverters._

import S3FileManagerSpec._

class S3FileManagerSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory {

  private val r = scala.util.Random

  private val s3 = mock[S3Client]

  private val io = S3FileManager(s3)

  "listDirectories" should "return empty sequence if there are no directories" in {

    val response = ListObjectsV2Response
      .builder()
      .contents(
        files.map(f => S3Object.builder().key(f.toString).build()).asJavaCollection
      )
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(response)

    val res = io.listDirectories(basePath)

    res.value shouldBe empty
  }

  it should "return empty sequence if the bucket is empty" in {

    val response = ListObjectsV2Response
      .builder()
      .contents(
        Seq[S3Object]().asJavaCollection
      )
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(response)

    val res = io.listDirectories(basePath)

    res.value shouldBe empty
  }

  it should "list each directory only once" in {

    val response = ListObjectsV2Response
      .builder()
      .contents(
        r.shuffle(files ++ directories).map(f => S3Object.builder().key(f.toString).build()).asJavaCollection
      )
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(response)

    val res = io.listDirectories(basePath)

    res.value.length shouldBe 4
  }

  it should "filter out keys with different paths" in {

    val expected = Seq(
      new Path("s3://bucket/path/to/root/_spark_metadata"),
      new Path("s3://bucket/path/to/root/directory"),
      new Path("s3://bucket/path/to/root/let=there"),
      new Path("s3://bucket/path/to/root/let=ereht")
    )

    val response = ListObjectsV2Response
      .builder()
      .contents(
        r.shuffle(files ++ directories ++ differentRoot)
          .map(f => S3Object.builder().key(f.toString).build())
          .asJavaCollection
      )
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(response)

    val res = io.listDirectories(basePath)

    res.value should contain theSameElementsAs expected
  }

  it should "return paths to all directories" in {

    val expected = Seq(
      new Path("s3://bucket/path/to/root/_spark_metadata"),
      new Path("s3://bucket/path/to/root/directory"),
      new Path("s3://bucket/path/to/root/let=there"),
      new Path("s3://bucket/path/to/root/let=ereht")
    )

    val response = ListObjectsV2Response
      .builder()
      .contents(
        r.shuffle(files ++ directories).map(f => S3Object.builder().key(f.toString).build()).asJavaCollection
      )
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(response)

    val res = io.listDirectories(basePath)

    res.value should contain theSameElementsAs expected
  }

  "listFiles" should "return empty sequence if there are no files in directory" in {

    val response = ListObjectsV2Response
      .builder()
      .contents(
        directories.map(f => S3Object.builder().key(f.toString).build()).asJavaCollection
      )
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(response)

    val res = io.listFiles(basePath)

    res.value shouldBe empty
  }

  it should "return empty sequence if the bucket is empty" in {

    val response = ListObjectsV2Response
      .builder()
      .contents(
        Seq[S3Object]().asJavaCollection
      )
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(response)

    val res = io.listFiles(basePath)

    res.value shouldBe empty
  }

  it should "filter out keys with different paths" in {

    val expected = Seq(
      new Path("s3://bucket/path/to/root/file.parquet"),
      new Path("s3://bucket/path/to/root/otherFile.parquet"),
      new Path("s3://bucket/path/to/root/textFile.txt"),
      new Path("s3://bucket/path/to/root/1"),
      new Path("s3://bucket/path/to/root/2")
    )

    val response = ListObjectsV2Response
      .builder()
      .contents(
        r.shuffle(files ++ directories ++ differentRoot)
          .map(f => S3Object.builder().key(f.toString).build())
          .asJavaCollection
      )
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(response)

    val res = io.listFiles(basePath)

    res.value should contain theSameElementsAs expected
  }

  it should "return paths to all files in directory" in {

    val expected = Seq(
      new Path("s3://bucket/path/to/root/file.parquet"),
      new Path("s3://bucket/path/to/root/otherFile.parquet"),
      new Path("s3://bucket/path/to/root/textFile.txt"),
      new Path("s3://bucket/path/to/root/1"),
      new Path("s3://bucket/path/to/root/2")
    )

    val response = ListObjectsV2Response
      .builder()
      .contents(
        r.shuffle(files ++ directories).map(f => S3Object.builder().key(f.toString).build()).asJavaCollection
      )
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(*).returning(response)

    val res = io.listFiles(basePath)

    res.value should contain theSameElementsAs expected
  }
}

object S3FileManagerSpec {
  val basePath = new Path("s3://bucket/path/to/root")

  val files = Seq(
    new Path("path/to/root/file.parquet"),
    new Path("path/to/root/otherFile.parquet"),
    new Path("path/to/root/textFile.txt"),
    new Path("path/to/root/1"),
    new Path("path/to/root/2")
  )

  val directories = Seq(
    new Path("path/to/root/_spark_metadata/1"),
    new Path("path/to/root/directory/this/is/file.parquet"),
    new Path("path/to/root/let=there/be=partitions/differentFile.parquet"),
    new Path("path/to/root/let=there/be=light/alsoFile.parquet"),
    new Path("path/to/root/let=ereht/be=thgil/alsoFile.parquet")
  )

  val differentRoot = Seq(
    new Path("some/other/root/_spark_metadata/1"),
    new Path("some/other/root/directory/this/is/file.parquet"),
    new Path("yet/another/root/directory/file.parquet"),
    new Path("and/one/more/key=value/anotherFile.parquet")
  )
}
