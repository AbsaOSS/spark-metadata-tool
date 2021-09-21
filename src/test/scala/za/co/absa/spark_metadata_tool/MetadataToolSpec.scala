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

import cats.implicits._
import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.model.FileLine
import za.co.absa.spark_metadata_tool.model.IoError
import za.co.absa.spark_metadata_tool.model.JsonLine
import za.co.absa.spark_metadata_tool.model.NotFoundError
import za.co.absa.spark_metadata_tool.model.StringLine

import MetadataToolSpec._

class MetadataToolSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory {

  private val fileManager = mock[FileManager]

  private val metadataTool = new MetadataTool(fileManager)

  "loadFile" should "parse lines containing JSON as JSON objects" in {
    val line = validLine(s3TestPath)

    (fileManager.readAllLines _).expects(*).returning(Seq(line).asRight)

    val res                     = metadataTool.loadFile(s3BasePath)
    val expected: Seq[FileLine] = Seq(JsonLine(line.parseJson.asJsObject))

    res.value should contain theSameElementsAs expected
  }

  it should "wrap regular String line into correct type" in {
    val line = "I am a very generic text"

    (fileManager.readAllLines _).expects(*).returning(Seq(line).asRight)

    val res                     = metadataTool.loadFile(s3BasePath)
    val expected: Seq[FileLine] = Seq(StringLine(line))

    res.value should contain theSameElementsAs expected
  }

  it should "return underlying error if the file couldn't be loaded" in {
    val path = createPath(s3BaseString, s"$SparkMetadataDir".some, "1".some)
    val err  = IoError(s"Path $path does not exist")

    (fileManager.readAllLines _).expects(path).returning(err.asLeft)

    val res = metadataTool.loadFile(path)

    res.left.value shouldBe err
  }

  "saveFile" should "return underlying error in case of failure" in {
    val path = createPath(s3BaseString, s"$SparkMetadataDir".some, "1".some)
    val err  = IoError(s"Failed to write file to $path")

    (fileManager.write _).expects(path, *).returning(err.asLeft)

    val res = metadataTool.saveFile(path, Seq.empty)

    res.left.value shouldBe err
  }

  "fixPaths" should "replace old paths if no partition key was provided" in {
    val numLines = 10
    val data: Seq[FileLine] =
      stringLines ++ jsonLines(hdfsBaseString, None, numLines)
    val expected: Seq[FileLine] =
      stringLines ++ jsonLines(s3BaseString, None, numLines)

    val res = metadataTool.fixPaths(data, s3BasePath, None)

    res.value should contain theSameElementsAs expected
  }

  it should "replace old base paths and keep partitions intact" in {
    val numLines                = 10
    val data: Seq[FileLine]     = stringLines ++ jsonLines(hdfsBaseString, firstPartKey.some, numLines)
    val expected: Seq[FileLine] = stringLines ++ jsonLines(s3BaseString, firstPartKey.some, numLines)

    val res = metadataTool.fixPaths(data, s3BasePath, firstPartKey.some)

    res.value should contain theSameElementsAs expected
  }

  it should "fail if any path doesn't contain specified partition key" in {
    val numLines = 10
    val corruptedPath =
      createPath(hdfsBaseString, createPartitions("differentKey", "value1").some, "testFile.parquet".some)
    val data: Seq[FileLine] = stringLines ++ jsonLines(hdfsBaseString, firstPartKey.some, numLines) ++ Seq(
      JsonLine(validLine(corruptedPath).parseJson.asJsObject)
    )
    val expected = NotFoundError(
      s"Failed to fix path $corruptedPath! Couldn't split as partition key $firstPartKey was not found in the path."
    )

    val res = metadataTool.fixPaths(data, s3BasePath, firstPartKey.some)

    res.left.value shouldBe expected
  }

  it should "fail if any JSON line doesn't contain 'path' key" in {
    val numLines      = 10
    val corruptedLine = JsonLine(lineNoPath.parseJson.asJsObject)
    val data: Seq[FileLine] =
      stringLines ++ jsonLines(hdfsBaseString, firstPartKey.some, numLines) ++ Seq(corruptedLine)
    val expected = NotFoundError(s"Couldn't find key 'path' in $corruptedLine")

    val res = metadataTool.fixPaths(data, s3BasePath, firstPartKey.some)

    res.left.value shouldBe expected
  }

  "getFirstPartitionKey" should "return correct key" in {
    val numLines     = 10
    val path         = s3BasePath
    val key          = "key"
    val metadataPath = createPath(s3BaseString, s"$SparkMetadataDir".some, None)
    val testFile     = stringLines ++ jsonLines(hdfsBaseString, key.some, numLines)

    (fileManager.listDirectories _).expects(path).returning(mixedDirs.asRight)

    (fileManager.listFiles _).expects(metadataPath).returning(metaFiles.asRight)

    (fileManager.readAllLines _).expects(*).returning(testFile.map(_.toString).asRight)

    val res = metadataTool.getFirstPartitionKey(path)

    res.value.value shouldBe key
  }

  it should "return None when no matching key is found" in {
    val numLines     = 10
    val path         = s3BasePath
    val metadataPath = createPath(s3BaseString, s"$SparkMetadataDir".some, None)
    val testFile     = stringLines ++ jsonLines(hdfsBaseString, None, numLines)

    (fileManager.listDirectories _).expects(path).returning(mixedDirs.asRight)

    (fileManager.listFiles _).expects(metadataPath).returning(metaFiles.asRight)

    (fileManager.readAllLines _).expects(*).returning(testFile.map(_.toString).asRight)

    val res = metadataTool.getFirstPartitionKey(path)

    res.value shouldBe None
  }

  it should "should ignore .compact files and fail if there's no file to load" in {
    val path         = s3BasePath
    val metadataPath = createPath(s3BaseString, s"$SparkMetadataDir".some, None)
    val expected     = NotFoundError(s"Couldn't find standard metadata file to load in $compactFiles")

    (fileManager.listDirectories _).expects(path).returning(mixedDirs.asRight)

    (fileManager.listFiles _).expects(metadataPath).returning(compactFiles.asRight)

    val res = metadataTool.getFirstPartitionKey(path)

    res.left.value shouldBe expected
  }

  it should "fail when there's no JSON line in loaded file" in {
    val path         = s3BasePath
    val metadataPath = createPath(s3BaseString, s"$SparkMetadataDir".some, None)
    val testFilePath = createPath(s3BaseString, s"$SparkMetadataDir".some, "1".some)
    val testFile     = stringLines
    val expected     = NotFoundError(s"Couldn't find any JSON line in file $testFilePath")

    (fileManager.listDirectories _).expects(path).returning(mixedDirs.asRight)

    (fileManager.listFiles _).expects(metadataPath).returning(metaFiles.asRight)

    (fileManager.readAllLines _).expects(testFilePath).returning(testFile.map(_.toString).asRight)

    val res = metadataTool.getFirstPartitionKey(path)

    res.left.value shouldBe expected
  }

  it should "fail when there's JSON with missing 'path' key" in {
    val path         = s3BasePath
    val metadataPath = createPath(s3BaseString, s"$SparkMetadataDir".some, None)
    val testFile     = stringLines ++ Seq(JsonLine(lineNoPath.parseJson.asJsObject))
    val expected     = NotFoundError(s"Couldn't find path in JSON line $lineNoPath")

    (fileManager.listDirectories _).expects(path).returning(mixedDirs.asRight)

    (fileManager.listFiles _).expects(metadataPath).returning(metaFiles.asRight)

    (fileManager.readAllLines _).expects(*).returning(testFile.map(_.toString).asRight)

    val res = metadataTool.getFirstPartitionKey(path)

    res.left.value shouldBe expected
  }

}

object MetadataToolSpec {
  val firstPartKey   = "key1"
  val hdfsBaseString = "hdfs://path/to/root/dir"
  val hdfsBasePath   = createPath(hdfsBaseString, None, None)
  val s3BaseString   = "s3://some/base/path"
  val s3BasePath     = createPath(s3BaseString, None, None)
  val s3TestPath     = createPath(s3BaseString, createPartitions("key1", "value1").some, "testFile.parquet".some)

  def createPartitions(key1Name: String, key1Value: String): String = s"$key1Name=$key1Value/key2=value2"
  def createPath(basePath: String, partitions: Option[String], fileName: Option[String]): Path = new Path(
    Seq(basePath.some, partitions, fileName).flatten.mkString("/")
  )

  val lineNoPath                    = """{"key":"value","key2":"value2","key3":"value3"}"""
  def validLine(path: Path): String = s"""{"path":"$path","key":"value","key2":"value2","key3":"value3"}"""

  val stringLines: Seq[FileLine] = Seq(
    StringLine("I am a regular String"),
    StringLine("Me too!")
  )

  def jsonLines(basePath: String, firstPartKey: Option[String], count: Int): Seq[FileLine] = for {
    value <- Seq.range(1, count)
    path = createPath(
             basePath,
             firstPartKey.map(key => createPartitions(key, s"value${value.toString}")),
             s"testFile${value.toString}.parquet".some
           )
  } yield JsonLine(validLine(path).parseJson.asJsObject)

  val mixedDirs = Seq(
    createPath(s3BaseString, s"$SparkMetadataDir".some, None),
    createPath(s3BaseString, s"key=value1".some, None),
    createPath(s3BaseString, s"randomDirectory".some, None),
    createPath(s3BaseString, s"key=value2".some, None),
    createPath(s3BaseString, s"key=value3".some, None),
    createPath(s3BaseString, s"dirWith=Sign".some, None),
    createPath(s3BaseString, s"key=value4".some, None)
  )

  val compactFiles = {
    val numFiles = 5
    for { index <- Seq.range(1, numFiles) } yield createPath(
      s3BaseString,
      SparkMetadataDir.some,
      s"$index.compact".some
    )
  }

  val metaFiles = {
    val numFiles = 5
    val files =
      for { index <- Seq.range(1, numFiles) } yield createPath(
        s3BaseString,
        SparkMetadataDir.some,
        s"$index".some
      )

    files ++ compactFiles
  }

}
