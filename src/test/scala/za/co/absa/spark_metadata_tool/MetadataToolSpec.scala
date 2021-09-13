package za.co.absa.spark_metadata_tool

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.OptionValues
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark_metadata_tool.io.FileManager
import org.scalamock.scalatest.MockFactory
import org.apache.hadoop.fs.Path
import MetadataToolSpec._
import cats.implicits._
import za.co.absa.spark_metadata_tool.model.JsonLine
import spray.json._
import za.co.absa.spark_metadata_tool.model.FileLine
import za.co.absa.spark_metadata_tool.model.StringLine
import za.co.absa.spark_metadata_tool.model.IoError
import za.co.absa.spark_metadata_tool.model.NotFoundError

class MetadataToolSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory {

  private val fileManager = mock[FileManager]

  private val metadataTool = new MetadataTool(fileManager)

  "Loading a file" should "parse lines containing JSON as JSON objects" in {
    val path = s3BasePath
    val line = validLine(path)

    (fileManager.readAllLines _).expects(*).returning(Seq(line).asRight)

    val res                     = metadataTool.loadFile(s3BasePath)
    val expected: Seq[FileLine] = Seq(JsonLine(line.parseJson.asJsObject))

    res.value should contain theSameElementsAs expected
  }

  it should "wrap regular String line into correct type" in {
    val path = s3BasePath
    val line = "I am a very generic text"

    (fileManager.readAllLines _).expects(*).returning(Seq(line).asRight)

    val res                     = metadataTool.loadFile(path)
    val expected: Seq[FileLine] = Seq(StringLine(line))

    res.value should contain theSameElementsAs expected
  }

  it should "return underlying error if the file couldn't be loaded" in {
    val path = s3BasePath
    val err  = IoError(s"Path $path does not exist")

    (fileManager.readAllLines _).expects(path).returning(err.asLeft)

    val res = metadataTool.loadFile(path)

    res.left.value shouldBe err
  }

  "Saving a file" should "return underlying error in case of failure" in {
    val path = s3BasePath
    val err  = IoError(s"Failed to write file to $path")

    (fileManager.write _).expects(path, *).returning(err.asLeft)

    val res = metadataTool.saveFile(path, Seq.empty)

    res.left.value shouldBe err
  }

  "Fixing paths" should "replace old paths if no partition key was provided" in {
    val path = s3BasePath
    val data: Seq[FileLine] =
      stringLines ++ jsonLines(hdfsBaseString, None, 10)
    val expected: Seq[FileLine] =
      stringLines ++ jsonLines(s3BaseString, None, 10)

    val res = metadataTool.fixPaths(data, path, None)

    res.value should contain theSameElementsAs expected
  }

  it should "replace old base paths and keep partitions intact" in {
    val path                    = s3BasePath
    val data: Seq[FileLine]     = stringLines ++ jsonLines(hdfsBaseString, firstPartitionKey.some, 10)
    val expected: Seq[FileLine] = stringLines ++ jsonLines(s3BaseString, firstPartitionKey.some, 10)

    val res = metadataTool.fixPaths(data, path, firstPartitionKey.some)

    res.value should contain theSameElementsAs expected
  }

  it should "fail if any path doesn't contain specified partition key" in {
    val path          = s3BasePath
    val corruptedPath = createPath(hdfsBaseString, createPartitions("differentKey", "value1").some)
    val data: Seq[FileLine] = stringLines ++ jsonLines(hdfsBaseString, firstPartitionKey.some, 10) ++ Seq(
      JsonLine(validLine(corruptedPath).parseJson.asJsObject)
    )
    val expected = NotFoundError(
      s"Failed to fix path $corruptedPath! Couldn't split as partition key $firstPartitionKey was not found in the path."
    )

    val res = metadataTool.fixPaths(data, path, firstPartitionKey.some)

    res.left.value shouldBe expected
  }

  it should "fail if any JSON line doesn't contain 'path' key" in {
    val path                = s3BasePath
    val corruptedLine       = JsonLine(lineNoPath.parseJson.asJsObject)
    val data: Seq[FileLine] = stringLines ++ jsonLines(hdfsBaseString, firstPartitionKey.some, 10) ++ Seq(corruptedLine)
    val expected            = NotFoundError(s"Couldn't find key 'path' in $corruptedLine")

    val res = metadataTool.fixPaths(data, path, firstPartitionKey.some)

    res.left.value shouldBe expected
  }

}

object MetadataToolSpec {
  val firstPartitionKey = "key1"
  val hdfsBaseString    = "hdfs://path/to/root/dir"
  val hdfsBasePath      = createPath(hdfsBaseString, None)
  val s3BaseString      = "s3://some/base/path"
  val s3BasePath        = createPath(s3BaseString, None)

  def createPartitions(key1Name: String, key1Value: String)    = s"/$key1Name=$key1Value/key2=value2"
  def createPath(basePath: String, partitions: Option[String]) = new Path(s"$basePath${partitions.getOrElse("")}")

  val lineNoPath            = """{"key":"value","key2":"value2","key3":"value3"}"""
  def validLine(path: Path) = s"""{"path":"$path","key":"value","key2":"value2","key3":"value3"}"""

  val stringLines: Seq[FileLine] = Seq(
    StringLine("I am a regular String"),
    StringLine("Me too!")
  )

  def jsonLines(basePath: String, firstPartitionKey: Option[String], count: Int): Seq[FileLine] = for {
    value <- Seq.range(1, count)
    path   = createPath(basePath, firstPartitionKey.map(key => createPartitions(key, value.toString)))
  } yield JsonLine(validLine(path).parseJson.asJsObject)

}
