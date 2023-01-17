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

import _root_.io.circe.parser._
import _root_.io.circe.syntax._
import _root_.io.circe.generic.auto._
import cats.implicits._
import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark_metadata_tool.io.FileManager
import za.co.absa.spark_metadata_tool.model.{FileLine, IoError, JsonLine, MetadataFile, MetadataRecord, NotFoundError, ParsingError, SinkFileStatus, StringLine}
import MetadataToolSpec._

class MetadataToolSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory {

  private val fileManager = mock[FileManager]

  private val metadataTool = new MetadataTool(fileManager)

  "loadFile" should "parse lines containing JSON as JSON objects" in {
    val line = validLine(s3TestPath)

    (fileManager.readAllLines _).expects(*).returning(Seq(line).asRight)

    val res                     = metadataTool.loadFile(s3BasePath)
    val expected: Seq[FileLine] = Seq(JsonLine(parse(line).value))

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
    val err  = IoError(s"Path $path does not exist", None)

    (fileManager.readAllLines _).expects(path).returning(err.asLeft)

    val res = metadataTool.loadFile(path)

    res.left.value shouldBe err
  }

  "saveFile" should "return underlying error in case of failure" in {
    val path = createPath(s3BaseString, s"$SparkMetadataDir".some, "1".some)
    val err  = IoError(s"Failed to write file to $path", None)

    (fileManager.write _).expects(path, *).returning(err.asLeft)

    val res = metadataTool.saveFile(path, Seq.empty, dryRun = false)

    res.left.value shouldBe err
  }

  "saveMetadataFiles" should "create properly formatted metadata files" in {
    val fileStatus = SinkFileStatus(
      path = s"$s3BaseString/part-0000-12345678-9999-0000-aaaa-bcdef12345.snappy.parquet",
      size = 1234L,
      isDir = false,
      modificationTime = System.currentTimeMillis(),
      blockReplication = 1,
      blockSize = 132456L,
      action = "add"
    )
    val metadataPath = new Path(s3BaseString, SparkMetadataDir)

    (fileManager.write _)
      .expects(
        new Path(metadataPath, "0"),
        Seq(
          "v1",
          fileStatus.asJson.noSpaces
        )
      )
      .returning(Right(()))

    val res = metadataTool.saveMetadataFiles(
      metadataPath,
      Seq((0, fileStatus)),
      dryRun = false
    )

    res should equal(Right(()))
  }

  it should "create multiple metadata files from multiple inputs" in {
    val zerothFileStatus = SinkFileStatus(
      path = s"$s3BaseString/part-0000-12345678-9999-0000-aaaa-bcdef12345.snappy.parquet",
      size = 1234L,
      isDir = false,
      modificationTime = System.currentTimeMillis(),
      blockReplication = 1,
      blockSize = 132456L,
      action = "add"
    )
    val firstFileStatus = SinkFileStatus(
      path = s"$s3BaseString/part-0001-12345678-9999-0000-aaaa-bcdef12345.snappy.parquet",
      size = 1234L,
      isDir = false,
      modificationTime = System.currentTimeMillis(),
      blockReplication = 1,
      blockSize = 132456L,
      action = "add"
    )
    val metadataPath = new Path(s3BaseString, SparkMetadataDir)

    (fileManager.write _)
      .expects(
        new Path(metadataPath, "0"),
        Seq(
          "v1",
          zerothFileStatus.asJson.noSpaces
        )
      )
      .returning(Right(()))
    (fileManager.write _)
      .expects(
        new Path(metadataPath, "1"),
        Seq(
          "v1",
          firstFileStatus.asJson.noSpaces
        )
      )
      .returning(Right(()))

    val res = metadataTool.saveMetadataFiles(
      metadataPath,
      Seq((0, zerothFileStatus), (1, firstFileStatus)),
      dryRun = false
    )

    res should equal(Right(()))
  }

  it should "fail when at least on metadata file can not be written" in  {
    val fileStatus = SinkFileStatus(
      path = s"$s3BaseString/part-0001-12345678-9999-0000-aaaa-bcdef12345.snappy.parquet",
      size = 1234L,
      isDir = false,
      modificationTime = System.currentTimeMillis(),
      blockReplication = 1,
      blockSize = 132456L,
      action = "add"
    )
    val metadataPath = new Path(s3BaseString, SparkMetadataDir)

    (fileManager.write _).expects(*, *).returning(Right(()))
    (fileManager.write _).expects(*, *).returning(Left(IoError("File already exists", None)))

    val res = metadataTool.saveMetadataFiles(metadataPath, Seq((0, fileStatus), (1, fileStatus)), dryRun = false)

    res should equal(Left(IoError("File already exists", None)))
  }

  "saveCompactedMetadata" should "create properly formatted compacted metadata file" in {
    val zerothFileStatus = SinkFileStatus(
      path = s"$s3BaseString/part-0000-12345678-9999-0000-aaaa-bcdef12345.snappy.parquet",
      size = 1234L,
      isDir = false,
      modificationTime = System.currentTimeMillis(),
      blockReplication = 1,
      blockSize = 132456L,
      action = "add"
    )
    val firstFileStatus = zerothFileStatus.copy(
      path = s"$s3BaseString/part-0001-12345678-9999-0000-aaaa-bcdef12345.snappy.parquet"
    )
    val secondFileStatus = zerothFileStatus.copy(
      path = s"$s3BaseString/part-0002-12345678-9999-0000-aaaa-bcdef12345.snappy.parquet"
    )
    val metadataDir = new Path(s3BaseString, SparkMetadataDir)

    (fileManager.write _).expects(
      new Path(metadataDir, "2.compact"),
      Seq(
        "v1",
        zerothFileStatus.asJson.noSpaces,
        firstFileStatus.asJson.noSpaces,
        secondFileStatus.asJson.noSpaces
      )
    ).returning(Right(()))

    val res = metadataTool.saveCompactedMetadata(
      metadataDir,
      metadata = 2,
      lines = Seq(zerothFileStatus, firstFileStatus, secondFileStatus),
      dryRun = false
    )

    res should equal(Right(()))
  }

  it should "fail on save metadata failure" in {

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
      JsonLine(parse(validLine(corruptedPath)).value)
    )
    val expected = NotFoundError(
      s"Failed to fix path $corruptedPath! Couldn't split as partition key $firstPartKey was not found in the path."
    )

    val res = metadataTool.fixPaths(data, s3BasePath, firstPartKey.some)

    res.left.value shouldBe expected
  }

  it should "not fail when encountering already fixed path" in {
    val numLines = 10
    val alreadyFixed =
      createPath(
        s3BaseString,
        createPartitions(firstPartKey, s"value${(numLines + 1).toString}").some,
        s"testFile${(numLines + 1).toString}.parquet".some
      )
    val data: Seq[FileLine] = stringLines ++ jsonLines(hdfsBaseString, firstPartKey.some, numLines) ++ Seq(
      JsonLine(parse(validLine(alreadyFixed)).value)
    )
    val expected: Seq[FileLine] = stringLines ++ jsonLines(s3BaseString, firstPartKey.some, numLines) ++ Seq(
      JsonLine(parse(validLine(alreadyFixed)).value)
    )

    val res = metadataTool.fixPaths(data, s3BasePath, firstPartKey.some)

    res.value should contain theSameElementsAs expected
  }

  it should "fail if any JSON line doesn't contain 'path' key" in {
    val numLines      = 10
    val corruptedLine = JsonLine(parse(lineNoPath).value)
    val data: Seq[FileLine] =
      stringLines ++ jsonLines(hdfsBaseString, firstPartKey.some, numLines) ++ Seq(corruptedLine)
    val expected = NotFoundError(s"Couldn't find path in JSON line ${corruptedLine.toString}")

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

    (fileManager.readAllLines _)
      .expects(*)
      .returning(testFile.map(_.toString).asRight)

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

    (fileManager.readAllLines _)
      .expects(*)
      .returning(testFile.map(_.toString).asRight)

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

    (fileManager.readAllLines _)
      .expects(testFilePath)
      .returning(testFile.map(_.toString).asRight)

    val res = metadataTool.getFirstPartitionKey(path)

    res.left.value shouldBe expected
  }

  it should "fail when there's JSON with missing 'path' key" in {
    val path         = s3BasePath
    val metadataPath = createPath(s3BaseString, s"$SparkMetadataDir".some, None)
    val testFile     = stringLines ++ Seq(JsonLine(parse(lineNoPath).value))
    val expected     = NotFoundError(s"Couldn't find path in JSON line $lineNoPath")

    (fileManager.listDirectories _).expects(path).returning(mixedDirs.asRight)

    (fileManager.listFiles _).expects(metadataPath).returning(metaFiles.asRight)

    (fileManager.readAllLines _)
      .expects(*)
      .returning(testFile.map(_.toString).asRight)

    val res = metadataTool.getFirstPartitionKey(path)

    res.left.value shouldBe expected
  }

  "listFilesRecursively" should "return all files recursively" in {
    val root                = unixBasePath
    val filesInRoot         = Seq(new Path(s"$unixBasePath/a.file"), new Path(s"$unixBasePath/b.file"))
    val subDir              = new Path(s"$unixBasePath/subDir")
    val filesInSubDir       = Seq(new Path(s"$subDir/c.file"), new Path(s"$subDir/d.file"))
    val subSubOneDir        = new Path(s"$subDir/subSubOneDir")
    val filesInSubSubOneDir = Seq(new Path(s"$subSubOneDir/e.file"), new Path(s"$subSubOneDir/f.file"))
    val subSubTwoDir        = new Path(s"$subDir/subSubTwoDir")
    val filesInSubSubTwoDir = Seq(new Path(s"$subSubTwoDir/g.file"), new Path(s"$subSubTwoDir/h.file"))
    val expected: Seq[Path] = filesInRoot ++ filesInSubDir ++ filesInSubSubOneDir ++ filesInSubSubTwoDir

    (fileManager.listDirectories _).expects(root).returning(Seq(subDir).asRight)
    (fileManager.listDirectories _).expects(subDir).returning(Seq(subSubOneDir, subSubTwoDir).asRight)
    (fileManager.listDirectories _).expects(subSubOneDir).returning(Seq.empty.asRight)
    (fileManager.listDirectories _).expects(subSubTwoDir).returning(Seq.empty.asRight)

    (fileManager.listFiles _).expects(root).returning(filesInRoot.asRight)
    (fileManager.listFiles _).expects(subDir).returning(filesInSubDir.asRight)
    (fileManager.listFiles _).expects(subSubOneDir).returning(filesInSubSubOneDir.asRight)
    (fileManager.listFiles _).expects(subSubTwoDir).returning(filesInSubSubTwoDir.asRight)

    val res = metadataTool.listFilesRecursively(root)
    res.value should contain theSameElementsAs expected
  }

  it should "return empty sequence when input folder is empty" in {
    val path = unixBasePath

    (fileManager.listDirectories _).expects(path).returning(Seq.empty.asRight)
    (fileManager.listFiles _).expects(path).returning(Seq.empty.asRight)

    val res = metadataTool.listFilesRecursively(path)

    res.value.isEmpty shouldBe true
  }

  "getMetaRecords" should "return all metadata records from metadata" in {
    val path              = unixBasePath
    val versionLineString = versionLine.value
    val metadataRecordA   = MetadataRecord(new Path(s"$unixBasePath/a.file"), "add")
    val metadataRecordB   = MetadataRecord(new Path(s"$unixBasePath/B.file"), "add")

    val fileA = validLineWithAction(metadataRecordA.path, metadataRecordA.action)
    val fileB = validLineWithAction(metadataRecordB.path, metadataRecordB.action)

    val expected = Seq(metadataRecordA, metadataRecordB)

    (fileManager.readAllLines _).expects(path).returning(Seq(versionLineString, fileA, fileB).asRight)

    val res = metadataTool.getMetaRecords(path)

    res.value should contain theSameElementsAs expected
  }

  it should "fail if file is not metadata file" in {
    val path = unixBasePath

    (fileManager.readAllLines _).expects(path).returning(stringLines.map(_.toString).asRight)

    val expected = MetadataToolSpec.getParsingError(path.toString)
    val res      = metadataTool.getMetaRecords(path)

    res.left.value shouldBe expected
  }

  it should "fail if any JSON line doesn't contain 'action' key" in {
    val path              = unixBasePath
    val versionLineString = versionLine.value
    val corruptedLine     = validLine(path)

    (fileManager.readAllLines _).expects(path).returning(Seq(versionLineString, corruptedLine).asRight)

    val expected = NotFoundError(s"Couldn't find action in JSON line $corruptedLine")
    val res      = metadataTool.getMetaRecords(path)

    res.left.value shouldBe expected
  }

  "backupFile" should "not perform any IO action in dry run" in {
    val path = s3BasePath

    val copyCall = (fileManager.copy _).expects(*, *).returning(().asRight)

    metadataTool.backupFile(path, dryRun = true)

    copyCall.never()
  }

  it should "create copy of the file in wet run" in {
    val path = s3BasePath

    val copyCall = (fileManager.copy _).expects(*, *).returning(().asRight)

    metadataTool.backupFile(path, dryRun = false)

    copyCall.once()
  }

  "saveFile" should "not perform any IO action in dry run" in {
    val path = s3BasePath

    val writeCall = (fileManager.write _).expects(*, *).returning(().asRight)

    metadataTool.saveFile(path, Seq.empty, dryRun = true)

    writeCall.never()
  }

  it should "write the file contents in wet run" in {
    val path = s3BasePath

    val writeCall = (fileManager.write _).expects(*, *).returning(().asRight)

    metadataTool.saveFile(path, Seq.empty, dryRun = false)

    writeCall.once()
  }

  "deleteBackup" should "not perform any IO action in dry run" in {
    val path  = s3BasePath
    val files = Seq(new Path("/backup/file/to/delete"))

    val listCall   = (fileManager.listFiles _).expects(*).returning(files.asRight)
    val deleteCall = (fileManager.delete _).expects(*).returning(().asRight)

    metadataTool.deleteBackup(path, dryRun = true)

    listCall.never()
    deleteCall.never()
  }

  it should "delete backup files and directory in wet run" in {
    val path  = s3BasePath
    val files = Seq(new Path("/backup/file/to/delete"))

    val listCall      = (fileManager.listFiles _).expects(path).returning(files.asRight)
    val deleteCall    = (fileManager.delete _).expects(files).returning(().asRight)
    val deleteDirCall = (fileManager.delete _).expects(Seq(path)).returning(().asRight)

    metadataTool.deleteBackup(path, dryRun = false)

    listCall.once()
    deleteCall.once()
    deleteDirCall.once()
  }

  "merge" should "merge multiple metadata files with correct order" in {
    val targetFile = MetadataFile(9, compact = true, new Path("hdfs://path/new/_spark_metadata/9.compact"))
    val targetLines = Seq(
      StringLine("v1"),
      JsonLine(parse(validLine(new Path("hdfs://path/from/target/test.parquet"))).value)
    )
    val oldFile1 = MetadataFile(1, compact = false, new Path("hdfs://path/old/_spark_metadata/1"))
    val oldFile2 = MetadataFile(2, compact = false, new Path("hdfs://path/old/_spark_metadata/2"))
    val oldLines1 = Seq(
      StringLine("v1"),
      JsonLine(parse(validLine(new Path("hdfs://path/from/old1/test.parquet"))).value)
    )
    val oldLines2 = Seq(
      StringLine("v1"),
      JsonLine(parse(validLine(new Path("hdfs://path/from/old2/test.parquet"))).value)
    )

    val expected = Seq(
      StringLine("v1"),
      JsonLine(parse(validLine(new Path("hdfs://path/from/old1/test.parquet"))).value),
      JsonLine(parse(validLine(new Path("hdfs://path/from/old2/test.parquet"))).value),
      JsonLine(parse(validLine(new Path("hdfs://path/from/target/test.parquet"))).value)
    )

    (fileManager.readAllLines _).expects(targetFile.path).returning(targetLines.map(_.toString).asRight).once()

    (fileManager.readAllLines _).expects(oldFile1.path).returning(oldLines1.map(_.toString).asRight).once()

    (fileManager.readAllLines _).expects(oldFile2.path).returning(oldLines2.map(_.toString).asRight).once()

    val res = metadataTool.merge(Seq(oldFile2, oldFile1), targetFile)

    res.value should contain theSameElementsInOrderAs expected
  }

  "filterLastCompact" should "return all metadata files in correct order if no .compact file was present" in {
    val paths = Seq(
      new Path("hdfs://path/to/root/_spark_metadata/0"),
      new Path("hdfs://path/to/root/_spark_metadata/3"),
      new Path("hdfs://path/to/root/_spark_metadata/2"),
      new Path("hdfs://path/to/root/_spark_metadata/1"),
      new Path("hdfs://path/to/root/_spark_metadata/4")
    )

    val expected = Seq(
      MetadataFile(0, compact = false, new Path("hdfs://path/to/root/_spark_metadata/0")),
      MetadataFile(1, compact = false, new Path("hdfs://path/to/root/_spark_metadata/1")),
      MetadataFile(2, compact = false, new Path("hdfs://path/to/root/_spark_metadata/2")),
      MetadataFile(3, compact = false, new Path("hdfs://path/to/root/_spark_metadata/3")),
      MetadataFile(4, compact = false, new Path("hdfs://path/to/root/_spark_metadata/4"))
    )

    val res = metadataTool.filterLastCompact(paths)

    res.value should contain theSameElementsInOrderAs expected
  }

  it should "return latest .compact file and all following metadata files in correct order" in {
    val paths = Seq(
      new Path("hdfs://path/to/root/_spark_metadata/3.compact"),
      new Path("hdfs://path/to/root/_spark_metadata/0"),
      new Path("hdfs://path/to/root/_spark_metadata/3"),
      new Path("hdfs://path/to/root/_spark_metadata/2"),
      new Path("hdfs://path/to/root/_spark_metadata/1"),
      new Path("hdfs://path/to/root/_spark_metadata/1.compact"),
      new Path("hdfs://path/to/root/_spark_metadata/4")
    )

    val expected = Seq(
      MetadataFile(3, compact = true, new Path("hdfs://path/to/root/_spark_metadata/3.compact")),
      MetadataFile(4, compact = false, new Path("hdfs://path/to/root/_spark_metadata/4"))
    )

    val res = metadataTool.filterLastCompact(paths)

    res.value should contain theSameElementsInOrderAs expected
  }

  "filterMetadataFiles" should "return only files with correct names" in {
    val metadataFiles = Seq(
      new Path("hdfs://path/to/root/_spark_metadata/0"),
      new Path("hdfs://path/to/root/_spark_metadata/1"),
      new Path("hdfs://path/to/root/_spark_metadata/2.compact"),
      new Path("hdfs://path/to/root/_spark_metadata/4"),
      new Path("hdfs://path/to/root/_spark_metadata/5"),
      new Path("hdfs://path/to/root/_spark_metadata/6.compact"),
      new Path("hdfs://path/to/root/_spark_metadata/7"),
      new Path("hdfs://path/to/root/_spark_metadata/8")
    )

    val nonMetadataFiles = Seq(
      new Path("hdfs://path/to/root/_spark_metadata/notNumber"),
      new Path("hdfs://path/to/root/_spark_metadata/1.incorrectSuffix"),
      new Path("hdfs://path/to/root/_spark_metadata/.onlyIncorrectSuffix"),
      new Path("hdfs://path/to/root/_spark_metadata/notNumber.incorrectSuffix"),
      new Path("hdfs://path/to/root/_spark_metadata/1.compact.tmp")
    )

    val inputFiles = metadataFiles.concat(nonMetadataFiles)

    val res = metadataTool.filterMetadataFiles(inputFiles)

    res.value should contain theSameElementsAs metadataFiles
  }

  "verifyMetadataFileContent" should "do nothing if file content is correct" in {
    val path = hdfsBasePath.toString

    val lines: Seq[FileLine] = Seq(
      MetadataToolSpec.versionLine,
      MetadataToolSpec.correctJsonLine,
      MetadataToolSpec.correctJsonLine
    )

    val res = metadataTool.verifyMetadataFileContent(path, lines)

    res.value shouldBe (): Unit
  }

  it should "fail if first line is json and not a version" in {
    val path = hdfsBasePath.toString

    val lines: Seq[FileLine] = Seq(MetadataToolSpec.correctJsonLine, MetadataToolSpec.correctJsonLine)

    val expected = MetadataToolSpec.getParsingError(path)
    val res      = metadataTool.verifyMetadataFileContent(path, lines)

    res.left.value shouldBe expected
  }

  it should "fail if first line is string and not a version" in {
    val path = hdfsBasePath.toString

    val lines: Seq[FileLine] = Seq(MetadataToolSpec.notJsonLine, MetadataToolSpec.correctJsonLine)

    val expected = MetadataToolSpec.getParsingError(path)
    val res      = metadataTool.verifyMetadataFileContent(path, lines)

    res.left.value shouldBe expected
  }

  it should "fail if json line is not a json" in {
    val path = hdfsBasePath.toString

    val lines: Seq[FileLine] = Seq(
      MetadataToolSpec.versionLine,
      MetadataToolSpec.correctJsonLine,
      MetadataToolSpec.notJsonLine
    )

    val expected = MetadataToolSpec.getParsingError(path)
    val res      = metadataTool.verifyMetadataFileContent(path, lines)

    res.left.value shouldBe expected
  }

  it should "fail if json line does not contain path key" in {
    val path = hdfsBasePath.toString

    val lines: Seq[FileLine] = Seq(MetadataToolSpec.versionLine, MetadataToolSpec.incorrectJsonLine)

    val expected = MetadataToolSpec.getParsingError(path)
    val res      = metadataTool.verifyMetadataFileContent(path, lines)

    res.left.value shouldBe expected
  }

  it should "fail if file is empty" in {
    val path = hdfsBasePath.toString

    val lines: Seq[FileLine] = Seq()

    val expected = MetadataToolSpec.getParsingError(path)
    val res      = metadataTool.verifyMetadataFileContent(path, lines)

    res.left.value shouldBe expected
  }
}

object MetadataToolSpec {

  val firstPartKey: String   = "key1"
  val hdfsBaseString: String = "hdfs://path/to/root/dir"
  val s3BaseString: String   = "s3://some/base/path"
  val unixBaseString: String = "/some/base/path"
  val hdfsBasePath: Path     = createPath(hdfsBaseString, None, None)
  val s3BasePath: Path       = createPath(s3BaseString, None, None)
  val s3TestPath: Path       = createPath(s3BaseString, createPartitions("key1", "value1").some, "file.parquet".some)
  val unixBasePath: Path     = createPath(unixBaseString, None, None)

  def createPartitions(key1Name: String, key1Value: String): String = s"$key1Name=$key1Value/key2=value2"
  def createPath(basePath: String, partitions: Option[String], fileName: Option[String]): Path = new Path(
    Seq(basePath.some, partitions, fileName).flatten.mkString("/")
  )

  val lineNoPath                    = """{"key":"value","key2":12345,"key3":false}"""
  def validLine(path: Path): String = s"""{"path":"$path","key":12345,"key2":true,"key3":"value3"}"""
  def validLineWithAction(path: Path, action: String): String =
    s"""{"path":"$path","action":"$action","key2":true,"key3":"value3"}"""

  val stringLines: Seq[FileLine] = Seq(
    StringLine("I am a regular String"),
    StringLine("Me too!")
  )

  def jsonLines(basePath: String, firstPartKey: Option[String], count: Int): Seq[FileLine] = for {
    index <- Seq.range(1, count)
    path = createPath(
             basePath,
             firstPartKey.map(key => createPartitions(key, s"value${index.toString}")),
             s"testFile${index.toString}.parquet".some
           )
    json <- parse(validLine(path)).toSeq
  } yield JsonLine(json)

  val mixedDirs = Seq(
    createPath(s3BaseString, s"$SparkMetadataDir".some, None),
    createPath(s3BaseString, s"key=value1".some, None),
    createPath(s3BaseString, s"randomDirectory".some, None),
    createPath(s3BaseString, s"key=value2".some, None),
    createPath(s3BaseString, s"key=value3".some, None),
    createPath(s3BaseString, s"dirWith=Sign".some, None),
    createPath(s3BaseString, s"key=value4".some, None)
  )

  val compactFiles: Seq[Path] = {
    val numFiles = 5
    for { index <- Seq.range(1, numFiles) } yield createPath(
      s3BaseString,
      SparkMetadataDir.some,
      s"$index.compact".some
    )
  }

  val metaFiles: Seq[Path] = {
    val numFiles = 5
    val files =
      for { index <- Seq.range(1, numFiles) } yield createPath(
        s3BaseString,
        SparkMetadataDir.some,
        s"$index".some
      )

    files ++ compactFiles
  }

  val versionLine: StringLine =
    StringLine("v1")
  val correctJsonLine: JsonLine =
    JsonLine(parse("""{"path":"/some/path","key1":12345,"key2":true,"key3":"value3"}""").toOption.get)
  val incorrectJsonLine: JsonLine =
    JsonLine(parse("""{"key1":12345,"key2":true,"key3":"value3"}""").toOption.get)
  val notJsonLine: StringLine =
    StringLine("not, a, json")

  def getParsingError(path: String): ParsingError =
    ParsingError(s"File $path did not match expected format", None)

}
