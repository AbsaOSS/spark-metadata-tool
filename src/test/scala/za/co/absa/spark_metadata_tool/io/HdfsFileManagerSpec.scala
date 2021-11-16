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

package za.co.absa.spark_metadata_tool.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, EitherValues, OptionValues}

import java.nio.file
import java.nio.file.Paths
import java.util.UUID


class HdfsFileManagerSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory
  with BeforeAndAfterEach with BeforeAndAfterAll {
  val clusterPath: file.Path = Paths.get("miniclusters", s"testCluster_${UUID.randomUUID().toString}")
  val conf = new Configuration()
  conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, clusterPath.toAbsolutePath.toString)
  val miniDFSCluster: MiniDFSCluster = new MiniDFSCluster.Builder(conf).build()
  miniDFSCluster.waitClusterUp()
  miniDFSCluster.waitActive()

  val testDataFolder = "/hdfsTestData"
  val testDataRootDir: String = getClass.getResource(testDataFolder).getPath
  val testDataHdfsRootDir: String = s"${miniDFSCluster.getFileSystem.getUri.toString}$testDataFolder"
  val topLevelDirOne: String = "dir1"
  val fileOneInDirOne: String = "testFile1.txt"
  val fileTwoInDirOne: String = "testFile2.txt"
  val topLevelDirTwo: String = "dir2"
  val innerDirInDirTwo: String = "innerDir1"
  val fileOneInInnerDirInDirTwo: String = "testFile1.txt"

  val hdfsFileManager: HdfsFileManager = HdfsFileManager(miniDFSCluster.getFileSystem())
  val unixFileManager: UnixFileManager.type = UnixFileManager

  override def beforeEach(): Unit = {
    deleteHdfsData()
    val testDataPath: Path = new Path(getClass.getResource(testDataFolder).getPath)
    miniDFSCluster.getFileSystem.copyFromLocalFile(
      testDataPath,
      new Path(s"${miniDFSCluster.getFileSystem.getUri.toString}/")
    )
  }

  override def afterAll(): Unit = {
    deleteHdfsData()
    miniDFSCluster.shutdown()
  }

  private def deleteHdfsData(): Unit = {
    val _ = miniDFSCluster.getFileSystem().delete(new org.apache.hadoop.fs.Path(testDataHdfsRootDir), true)
  }

  "ListFiles" should "list files in dir" in {
    val resultEmpty = hdfsFileManager.listFiles(new Path(testDataHdfsRootDir))

    val resultDefined = hdfsFileManager.listFiles(new Path(s"$testDataHdfsRootDir/$topLevelDirOne"))
    val expectedDefined = unixFileManager.listFiles(new Path(s"$testDataRootDir/$topLevelDirOne"))
    resultEmpty.isRight shouldBe true
    resultEmpty.value shouldBe Seq.empty[Path]

    resultDefined.isRight shouldBe true
    expectedDefined.isRight shouldBe true
    resultDefined.value.isEmpty shouldBe false
    resultDefined.value.map(removeRoot(_, testDataHdfsRootDir)) shouldBe
      expectedDefined.value.map(removeRoot(_, testDataRootDir))
  }

  "ListDirectories" should "list directories in dir" in {
    val resultEmpty = hdfsFileManager.listDirectories(new Path(s"$testDataHdfsRootDir/$topLevelDirOne"))

    val resultDefined = hdfsFileManager.listDirectories(new Path(s"$testDataHdfsRootDir"))
    val expectedDefined = unixFileManager.listDirectories(new Path(s"$testDataRootDir"))
    resultEmpty.isRight shouldBe true
    resultEmpty.value shouldBe Seq.empty[Path]

    resultDefined.isRight shouldBe true
    expectedDefined.isRight shouldBe true
    resultDefined.value.isEmpty shouldBe false
    resultDefined.value.map(removeRoot(_, testDataHdfsRootDir)) should contain theSameElementsAs
      expectedDefined.value.map(removeRoot(_, testDataRootDir))
  }

  "ReadAllLines" should "read and return content of the file" in {
    val resultFailure = hdfsFileManager.readAllLines(new Path(s"$testDataHdfsRootDir/$topLevelDirOne/fakeFile.txt"))

    val resultSuccess = hdfsFileManager.readAllLines(
      new Path(s"$testDataHdfsRootDir/$topLevelDirOne/$fileOneInDirOne")
    )
    val expectedSuccess = unixFileManager.readAllLines(new Path(s"$testDataRootDir/$topLevelDirOne/$fileOneInDirOne"))

    resultFailure.isLeft shouldBe true

    resultSuccess.isRight shouldBe true
    expectedSuccess.isRight shouldBe true
    resultSuccess.value.isEmpty shouldBe false
    resultSuccess.value shouldBe expectedSuccess.value
  }

  "Write" should "should write lines into the file" in {
    val dataToWrite = Seq("111", "222")
    val pathToDir = s"$testDataHdfsRootDir/$topLevelDirOne"

    val _ = hdfsFileManager.write(
      new Path(s"$pathToDir/$fileOneInDirOne"),
      dataToWrite
    )
    val _ = hdfsFileManager.write(
      new Path(s"$pathToDir/fakeFile.txt"),
      dataToWrite
    )
    val resultExistingFile =
      hdfsFileManager.readAllLines(new Path(s"$pathToDir/$fileOneInDirOne"))
    val resultNonExistingFile =
      hdfsFileManager.readAllLines(new Path(s"$pathToDir/fakeFile.txt"))

    resultExistingFile.isRight shouldBe true
    resultExistingFile.value.isEmpty shouldBe false
    resultExistingFile.value should contain theSameElementsAs dataToWrite

    resultNonExistingFile.isRight shouldBe true
    resultNonExistingFile.value.isEmpty shouldBe false
    resultNonExistingFile.value should contain theSameElementsAs dataToWrite
  }

  "Copy" should "should copy file from source to destination" in {
    val filesBeforeCopy = hdfsFileManager.listFiles(
      new Path(s"$testDataHdfsRootDir/$topLevelDirTwo")
    )
    val _ = hdfsFileManager.copy(
      new Path(s"$testDataHdfsRootDir/$topLevelDirOne/$fileOneInDirOne"),
      new Path(s"$testDataHdfsRootDir/$topLevelDirTwo")
    )
    val filesAfterCopy = hdfsFileManager.listFiles(
      new Path(s"$testDataHdfsRootDir/$topLevelDirTwo")
    )

    filesBeforeCopy.isRight shouldBe true
    filesBeforeCopy.value.isEmpty shouldBe true
    filesAfterCopy.isRight shouldBe true
    filesAfterCopy.value.isEmpty shouldBe false
    filesAfterCopy.value.size shouldBe filesBeforeCopy.value.size + 1

    filesAfterCopy.value.map(removeRoot(_, testDataHdfsRootDir)) should
      contain(s"/$topLevelDirTwo/$fileOneInDirOne")
  }

  "Delete" should "should delete file" in {
    val filesBeforeDelete = hdfsFileManager.listFiles(
      new Path(s"$testDataHdfsRootDir/$topLevelDirOne")
    )
    val _ = hdfsFileManager.delete(Seq(
      new Path(s"$testDataHdfsRootDir/$topLevelDirOne/$fileOneInDirOne")
    ))
    val filesAfterDelete = hdfsFileManager.listFiles(
      new Path(s"$testDataHdfsRootDir/$topLevelDirOne")
    )

    filesBeforeDelete.isRight shouldBe true
    filesBeforeDelete.value.isEmpty shouldBe false
    filesAfterDelete.isRight shouldBe true
    filesBeforeDelete.value.isEmpty shouldBe false
    filesBeforeDelete.value.size should not be filesAfterDelete.value.size
    filesBeforeDelete.value.map(removeRoot(_, testDataHdfsRootDir)) should
      contain(s"/$topLevelDirOne/$fileOneInDirOne")
    filesAfterDelete.value.map(removeRoot(_, testDataHdfsRootDir)) should
      not contain s"/$topLevelDirOne/$fileOneInDirOne"
  }


  private def removeRoot(path: Path, rootPrefix: String): String = path.toString.replace(rootPrefix, "")
}