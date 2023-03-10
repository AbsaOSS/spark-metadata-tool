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
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.util.Progressable
import org.scalamock.scalatest.MockFactory
import org.scalatest.Inside.inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, EitherValues, OptionValues}
import za.co.absa.spark_metadata_tool.model.IoError

import java.io.{File, FileNotFoundException, IOException}
import java.net.URI
import java.nio.file
import java.nio.file.Paths
import java.util.UUID

class HdfsFileManagerSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with EitherValues
    with MockFactory
    with BeforeAndAfterEach
    with BeforeAndAfterAll {
  val clusterPath: file.Path = Paths.get("miniclusters", s"testCluster_${UUID.randomUUID().toString}")
  val conf                   = new Configuration()
  conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, clusterPath.toAbsolutePath.toString)
  val miniDFSCluster: MiniDFSCluster = new MiniDFSCluster.Builder(conf).build()
  miniDFSCluster.waitClusterUp()
  miniDFSCluster.waitActive()

  val testDataFolder = "/hdfsTestData"
  // getting absolute path and replacing actions are required to keep test running on windows too
  val testDataRootDir: String           = new File(getClass.getResource(testDataFolder).toURI).getAbsolutePath.replace("\\", "/")
  val testDataHdfsRootDir: String       = s"${miniDFSCluster.getFileSystem.getUri.toString}$testDataFolder"
  val topLevelDirOne: String            = "dir1"
  val fileOneInDirOne: String           = "testFile1.txt"
  val fileTwoInDirOne: String           = "testFile2.txt"
  val topLevelDirTwo: String            = "dir2"
  val innerDirInDirTwo: String          = "innerDir1"
  val fileOneInInnerDirInDirTwo: String = "testFile1.txt"

  val hdfsFileManager: HdfsFileManager      = HdfsFileManager(miniDFSCluster.getFileSystem())
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

    val resultDefined   = hdfsFileManager.listFiles(new Path(s"$testDataHdfsRootDir/$topLevelDirOne"))
    val expectedDefined = unixFileManager.listFiles(new Path(s"$testDataRootDir/$topLevelDirOne"))
    resultEmpty.isRight shouldBe true
    resultEmpty.value shouldBe Seq.empty[Path]

    resultDefined.isRight shouldBe true
    expectedDefined.isRight shouldBe true
    resultDefined.value.isEmpty shouldBe false
    resultDefined.value.map(removeRoot(_, testDataHdfsRootDir)) should contain theSameElementsAs
      expectedDefined.value.map(removeRoot(_, testDataRootDir))
  }

  "ListDirectories" should "list directories in dir" in {
    val resultEmpty = hdfsFileManager.listDirectories(new Path(s"$testDataHdfsRootDir/$topLevelDirOne"))

    val resultDefined   = hdfsFileManager.listDirectories(new Path(s"$testDataHdfsRootDir"))
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

  "Write" should "write lines into the file" in {
    val dataToWrite = Seq("111", "222")
    val pathToDir   = s"$testDataHdfsRootDir/$topLevelDirOne"

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

  "Copy" should "copy file from source to destination" in {
    val filesBeforeCopy = hdfsFileManager.listFiles(
      new Path(s"$testDataHdfsRootDir/$topLevelDirTwo")
    )
    val _ = hdfsFileManager.copy(
      new Path(s"$testDataHdfsRootDir/$topLevelDirOne/$fileOneInDirOne"),
      new Path(s"$testDataHdfsRootDir/$topLevelDirTwo/$fileOneInDirOne")
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

  "Copy" should "copy file from source to destination and create destination dir if does not exist" in {
    val filesBeforeCopy = hdfsFileManager.listFiles(
      new Path(s"$testDataHdfsRootDir/$topLevelDirTwo")
    )
    val nonExistingDestinationDir = s"$testDataHdfsRootDir/nonExistingDir/nonExistingSubDir"

    val _ = hdfsFileManager.copy(
      new Path(s"$testDataHdfsRootDir/$topLevelDirOne/$fileOneInDirOne"),
      new Path(s"$nonExistingDestinationDir/$fileOneInDirOne")
    )
    val filesAfterCopy = hdfsFileManager.listFiles(
      new Path(nonExistingDestinationDir)
    )

    filesBeforeCopy.isRight shouldBe true
    filesBeforeCopy.value.isEmpty shouldBe true
    filesAfterCopy.isRight shouldBe true
    filesAfterCopy.value.isEmpty shouldBe false
    filesAfterCopy.value.size shouldBe filesBeforeCopy.value.size + 1

    filesAfterCopy.value.map(removeRoot(_, nonExistingDestinationDir)) should
      contain(s"/$fileOneInDirOne")
  }

  "Delete" should "delete file" in {
    val filesBeforeDelete = hdfsFileManager.listFiles(
      new Path(s"$testDataHdfsRootDir/$topLevelDirOne")
    )
    val _ = hdfsFileManager.delete(
      Seq(
        new Path(s"$testDataHdfsRootDir/$topLevelDirOne/$fileOneInDirOne")
      )
    )
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

  "makeDir" should "create a directory in specified directory tree path" in {
    val res               = hdfsFileManager.makeDir(new Path(testDataHdfsRootDir, "new-dir"))
    val filesAfterMakeDir = hdfsFileManager.listDirectories(new Path(testDataHdfsRootDir))

    res should matchPattern { case Right(_) => }

    inside(filesAfterMakeDir) { case Right(dirs) =>
      dirs.exists(_.getName == "new-dir")
    }
  }

  it should "fail to create dir when directory already exists" in {
    val res = hdfsFileManager.makeDir(new Path(testDataHdfsRootDir, topLevelDirOne))

    inside(res) { case Left(IoError(msg, None)) => msg.endsWith(": File exists") }
  }

  it should "fail when parent dir does not exist" in {
    val res = hdfsFileManager.makeDir(new Path(testDataHdfsRootDir, "non-existing-dir/new-dir"))

    inside(res) { case Left(IoError(msg, _)) => msg.endsWith(": No such file or directory") }
  }

  it should "fail when hdfs FileSystem failure occurs" in {
    val parentPath = new Path("hdfs:///root/")
    val dirPath    = new Path(parentPath, "new")
    val fsMock = new FileSystem {

      override def mkdirs(f: Path, permission: FsPermission): Boolean = {
        f should equal(dirPath)
        throw new IOException("hdfs error")
      }

      override def getFileStatus(f: Path): FileStatus =
        if (parentPath == f) {
          new FileStatus(0L, true, 3, 134217728L, System.currentTimeMillis(), parentPath)
        } else {
          throw new FileNotFoundException(s"$f: does not exist")
        }

      override def getUri: URI                                       = ???
      override def open(f: Path, bufferSize: Int): FSDataInputStream = ???
      override def create(
        f: Path,
        permission: FsPermission,
        overwrite: Boolean,
        bufferSize: Int,
        replication: Short,
        blockSize: Long,
        progress: Progressable
      ): FSDataOutputStream                                                                     = ???
      override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???
      override def rename(src: Path, dst: Path): Boolean                                        = ???
      override def delete(f: Path, recursive: Boolean): Boolean                                 = ???
      override def listStatus(f: Path): Array[FileStatus]                                       = ???
      override def setWorkingDirectory(new_dir: Path): Unit                                     = ???
      override def getWorkingDirectory: Path                                                    = ???
    }
    val hdfsFileManager = HdfsFileManager(fsMock)

    val res = hdfsFileManager.makeDir(dirPath)
    res should matchPattern { case Left(IoError("hdfs error", Some(_))) => }
  }

  "walkFileStatuses" should "search all files in baseDir tree" in {
    val expected = Seq(
      new Path(testDataHdfsRootDir, "dir1/testFile1.txt"),
      new Path(testDataHdfsRootDir, "dir1/testFile2.txt"),
      new Path(testDataHdfsRootDir, "dir2/innerDir1/testFile1.txt")
    )
    val paths = hdfsFileManager
      .walkFileStatuses(new Path(testDataHdfsRootDir), _ => true)
      .map(_.map(_.getPath))
    paths should equal(Right(expected))
  }

  it should "pass only files matched by path filter" in {
    val expected = Seq(
      new Path(testDataHdfsRootDir, "dir1/testFile1.txt"),
      new Path(testDataHdfsRootDir, "dir2/innerDir1/testFile1.txt")
    )
    val paths = hdfsFileManager
      .walkFileStatuses(new Path(testDataHdfsRootDir), _.getName == "testFile1.txt")
      .map(_.map(_.getPath))

    paths should equal(Right(expected))
  }

  it should "should fail on io exception from RemoteIterator" in {
    val parentPath = new Path("hdfs:///root/")
    val dirPath    = new Path(parentPath, "new")
    val fsMock = new FileSystem {

      override def mkdirs(f: Path, permission: FsPermission): Boolean = ???
      override def getFileStatus(f: Path): FileStatus                 = ???
      override def getUri: URI                                        = ???
      override def open(f: Path, bufferSize: Int): FSDataInputStream  = ???
      override def create(
        f: Path,
        permission: FsPermission,
        overwrite: Boolean,
        bufferSize: Int,
        replication: Short,
        blockSize: Long,
        progress: Progressable
      ): FSDataOutputStream                                                                     = ???
      override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???
      override def rename(src: Path, dst: Path): Boolean                                        = ???
      override def delete(f: Path, recursive: Boolean): Boolean                                 = ???

      override def listStatus(f: Path): Array[FileStatus] = {
        throw new IOException("list status io exception")
      }

      override def setWorkingDirectory(new_dir: Path): Unit = ???
      override def getWorkingDirectory: Path                = ???
    }
    val hdfsFileManager = HdfsFileManager(fsMock)

    val res = hdfsFileManager.walkFileStatuses(dirPath, _ => true)

    res should matchPattern { case Left(IoError("list status io exception", Some(_))) => }
  }

  private def removeRoot(path: Path, rootPrefix: String): String = path.toString.replace(rootPrefix, "")
}
