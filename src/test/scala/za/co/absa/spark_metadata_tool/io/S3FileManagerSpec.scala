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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalamock.matchers.ArgCapture
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CommonPrefix, HeadObjectRequest, HeadObjectResponse, ListObjectsV2Request, ListObjectsV2Response, NoSuchKeyException, PutObjectRequest, PutObjectResponse, S3Exception, S3Object}
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import za.co.absa.spark_metadata_tool.model.IoError

import java.time.Instant
import scala.io.Source
import scala.util.Using

class S3FileManagerSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory {

  private val s3 = mock[S3Client]

  private val io = S3FileManager(s3)

  "Write" should "correctly serialize file contents to bytes" in {

    val path = new Path("s3://bucket/path/to/root")
    val lines = Seq(
      "I am a regular String",
      "Me too!",
      """{"key":"value","key2":54321,"key3":false}""",
      """{"key":"value4","key2":12345","key3":true}"""
    )
    val response = PutObjectResponse.builder().build()
    val reqBody  = ArgCapture.CaptureOne[RequestBody]()

    (s3.putObject(_: PutObjectRequest, _: RequestBody)).expects(*, capture(reqBody)).returning(response)

    val res = io.write(path, lines)

    val content = Using(Source.fromInputStream(reqBody.value.contentStreamProvider().newStream())) { src =>
      src.getLines().toSeq
    }.toEither

    res.isRight shouldBe true
    content.value shouldBe lines

  }

  "makeDir" should "check that parent directory exists and expected dir is not present" in {
    val rootPath = new Path("s3://bucket/path/to/root")
    val dirPath  = new Path(rootPath, "child")

    val expectedReq = ListObjectsV2Request
      .builder()
      .bucket("bucket")
      .prefix(s"path/to/root/")
      .delimiter("/")
      .build()
    val expectedResp = ListObjectsV2Response
      .builder()
      .prefix("path/to/root/")
      .delimiter("/")
      .commonPrefixes(
        CommonPrefix.builder().prefix("path/to/root/").build(),
        CommonPrefix.builder().prefix("path/to/root/dir1").build(),
        CommonPrefix.builder().prefix("path/to/root/dir2").build()
      )
      .contents(S3Object.builder().key("path/to/root/file1").build())
      .build()
    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(expectedReq).returning(expectedResp)

    val res = io.makeDir(dirPath)

    res should equal(Right(()))

  }

  it should "fail when prefix already exists" in {
    val rootPath = new Path("s3://bucket/path/to/root")
    val dirPath  = new Path(rootPath, "child")

    val expectedReq = ListObjectsV2Request
      .builder()
      .bucket("bucket")
      .prefix(s"path/to/root/")
      .delimiter("/")
      .build()
    val expectedResp = ListObjectsV2Response
      .builder()
      .prefix("path/to/root/")
      .delimiter("/")
      .commonPrefixes(
        CommonPrefix.builder().prefix("path/to/root/child/hidden=True").build(),
        CommonPrefix.builder().prefix("path/to/root/child/hidden=False").build(),
        CommonPrefix.builder().prefix("path/to/root/dir1/").build(),
        CommonPrefix.builder().prefix("path/to/root/dir2/").build()
      )
      .contents(S3Object.builder().key("path/to/root/file1").build())
      .build()
    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(expectedReq).returning(expectedResp)

    val res = io.makeDir(dirPath)

    res should equal(Left(IoError("child: File exists", None)))
  }

  it should "fail when parent prefix does not exist" in {
    val rootPath = new Path("s3://bucket/path/to/root")
    val dirPath  = new Path(rootPath, "child")

    val expectedReq = ListObjectsV2Request
      .builder()
      .bucket("bucket")
      .prefix(s"path/to/root/")
      .delimiter("/")
      .build()
    val expectedResp = ListObjectsV2Response
      .builder()
      .prefix("path/to/root/")
      .delimiter("/")
      .build()
    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(expectedReq).returning(expectedResp)

    val res = io.makeDir(dirPath)

    res should equal(Left(IoError("s3://bucket/path/to/root: No such file or directory", None)))
  }

  "getFileStatus" should "return FileStatus for specified file" in {
    val file    = new Path("s3://bucket/path/to/file.csv")
    val modTime = Instant.parse("2022-12-25T12:31:54.962Z")
    val bytes   = 123456L

    val dirsRequest = ListObjectsV2Request
      .builder()
      .prefix("path/to/")
      .bucket("bucket")
      .delimiter("/")
      .build()

    val dirsResp = ListObjectsV2Response
      .builder()
      .delimiter("/")
      .contents(S3Object.builder().key("file.csv").build())
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(dirsRequest).returning(dirsResp)

    val expectedRequest = HeadObjectRequest
      .builder()
      .key("path/to/file.csv")
      .bucket("bucket")
      .build()

    val expectedResponse = HeadObjectResponse
      .builder()
      .contentLength(bytes)
      .contentType("text/csv")
      .lastModified(modTime)
      .build()

    (s3.headObject(_: HeadObjectRequest)).expects(expectedRequest).returning(expectedResponse)

    val res = io.getFileStatus(file)

    res should equal(
      Right(
        new FileStatus(
          bytes,
          false,
          1,
          1,
          modTime.toEpochMilli,
          file
        )
      )
    )
  }

  it should "fail when resource or prefix does not exist" in {
    val file = new Path("s3://bucket/path/to/file.csv")

    val dirsRequest = ListObjectsV2Request
      .builder()
      .prefix("path/to/")
      .bucket("bucket")
      .delimiter("/")
      .build()

    val dirsResp = ListObjectsV2Response
      .builder()
      .delimiter("/")
      .contents(S3Object.builder().key("other.csv").build())
      .build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(dirsRequest).returning(dirsResp)

    val expectedRequest = HeadObjectRequest
      .builder()
      .key("path/to/file.csv")
      .bucket("bucket")
      .build()

    (s3
      .headObject(_: HeadObjectRequest))
      .expects(expectedRequest)
      .throwing(NoSuchKeyException.builder().message("key does not exist").build())

    io.getFileStatus(file) should matchPattern { case Left(IoError("key does not exist", Some(_))) => }
  }

  it should "fail when s3 return error" in {
    val file = new Path("s3://bucket/path/to/file.csv")

    val dirsRequest = ListObjectsV2Request
      .builder()
      .prefix("path/to/")
      .bucket("bucket")
      .delimiter("/")
      .build()

    val error = S3Exception.builder().message("s3 exception").build()

    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(dirsRequest).throwing(error)

    io.getFileStatus(file) should matchPattern { case Left(IoError("s3 exception", Some(`error`))) => }
  }

  "walkFiles" should "recursively list file tree in subdirectory" in {
    val rootDir = new Path("s3://bucket/path/to/root")

    val listRequest = ListObjectsV2Request
      .builder()
      .prefix("path/to/root/")
      .bucket("bucket")
      .build()

    val listResponse = ListObjectsV2Response
      .builder()
      .prefix("path/to/root")
      .commonPrefixes(
        CommonPrefix.builder().prefix("gender=Male").build(),
        CommonPrefix.builder().prefix("gender=Female").build(),
        CommonPrefix.builder().prefix("gender=Unknown").build()
      )
      .contents(
        S3Object
          .builder()
          .key("path/to/root/gender=Male/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Male/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Male/part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Female/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Female/part-00003-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Unknown/part-00004-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build()
      )
      .build()

    (s3.listObjectsV2Paginator(_: ListObjectsV2Request))
      .expects(listRequest)
      .returning(new ListObjectsV2Iterable(s3, listRequest))

    (s3.listObjectsV2(_: ListObjectsV2Request))
      .expects(listRequest)
      .returning(listResponse)

    io.walkFiles(rootDir, _ => true) should equal(
      Right(
        Seq(
          new Path(
            "s3://bucket/path/to/root/gender=Male/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          ),
          new Path(
            "s3://bucket/path/to/root/gender=Male/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          ),
          new Path(
            "s3://bucket/path/to/root/gender=Male/part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          ),
          new Path(
            "s3://bucket/path/to/root/gender=Female/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          ),
          new Path(
            "s3://bucket/path/to/root/gender=Female/part-00003-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          ),
          new Path(
            "s3://bucket/path/to/root/gender=Unknown/part-00004-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          )
        )
      )
    )
  }

  it should "apply filter on returned files" in {
    val rootDir = new Path("s3://bucket/path/to/root")

    val listRequest = ListObjectsV2Request
      .builder()
      .prefix("path/to/root/")
      .bucket("bucket")
      .build()

    val listResponse = ListObjectsV2Response
      .builder()
      .prefix("path/to/root")
      .commonPrefixes(
        CommonPrefix.builder().prefix("gender=Male").build(),
        CommonPrefix.builder().prefix("gender=Female").build(),
        CommonPrefix.builder().prefix("gender=Unknown").build()
      )
      .contents(
        S3Object
          .builder()
          .key("path/to/root/_SUCCESS")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Male/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Male/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet.crc")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Male/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Male/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet.crc")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Male/part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Male/part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet.crc")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Female/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Female/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet.crc")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Female/part-00003-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Female/part-00003-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet.crc")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Unknown/part-00004-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet")
          .build(),
        S3Object
          .builder()
          .key("path/to/root/gender=Unknown/part-00004-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet.crc")
          .build()
      )
      .build()

    (s3.listObjectsV2Paginator(_: ListObjectsV2Request))
      .expects(listRequest)
      .returning(new ListObjectsV2Iterable(s3, listRequest))

    (s3.listObjectsV2(_: ListObjectsV2Request))
      .expects(listRequest)
      .returning(listResponse)

    io.walkFiles(rootDir, _.getName.endsWith("parquet")) should equal(
      Right(
        Seq(
          new Path(
            "s3://bucket/path/to/root/gender=Male/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          ),
          new Path(
            "s3://bucket/path/to/root/gender=Male/part-00001-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          ),
          new Path(
            "s3://bucket/path/to/root/gender=Male/part-00002-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          ),
          new Path(
            "s3://bucket/path/to/root/gender=Female/part-00000-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          ),
          new Path(
            "s3://bucket/path/to/root/gender=Female/part-00003-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          ),
          new Path(
            "s3://bucket/path/to/root/gender=Unknown/part-00004-a1216290-6a82-4a9d-9e6c-de1e7c9bbe5b.c000.snappy.parquet"
          )
        )
      )
    )

  }

  it should "fail on s3 error" in {
    val rootDir = new Path("s3://bucket/path/to/root")

    val listRequest = ListObjectsV2Request
      .builder()
      .prefix("path/to/root/")
      .bucket("bucket")
      .build()

    val error = S3Exception.builder().message("s3 error").build()

    (s3.listObjectsV2Paginator(_: ListObjectsV2Request)).expects(listRequest).throwing(error)

    io.walkFiles(rootDir, _ => true) should equal(Left(IoError("s3 error", Some(error))))
  }
}
