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

import org.apache.hadoop.fs.Path
import org.scalamock.matchers.ArgCapture
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  CommonPrefix,
  ListObjectsV2Request,
  ListObjectsV2Response,
  PutObjectRequest,
  PutObjectResponse,
  S3Object
}

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

  "makeDir" should "create prefix in bucket tree with no size" in {
    val rootPath = new Path("s3://bucket/path/to/root")
    val dirPath  = new Path(rootPath, "child")

    val expectedReq = ListObjectsV2Request
      .builder()
      .bucket("bucket")
      .prefix(s"path/to/")
      .delimiter("/")
      .build()
    val expectedResp = ListObjectsV2Response
      .builder()
      .prefix("path/to/")
      .delimiter("/")
      .commonPrefixes(
        CommonPrefix.builder().prefix("path/to/dir").build(),
        CommonPrefix.builder().prefix("path/to/root").build(),
        CommonPrefix.builder().prefix("path/to/root/dir1").build(),
        CommonPrefix.builder().prefix("path/to/root/dir2").build()
      )
      .contents(S3Object.builder().key("path/to/root/file1").build())
      .build()
    (s3.listObjectsV2(_: ListObjectsV2Request)).expects(expectedReq).returning(expectedResp)

    val putDirRequest = PutObjectRequest
      .builder()
      .bucket("bucket")
      .key("path/to/root/child/")
      .build()
    val putDirResp = PutObjectResponse
      .builder()
      .build()
    (s3
      .putObject(_: PutObjectRequest, _: RequestBody))
      .expects(putDirRequest, RequestBody.empty())
      .returning(putDirResp)

    val res = io.makeDir(dirPath)

    res should equal(Right(()))

  }

  it should "fail when prefix already exists" in {}

  it should "fail when parent prefix does not exist" in {}

  it should "fail when creating prefix returns an error" in {}

  "getFileStatus" should "return FileStatus for specified file" in {}

  it should "fail when resource or prefix does not exist" in {}

  it should "fail when s3 return error" in {}
}
