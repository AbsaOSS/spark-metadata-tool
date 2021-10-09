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

// import org.apache.hadoop.fs.Path
// import org.scalamock.matchers.ArgCapture
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
// import software.amazon.awssdk.core.sync.RequestBody
// import software.amazon.awssdk.services.s3.S3Client
// import software.amazon.awssdk.services.s3.model.PutObjectRequest
// import software.amazon.awssdk.services.s3.model.PutObjectResponse
// import za.co.absa.spark_metadata_tool.model.JsonLine
// import za.co.absa.spark_metadata_tool.model.StringLine

// import scala.io.Source
// import scala.util.Using

class S3FileManagerSpec extends AnyFlatSpec with Matchers with OptionValues with EitherValues with MockFactory {

  //FIXME: re-enable once JSON serialization works
  // private val s3 = mock[S3Client]

  // private val io = S3FileManager(s3)

  // "Write" should "correctly serialize file contents to bytes" in {

  //   val path = new Path("s3://bucket/path/to/root")
  //   val lines = Seq(
  //     StringLine("I am a regular String"),
  //     StringLine("Me too!"),
  //     JsonLine("""{"key":"value","key2":"value2","key3":"value3"}""".parseJson.asJsObject),
  //     JsonLine("""{"key":"value4","key2":"value5","key3":"value6"}""".parseJson.asJsObject)
  //   )
  //   val response = PutObjectResponse.builder().build()
  //   val reqBody  = ArgCapture.CaptureOne[RequestBody]()

  //   (s3.putObject(_: PutObjectRequest, _: RequestBody)).expects(*, capture(reqBody)).returning(response)

  //   val res = io.write(path, lines)

  //   val content = Using(Source.fromInputStream(reqBody.value.contentStreamProvider().newStream())) { src =>
  //     src.getLines().toSeq
  //   }.toEither

  //   res.isRight shouldBe true
  //   content.value shouldBe lines.map(_.toString)

  // }
}
