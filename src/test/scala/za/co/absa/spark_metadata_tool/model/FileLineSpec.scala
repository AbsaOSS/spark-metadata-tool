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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

class FileLineSpec extends AnyFlatSpec with Matchers {
  private implicit val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

  "JSON (de)serialization" should "preserve insertion order" in {

    val json = """{"key4":"value4","key":"value","key2":"value2","key3":"value3"}"""
    val expected = List(
      "key4" -> "value4",
      "key"  -> "value",
      "key2" -> "value2",
      "key3" -> "value3"
    )

    val parsed = JsonLine(mapper.readValue(json, classOf[ListMap[String, String]]))

    parsed.fields.toList should contain theSameElementsInOrderAs expected
    FileLine.write(parsed) shouldBe json
  }
}
