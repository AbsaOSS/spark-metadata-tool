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

import _root_.io.circe.Decoder
import _root_.io.circe.Encoder
import org.apache.hadoop.fs.Path

import scala.util.Try

object PathDerivation {

  implicit val encodePath: Encoder[Path] = Encoder.encodeString.contramap[Path](_.toString)

  implicit val decodePath: Decoder[Path] = Decoder.decodeString.emapTry(str => Try(new Path(str)))

}
