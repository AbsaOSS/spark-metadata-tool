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

package za.co.absa

import org.apache.hadoop.fs.Path
import spray.json._

import DefaultJsonProtocol._

package object spark_metadata_tool {

  val SparkMetadataDir = "_spark_metadata"
  val BackupDir        = "_spark_metadata_backup"

  implicit object HadoopPathJsonFormat extends RootJsonFormat[Path] {
    def write(p: Path): JsValue = p.toString.toJson
    def read(value: JsValue): Path = value match {
      case JsString(path) => new Path(path)
      case _              => deserializationError(s"Expected path, got $value")
    }
  }
}
