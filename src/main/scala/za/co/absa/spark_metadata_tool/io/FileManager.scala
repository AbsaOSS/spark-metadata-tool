/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark_metadata_tool.io

import za.co.absa.spark_metadata_tool.model.IoError

//TODO: refactor list* into single parametrized method
trait FileManager {
  def listFiles(path: String): Either[IoError, Seq[String]]
  def listDirs(path: String): Either[IoError, Seq[String]]
  def readAllLines(path: String): Either[IoError, Seq[String]]
  def write(path: String, lines: Seq[String]): Either[IoError, Unit]
  def delete(path: String): Either[IoError, Unit]
  def move(from: String, to: String): Either[IoError, Unit]
}
