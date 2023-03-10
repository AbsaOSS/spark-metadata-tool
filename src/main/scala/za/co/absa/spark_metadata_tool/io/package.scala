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

import za.co.absa.spark_metadata_tool.model.IoError
import cats.syntax.either._
import cats.syntax.option._

package object io {
  private[io] val DefaultBlockSize: Long = 64 * 1024 * 1024
  private[io] val DefaultBlockReplication: Int = 1

  private[io] def catchAsIoError[R](resource: => R): Either[IoError, R] =
    Either.catchNonFatal(resource).leftMap(err => IoError(err.getMessage, err.some))
}
