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

sealed trait AppError {
  def msg: String
}

sealed trait AppErrorWithThrowable extends AppError {
  def ex: Option[Throwable]
}

case class InitializationError(msg: String, ex: Option[Throwable]) extends AppErrorWithThrowable
case class IoError(msg: String, ex: Option[Throwable])             extends AppErrorWithThrowable
case class ParsingError(msg: String, ex: Option[Throwable])        extends AppErrorWithThrowable
case class NotFoundError(msg: String)                              extends AppError
case class UnknownFileSystemError(msg: String)                     extends AppError
case class UnknownError(msg: String)                               extends AppError
case class NotImplementedError(msg: String)                        extends AppError
case class JsonError(msg: String)                                  extends AppError
