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
import za.co.absa.spark_metadata_tool.model.IoError

trait FileManager {
  def listFiles(path: Path): Either[IoError, Seq[Path]]
  def listDirectories(path: Path): Either[IoError, Seq[Path]]
  def readAllLines(path: Path): Either[IoError, Seq[String]]
  def write(path: Path, lines: Seq[String]): Either[IoError, Unit]
  def copy(from: Path, to: Path): Either[IoError, Unit]
  def delete(paths: Seq[Path]): Either[IoError, Unit]

  /** Create directory in specified location
    *
    * Note that directory will be created only if following conditions are met:
    *
    *   - Directory will be created only if parent directory
    *   - Directory will be created only if location with same name doesn't exist.
    * @param dir
    *   Directory path
    * @return
    *   Unit.right if directory was created, otherwise IoError.left with appropriate exception
    */
  def makeDir(dir: Path): Either[IoError, Unit]

  /** Walks through directory tree returning all statuses of visited files
    *
    * FilesStatuses are ordered ascendingly by their uri.
    *
    * @param baseDir
    *   root directory of directory tree
    * @param filter
    *   predicated used for filtering files by path
    * @return
    *   sequence of statuses of visited files their paths satisfied condition, or error.
    */
  def walkFileStatuses(baseDir: Path, filter: Path => Boolean): Either[IoError, Seq[FileStatus]]
}
