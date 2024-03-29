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

import org.apache.hadoop.fs.FileStatus

object SinkFileStatus {

  def asAddStatus(status: FileStatus): SinkFileStatus =
    SinkFileStatus(
      status.getPath.toString,
      status.getLen,
      status.isDirectory,
      status.getModificationTime,
      status.getReplication.toInt,
      status.getBlockSize,
      Action.Add
    )
}

/** SinkFileStatus model used for serializing metadata from spark streaming job.
  *
  * This class was copied from ``org.apache.spark.sql.execution.streaming.SinkFileStatus``
  */
case class SinkFileStatus(
  path: String,
  size: Long,
  isDir: Boolean,
  modificationTime: Long,
  blockReplication: Int,
  blockSize: Long,
  action: String
)
