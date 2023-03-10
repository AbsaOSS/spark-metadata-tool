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

object Compaction {

  /** Calculates amount of metadata entries entering latest `.compact` file
    *
    * Such number might be negative meaning, that we have to create dummy metadata files. The amount of missing metadata
    * files is negative of returned value (`- Compaction#compactedSize(...)`).
    *
    * @param statusLen
    *   size of collected data files by metadata tool.
    * @param maxMicroBatchNum
    *   highest number of metadata file
    * @param compactionPeriod
    *   period of compaction cycle
    * @return
    *   amount of entries in compact files, or if negative, amount of missing entries
    */
  def compactedSize(statusLen: Int, maxMicroBatchNum: Int, compactionPeriod: Int): Int =
    statusLen - (maxMicroBatchNum % compactionPeriod) - 1 // statusLen - (maxMicroBatchNum - lastCompaction)

  /** Calculate batch number of last compacted file.
    *
    * If `compactionPeriod` is greater than `maxMicroBatchNumber` `None` is returned instead of `-1`.
    *
    * @param maxMicroBatchNum
    *   highest number of metadata file
    * @param compactionPeriod
    *   period of compaction cycle
    * @return
    *   micro batch number of last compact file or none
    */
  def lastCompaction(maxMicroBatchNum: Int, compactionPeriod: Int): Option[Int] =
    if (maxMicroBatchNum > compactionPeriod - 1) {
      Some(maxMicroBatchNum - (maxMicroBatchNum % compactionPeriod) - 1)
    } else {
      None
    }
}
