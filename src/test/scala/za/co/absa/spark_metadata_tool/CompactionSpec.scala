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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CompactionSpec extends AnyFlatSpec with Matchers {
  "lastCompaction" should "calculate number of last compaction file" in {
    val maxMicroBatchNum = 20
    val compactionNum = 5
    // 0, 1, 2, 3, 4.compact, 5, 6, 7, 8, 9.compact, 10, 11, 12, 13, 14.compact, 15, 16, 17, 18, 19.compact, 20
    Compaction.lastCompaction(maxMicroBatchNum, compactionNum) should equal(Some(19))
  }

  it should "return None for small micro batch number (no compacted files were created)" in {
    val maxMicroBatchNum = 0
    val compactionNum = 12
    Compaction.lastCompaction(maxMicroBatchNum, compactionNum) should equal(None)
  }

  it should "work with compactionNum = 1" in {
    val maxMicroBatchNum = 5
    val compactionNum = 1
    Compaction.lastCompaction(maxMicroBatchNum, compactionNum) should equal(Some(4))
  }

  "compactedSize" should "calculate amount of entries in .compact file" in {
    val dataFilesLen = 24  // +1 for 0 metadata file
    val maxMicroBatchNum = 23
    val compactionPeriod = 10

    // (0..19).compact | 20, 21, 22, 23
    Compaction.compactedSize(dataFilesLen, maxMicroBatchNum, compactionPeriod) should equal(20)
  }

  it should "calculate amount of entries in .compact file with less files than maxMicroBatchNum" in {
    val dataFilesLen = 15
    val maxMicroBatchNum = 23
    val compactionPeriod = 10

    // (8..19).compact | 20, 21, 22, 23
    Compaction.compactedSize(dataFilesLen, maxMicroBatchNum, compactionPeriod) should equal(11)
  }

  it should "calaculate amount of missing files as negative" in {
    val dataFilesLen = 4
    val maxMicroBatchNum = 18
    val compactionPeriod = 10

    // 9.compact | 10, 11, 12, 13, 14 | 15, 16, 17, 18
    Compaction.compactedSize(dataFilesLen, maxMicroBatchNum, compactionPeriod) should equal(-5)
  }
}
