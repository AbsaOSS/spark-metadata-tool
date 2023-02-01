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
    val maxUBatchNum = 20
    val compactionNum = 5
    // 0, 1, 2, 3, 4.compact, 5, 6, 7, 8, 9.compact, 10, 11, 12, 13, 14.compact, 15, 16, 17, 18, 19.compact, 20
    Compaction.lastCompaction(maxUBatchNum, compactionNum) should equal(Some(19))
  }

  it should "return None for small micro batch number (no compacted files were created)" in {
    val maxUBatchNum = 0
    val compactionNum = 12
    Compaction.lastCompaction(maxUBatchNum, compactionNum) should equal(None)  // Maybe Option[Int] would be nice
  }

  it should "work with compactionNum = 1" in {
    val maxUBatchNum = 5
    val compactionNum = 1
    Compaction.lastCompaction(maxUBatchNum, compactionNum) should equal(Some(4))
  }
}
