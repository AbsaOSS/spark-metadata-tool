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

import org.apache.hadoop.fs.Path

final case class AppConfig(
  mode: Mode,
  oldPath: Option[Path],
  path: Path,
  filesystem: TargetFilesystem,
  keepBackup: Boolean,
  verbose: Boolean,
  logToFile: Boolean,
  dryRun: Boolean
)

sealed trait Mode
case object FixPaths                                                              extends Mode
case object Merge                                                                 extends Mode
case object CompareMetadataWithData                                               extends Mode
final case class CreateMetadata(maxMicroBatchNumber: Int, compactionNumber: Int) extends Mode
