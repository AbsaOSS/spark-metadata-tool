package za.co.absa.spark_metadata_tool.model

import org.apache.hadoop.fs.Path

case class ToolFileStatus(
  path: Path,
  size: Long,
  isDir: Boolean,
  modificationTime: Long,
  blockReplication: Int,
  blockSize: Long
)
