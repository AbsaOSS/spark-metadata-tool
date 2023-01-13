package za.co.absa.spark_metadata_tool.model

object SinkFileStatus {
  def from(status: ToolFileStatus, action: String): SinkFileStatus =
    SinkFileStatus(
      status.path.toString,
      status.size,
      status.isDir,
      status.modificationTime,
      status.blockReplication,
      status.blockSize,
      action
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
