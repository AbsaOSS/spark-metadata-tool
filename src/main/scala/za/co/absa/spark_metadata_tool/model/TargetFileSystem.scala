package za.co.absa.spark_metadata_tool.model

sealed trait TargetFilesystem {
  def pathPrefix: String
}

case object Unix extends TargetFilesystem {
  override def pathPrefix: String = "file://"
}
case object Hdfs extends TargetFilesystem {
  override def pathPrefix: String = "hdfs://"
}
case object S3 extends TargetFilesystem {
  override def pathPrefix: String = "s3://"
}
