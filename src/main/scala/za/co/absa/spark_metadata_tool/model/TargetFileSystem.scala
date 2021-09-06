package za.co.absa.spark_metadata_tool.model

sealed trait TargetFilesystem

object TargetFilesystem {

  //TODO: consider supporting Windows FS
  case object Unix extends TargetFilesystem
  case object Hdfs extends TargetFilesystem
  case object S3   extends TargetFilesystem

}
