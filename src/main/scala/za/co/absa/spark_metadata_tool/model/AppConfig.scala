package za.co.absa.spark_metadata_tool.model

final case class AppConfig(
  path: String,
  filesystem: TargetFilesystem
)
