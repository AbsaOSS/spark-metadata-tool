package za.co.absa.spark_metadata_tool.model

trait AppError

case class UnknownFileSystemError(msg: String) extends AppError

case class UnknownError(msg: String) extends AppError
