package za.co.absa.spark_metadata_tool.model

import spray.json.JsValue

sealed trait FileLine

case class StringLine(line: String) extends FileLine {
  override def toString() = line
}
case class JsonLine(line: JsValue) extends FileLine {
  override def toString() = line.compactPrint
}
