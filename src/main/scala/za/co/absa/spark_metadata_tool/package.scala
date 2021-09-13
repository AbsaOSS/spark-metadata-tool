package za.co.absa

import org.apache.hadoop.fs.Path
import spray.json._
import DefaultJsonProtocol._

package object spark_metadata_tool {

  val SparkMetadataDir = "_spark_metadata"

  implicit object HadoopPathJsonFormat extends RootJsonFormat[Path] {
    def write(p: Path) = p.toString.toJson
    def read(value: JsValue) = value match {
      case JsString(path) => new Path(path)
      case _              => deserializationError(s"Expected path, got $value")
    }
  }
}
