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

import org.log4s.Logger

object LoggingImplicits {

  implicit class EitherOps(e: Either[_, _]) {

    def logInfo(message: String)(implicit logger: Logger): Unit       = log(message, logValue = false, logger.info(_: String))
    def logDebug(message: String)(implicit logger: Logger): Unit      = log(message, logValue = false, logger.debug(_: String))
    def logValueInfo(message: String)(implicit logger: Logger): Unit  = log(message, logValue = true, logger.info(_: String))
    def logValueDebug(message: String)(implicit logger: Logger): Unit = log(message, logValue = true, logger.debug(_: String))

    private def log(msg: String, logValue: Boolean, logChannel: String => Unit): Unit = e.fold(
      _ => (), // Do nothing, handle errors at application entry point
      v => logChannel(s"$msg${if (logValue) s" : ${v.toString}" else ""}")
    )

  }

}
