/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark_metadata_tool

import cats.implicits._
import za.co.absa.spark_metadata_tool.io.UnixFileManager
import za.co.absa.spark_metadata_tool.model.AppError

object Application extends App {

  //TODO: proper error handling
  run.leftMap(err => throw new RuntimeException(err.toString()))

  def run: Either[AppError, Unit] = for {
    config <- ArgumentParser.createConfig(args)
    tool    = new MetadataTool(UnixFileManager, config)
    _      <- tool.fixMetadataFiles
  } yield ()

  println("done") //FIXME: PEM
}
