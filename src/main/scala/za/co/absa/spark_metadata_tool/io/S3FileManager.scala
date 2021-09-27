/*
 * Copyright 2021 ABSA Group Limited
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

package za.co.absa.spark_metadata_tool.io

import cats.implicits._
import org.apache.hadoop.fs.Path
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import za.co.absa.spark_metadata_tool.model.All
import za.co.absa.spark_metadata_tool.model.Directory
import za.co.absa.spark_metadata_tool.model.File
import za.co.absa.spark_metadata_tool.model.FileLine
import za.co.absa.spark_metadata_tool.model.FileType
import za.co.absa.spark_metadata_tool.model.IoError

import java.io.ByteArrayOutputStream
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.Using

case class S3FileManager(s3: S3Client) extends FileManager {

  override def readAllLines(path: Path): Either[IoError, Seq[String]] = {
    val bucket = getBucket(path)
    val key    = path.toString.stripPrefix(s"s3://$bucket/")

    val getObjectRequest = GetObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .build()

    val fileStream = getFileStream(getObjectRequest)

    val parsedLines = for {
      stream <- fileStream
      lines  <- parseFileStream(stream)
    } yield lines

    fileStream.map(_.close())

    parsedLines
  }

  override def write(path: Path, lines: Seq[FileLine]): Either[IoError, Unit] = {
    val bucket = getBucket(path)
    val key    = path.toString.stripPrefix(s"s3://$bucket/")

    val objectRequest = PutObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .build()

    for {
      bytes <- toBytes(lines)
      _     <- put(objectRequest, RequestBody.fromBytes(bytes))
    } yield ()
  }

  // lists only non-empty directories
  override def listDirectories(path: Path): Either[IoError, Seq[Path]] = listDir(path, Directory)

  override def listFiles(path: Path): Either[IoError, Seq[Path]] = listDir(path, File)

  private def toBytes(lines: Seq[FileLine]): Either[IoError, Array[Byte]] =
    Using(new ByteArrayOutputStream()) { stream =>
      lines.foreach(l => stream.write(s"${l.toString}\n".getBytes))
      stream.toByteArray
    }.toEither.leftMap(err => IoError(err.getMessage, err.getStackTrace.toSeq.some))

  private def put(request: PutObjectRequest, body: RequestBody): Either[IoError, PutObjectResponse] =
    Try {
      s3.putObject(request, body)
    }.toEither.leftMap(err => IoError(err.getMessage, err.getStackTrace.toSeq.some))

  private def getFileStream(
    getObjectRequest: GetObjectRequest
  ): Either[IoError, ResponseInputStream[GetObjectResponse]] =
    Try {
      s3.getObject(getObjectRequest, ResponseTransformer.toInputStream()): ResponseInputStream[GetObjectResponse]
    }.toEither.leftMap(err => IoError(err.getMessage, err.getStackTrace.toSeq.some))

  private def parseFileStream(stream: ResponseInputStream[GetObjectResponse]): Either[IoError, Seq[String]] =
    Using(Source.fromInputStream(stream)) { src =>
      src.getLines().toSeq
    }.toEither.leftMap(err => IoError(err.getMessage, err.getStackTrace.toSeq.some))

  private def listDir(path: Path, filter: FileType): Either[IoError, Seq[Path]] = {
    val bucket  = getBucket(path)
    val rootKey = path.toString.stripPrefix(s"s3://$bucket/")

    val filterPredicate: Int => Boolean = filter match {
      case All       => _ => true
      case Directory => _ > 1  // e.g. "directory/some.file".split("/") = ("directory", "some.file")
      case File      => _ == 1 // e.g. "some.file".split("/") = ("some.file")
    }

    for {
      keys     <- listBucket(bucket)
      inRoot   <- keys.filter(_.toString.startsWith(rootKey)).asRight
      suffixes <- inRoot.map(_.toString.stripPrefix(s"$rootKey/")).asRight
      names <- suffixes
                 .map(_.split("/"))
                 .filter(p => filterPredicate(p.length))
                 .traverse(_.headOption)
                 .toRight(IoError(s"Splitting paths $suffixes with delimiter '/' returned empty arrays", None))
    } yield names.distinct.map(n => new Path(s"$path/$n"))
  }

  private def listBucket(bucketName: String): Either[IoError, Seq[Path]] = Try {
    val req = ListObjectsV2Request
      .builder()
      .bucket(bucketName)
      .build()

    val res = s3.listObjectsV2(req)

    res.contents.asScala.map(o => new Path(o.key)).toSeq
  }.toEither.leftMap(err => IoError(err.getMessage, err.getStackTrace.toSeq.some))

  private def getBucket(path: Path): String = path.toUri.getHost
}
