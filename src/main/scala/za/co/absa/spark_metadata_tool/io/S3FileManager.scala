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

package za.co.absa.spark_metadata_tool.io

import cats.implicits._
import org.apache.hadoop.fs.Path
import org.log4s.Logger
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ObjectIdentifier
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import za.co.absa.spark_metadata_tool.LoggingImplicits._
import za.co.absa.spark_metadata_tool.model.All
import za.co.absa.spark_metadata_tool.model.Directory
import za.co.absa.spark_metadata_tool.model.File
import za.co.absa.spark_metadata_tool.model.FileType
import za.co.absa.spark_metadata_tool.model.IoError

import java.io.ByteArrayOutputStream
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.Using
import scala.util.chaining._

case class S3FileManager(s3: S3Client) extends FileManager {
  implicit private val logger: Logger = org.log4s.getLogger

  override def copy(from: Path, to: Path): Either[IoError, Unit] = Try {
    val bucket  = getBucket(from)
    val srcKey  = getKey(from, bucket)
    val destKey = getKey(to, bucket)

    val request = CopyObjectRequest
      .builder()
      .sourceBucket(bucket)
      .sourceKey(srcKey)
      .destinationBucket(bucket)
      .destinationKey(destKey)
      .build()

    s3.copyObject(request)

  }.toEither.map(_ => ()).leftMap(err => IoError(err.getMessage, err.getStackTrace.toSeq.some))

  override def delete(paths: Seq[Path]): Either[IoError, Unit] = Try {
    val bucket = getBucket(
      paths.headOption.getOrElse(throw new IllegalArgumentException("Empty list of paths to delete"))
    ) //TODO: use NEL
    val keys = paths.map(p => ObjectIdentifier.builder().key(getKey(p, bucket)).build())

    val del = Delete.builder().objects(keys: _*).quiet(false).build()
    val req = DeleteObjectsRequest.builder().bucket(bucket).delete(del).build()

    s3.deleteObjects(req)

  }.toEither.map(_ => ()).leftMap(err => IoError(err.getMessage, err.getStackTrace.toSeq.some))

  override def readAllLines(path: Path): Either[IoError, Seq[String]] = {
    val bucket = getBucket(path)
    val key    = getKey(path, bucket)

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

  override def write(path: Path, lines: Seq[String]): Either[IoError, Unit] = {
    val bucket = getBucket(path)
    val key    = getKey(path, bucket)

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

  override def listDirectories(path: Path): Either[IoError, Seq[Path]] =
    listBucket(path, Directory).tap(_.logValueDebug(s"Listing files in ${path.toString}"))

  override def listFiles(path: Path): Either[IoError, Seq[Path]] =
    listBucket(path, File).tap(_.logValueDebug(s"Listing directories in ${path.toString}"))

  private def toBytes(lines: Seq[String]): Either[IoError, Array[Byte]] =
    Using(new ByteArrayOutputStream()) { stream =>
      lines.foreach(l => stream.write(s"$l\n".getBytes))
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

  private def listBucket(path: Path, filter: FileType): Either[IoError, Seq[Path]] = Try {
    val bucket     = getBucket(path)
    val pathPrefix = s"s3://$bucket/"
    val rootKey    = path.toString.stripPrefix(pathPrefix)

    val req = ListObjectsV2Request
      .builder()
      .bucket(bucket)
      .prefix(s"$rootKey/")
      .delimiter("/")
      .build()

    val res = s3.listObjectsV2(req)

    lazy val files = res.contents.asScala.map(f => new Path(s"$pathPrefix${f.key}"))
    lazy val dirs  = res.commonPrefixes.asScala.map(d => new Path(s"$pathPrefix${d.prefix}"))

    filter match {
      case File      => files.toSeq
      case Directory => dirs.toSeq
      case All       => (files ++ dirs).toSeq
    }
  }.toEither.leftMap(err => IoError(err.getMessage, err.getStackTrace.toSeq.some))

  private def getBucket(path: Path): String              = path.toUri.getHost
  private def getKey(path: Path, bucket: String): String = path.toString.stripPrefix(s"s3://$bucket/")
}
