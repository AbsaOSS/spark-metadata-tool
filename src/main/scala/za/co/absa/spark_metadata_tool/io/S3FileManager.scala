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
import org.apache.hadoop.fs.{FileStatus, Path}
import org.log4s.Logger
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  CopyObjectRequest,
  Delete,
  DeleteObjectsRequest,
  GetObjectRequest,
  GetObjectResponse,
  HeadObjectRequest,
  HeadObjectResponse,
  ListObjectsV2Request,
  ObjectIdentifier,
  PutObjectRequest
}
import za.co.absa.spark_metadata_tool.LoggingImplicits._
import za.co.absa.spark_metadata_tool.model.{All, Directory, File, FileType, IoError}

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

  }.toEither.map(_ => ()).leftMap(err => IoError(err.getMessage, err.some))

  override def delete(paths: Seq[Path]): Either[IoError, Unit] = Try {
    val bucket = getBucket(
      paths.headOption.getOrElse(throw new IllegalArgumentException("Empty list of paths to delete"))
    ) //TODO: use NEL
    val keys = paths.map(p => ObjectIdentifier.builder().key(getKey(p, bucket)).build())

    val del = Delete.builder().objects(keys: _*).quiet(false).build()
    val req = DeleteObjectsRequest.builder().bucket(bucket).delete(del).build()

    s3.deleteObjects(req)

  }.toEither.map(_ => ()).leftMap(err => IoError(err.getMessage, err.some))

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

  override def write(path: Path, lines: Seq[String]): Either[IoError, Unit] =
    for {
      bytes <- toBytes(lines)
      _     <- putToDestination(path, RequestBody.fromBytes(bytes))
    } yield ()

  override def listDirectories(path: Path): Either[IoError, Seq[Path]] =
    listBucket(path, Directory).tap(_.logValueDebug(s"Listed directories in ${path.toString}"))

  override def listFiles(path: Path): Either[IoError, Seq[Path]] =
    listBucket(path, File).tap(_.logValueDebug(s"Listed files in ${path.toString}"))

  override def makeDir(dir: Path): Either[IoError, Unit] = {
    val parentDir = dir.getParent
    val dirName   = dir.getName
    for {
      files <- listBucket(dir.getParent.getParent, All).map(_.filter(_.getParent == parentDir))
      _     <- Either.cond(files.nonEmpty, (), IoError(s"${dir.getParent}: No such file or directory", None))
      _     <- Either.cond(files.forall(_.getName != dirName), (), IoError(s"${dir.getName}: File exists", None))
      _     <- putToDestination(dir, RequestBody.empty())
    } yield ()
  }

  override def getFileStatus(file: Path): Either[IoError, FileStatus] =
    listDirectories(file.getParent).flatMap { dirs =>
      if (dirs.contains(file)) {
        Right(
          new FileStatus(
            0,
            true,
            1,
            1,
            -1,
            file
          )
        )
      } else {
        headObject(file).map(meta =>
          new FileStatus(
            meta.contentLength(),
            false,
            1,
            1,
            meta.lastModified().toEpochMilli,
            file
          )
        )
      }
    }

  override def walkFiles(baseDir: Path, filter: Path => Boolean): Either[IoError, Seq[Path]] =
    listFiles(baseDir).map(_.filter(filter)).tap(_.logValueDebug(s"Contents of directory $baseDir"))

  private def headObject(file: Path): Either[IoError, HeadObjectResponse] = {
    val bucket = getBucket(file)
    val key    = getKey(file, bucket)

    val request = HeadObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .build()

    catchAsIoError(s3.headObject(request))
  }

  private def putToDestination(path: Path, body: RequestBody): Either[IoError, Unit] = {
    val bucket = getBucket(path)
    val key    = getKey(path, bucket)

    val objectRequest = PutObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .build()

    catchAsIoError(s3.putObject(objectRequest, body)).map(_ => ())
  }

  private def toBytes(lines: Seq[String]): Either[IoError, Array[Byte]] =
    Using(new ByteArrayOutputStream()) { stream =>
      lines.foreach(l => stream.write(s"$l\n".getBytes))
      stream.toByteArray
    }.toEither.leftMap(err => IoError(err.getMessage, err.some))

  private def getFileStream(
    getObjectRequest: GetObjectRequest
  ): Either[IoError, ResponseInputStream[GetObjectResponse]] =
    Try {
      s3.getObject(getObjectRequest, ResponseTransformer.toInputStream()): ResponseInputStream[GetObjectResponse]
    }.toEither.leftMap(err => IoError(err.getMessage, err.some))

  private def parseFileStream(stream: ResponseInputStream[GetObjectResponse]): Either[IoError, Seq[String]] =
    Using(Source.fromInputStream(stream)) { src =>
      src.getLines().toSeq
    }.toEither.leftMap(err => IoError(err.getMessage, err.some))

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
  }.toEither.leftMap(err => IoError(err.getMessage, err.some))

  private def getBucket(path: Path): String              = path.toUri.getHost
  private def getKey(path: Path, bucket: String): String = path.toString.stripPrefix(s"s3://$bucket/")
}
