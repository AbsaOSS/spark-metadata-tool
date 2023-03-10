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
import org.apache.http.client.utils.URIBuilder
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
  ListObjectsV2Request,
  ObjectIdentifier,
  PutObjectRequest,
  S3Object
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
  import S3FileManager._
  implicit private val logger: Logger = org.log4s.getLogger

  override def copy(from: Path, to: Path): Either[IoError, Unit] = Try {
    val bucket  = getBucket(from)
    val srcKey  = getKey(from)
    val destKey = getKey(to)

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
    val keys = paths.map(p => ObjectIdentifier.builder().key(getKey(p)).build())

    val del = Delete.builder().objects(keys: _*).quiet(false).build()
    val req = DeleteObjectsRequest.builder().bucket(bucket).delete(del).build()

    s3.deleteObjects(req)

  }.toEither.map(_ => ()).leftMap(err => IoError(err.getMessage, err.some))

  override def readAllLines(path: Path): Either[IoError, Seq[String]] = {
    val bucket = getBucket(path)
    val key    = getKey(path)

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

  override def makeDir(dir: Path): Either[IoError, Unit] =
    for {
      fls <- listDirectories(dir.getParent)
      _   <- Either.cond(fls.nonEmpty, (), IoError(s"${dir.getParent}: No such file or directory", None))
      _   <- Either.cond(notContainDir(fls, dir), (), IoError(s"${dir.getName}: File exists", None))
    } yield ()

  override def walkFileStatuses(baseDir: Path, filter: Path => Boolean): Either[IoError, Seq[FileStatus]] = {
    val bucket = getBucket(baseDir)
    val prefix = ensureTrailingSlash(getKey(baseDir))

    val builder = new URIBuilder().setScheme("s3").setHost(bucket)

    val request = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()

    // listObjectsV2 returns objects in ascending order by key, so we don't have to order them
    catchAsIoError {
      for {
        page   <- s3.listObjectsV2Paginator(request).iterator().asScala
        obj    <- page.contents().asScala
        status <- Some(new Path(builder.setPath(obj.key()).build())).filter(filter).map(asFileStatus(obj))
      } yield status
    }.map(_.toSeq)
  }

  private def putToDestination(path: Path, body: RequestBody): Either[IoError, Unit] = {
    val bucket = getBucket(path)
    val key    = getKey(path)

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
}

object S3FileManager {

  private def getBucket(path: Path): String =
    path.toUri.getHost

  private def notContainDir(prefixes: Iterable[Path], dir: Path): Boolean = {
    val dirPath = ensureTrailingSlash(getKey(dir))
    prefixes.forall(pref => !getKey(pref).startsWith(dirPath))
  }

  private def getKey(path: Path): String =
    path.toUri.getPath.stripPrefix("/")

  private def ensureTrailingSlash(path: String): String = path.last match {
    case '/' => path
    case _   => path :+ '/'
  }

  private def asFileStatus(s3Object: S3Object)(path: Path): FileStatus =
    new FileStatus(
      s3Object.size(),
      false,
      DefaultBlockReplication,
      DefaultBlockSize,
      s3Object.lastModified().toEpochMilli,
      path
    )
}
