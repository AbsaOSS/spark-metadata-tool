# spark-metadata-tool
Tool to fix _spark_metadata from Structured Streaming queries

## Motivation
Spark Structured Streaming references data files using absolute paths, which makes it impossible to move the data to a different location without breaking functionality.
The tool solves this issue by fixing all paths in Spark metadata files to point to the current location of the data.

## Features
- Fixes all paths in metadata files to point to the current location of the data
- Creates backup of each file before processing
- Backup is deleted after a successful run(can be overridden to keep the backup)
- Currently supported file systems:
    - S3
    - Unix

Note that the tool doesn't perform any validation and assumes files are in the correct state. Consider the following example:

- Old data location: `hdfs://old/path/old_root`
- Current data location: `s3://bucket/new_root`

In this case Spark metadata files might contain paths like
```
hdfs://old/path/old_root/partition1=x/partition2=y/file.parquet
```
After the fix
```
s3://bucket/new_root/partition1=x/partition2=y/file.parquet
```

Only the base path is changed, **no checks are performed** whether the `.parquet` files or partition folders actually exist.

## Usage
### Obtaining
The application is being published as a standalone executable JAR. Simply download the most recent version of the file `spark-metadata-tool-assembly_2.13-x.y.z.jar` from the [package repository](https://github.com/orgs/AbsaOSS/packages?repo_name=spark-metadata-tool).

### Building
To build the package locally, use command
```
sbt clean assembly
```

### Running
Run the application by executing the JAR with desired arguments, e.g.
```
java -jar spark-metadata-tool-assembly_2.13-x.y.z.jar --path "s3://bucket/foo/baz
```

The target filesystem is derived automatically from the provided path:
- `s3://`   for S3 storage
- `/`       for Unix filesystem

### Complete list of allowed arguments:
```
Usage: spark-metadata-tool [options]

  -p, --path <value>  full path to the data folder, including filesystem (e.g. s3://bucket/foo/root)
  -k, --keep-backup   persist backup files after successful run
  -v, --verbose       increase verbosity of application logging
  --log-to-file       enable logging to a file
  --dry-run           enable dry run mode
  --help              print this usage text
```

### S3 Credentials
To be able to perform any operation on S3 you must provide AWS credentials. The easiest way to do so is to set environment variables
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. The application will read them automatically. For more information, as well as other
ways to provide credentials, see [Using credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html)

## Linking
This project is not meant to be linked against, as it is being published as an executable fat jar. 

## Publishing artifacts
Artifacts are published into [GitHub Packages Apache Maven registry](https://docs.github.com/en/packages/learn-github-packages/introduction-to-github-packages).

To publish a package, you will need Personal Access Token with `read: package` and `write: package` permissions, stored in environment variable `GITHUB_TOKEN`.
For other ways to provide PAT, see [sbt-github-packages](https://github.com/djspiewak/sbt-github-packages) plugin page.

- To create new release, checkout new release branch and push it to remote.
Then switch to SBT project `publishing` and run `release` Task:
```
sbt clean release
```

- To simply publish current version of the artifact, run
```
sbt clean publishSigned
```

Note that automatic overwriting of packages (including `-SNAPSHOT` versions) is currently not supported by [GitHub Packages](https://docs.github.com/en/packages/learn-github-packages/introduction-to-github-packages)
and will fail unless the previous package is manually deleted.
