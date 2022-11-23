# spark-metadata-tool
Tool to fix _spark_metadata from Structured Streaming queries

## Motivation
Spark Structured Streaming references data files using absolute paths, which makes it impossible to move the data to a different location without breaking functionality.
The tool solves this issue by fixing all paths in Spark metadata files to point to the current location of the data.

## Features
The tool currently provides 3 run modes - `fix-paths`, `merge` and `compare-metadata-with-data`.

### fix-paths
- Fixes all paths in metadata files to point to the current location of the data

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

### merge
- Merges content of the metadata files into the files in another Spark metadata directory

Example:
- Old `_spark_metadata` directory containing metadata files `0`, `1`, `2`, `3`, `3.compact`, `4`, `5`
- New `_spark_metadata` directory containing metadata files `0`, `1`, `2`, `3`, `4`, `5`, `5.compact`, `6`

1. The tool finds the target file, into which it will write the data. This is either the latest `.compact` file, or earliest regular file, if no `.compact` files are present.
In the above example, merged data would be written into file `5.compact`.
2. The tool determines, which files from the old `_spark_metadata` directory use for merging. They're either the latest `.compact` file and all following regular files in order,
or simply all regular files, in case no `.compact` files are present.
3. The contents of the old metadata files are merged into the target file. For the example above, the result would be as follows:
```
version                 // First line taken from target file, i.e. `5.compact`.
lines from 3.compact    // Contents of the lates .compact file from the old metadata directory. Version is omitted.
lines from 4            // Contents of the following regular file from the old metadata directory. Version is omitted.
lines from 5            // Same as with `4`.
lines from 5.compact   // Remaining contents of the target metadata file, i.e. 5.compact from the new metadata directory.
```

In every run mode, the tool offers following universal features:
- Creates backup of each file before processing
- Backup is deleted after a successful run(can be overridden to keep the backup)
- Currently supported file systems:
    - S3
    - Unix
    - HDFS

### compare-metadata-with-data
- Compares metadata records with data and log all inconsistencies

Note that the tool does not perform any operation to file system

## Usage
### Obtaining
The application is being published as a standalone executable JAR. Simply download the most recent version of the file `spark-metadata-tool_2.13-x.y.z-assembly.jar` from the [package repository](https://github.com/orgs/AbsaOSS/packages?repo_name=spark-metadata-tool).

### Building
To build the package locally, use command
```
sbt clean assembly
```

### Running
Run the application by executing the JAR with desired arguments, e.g.
```
java -jar spark-metadata-tool_2.13-x.y.z-assembly.jar fix-paths --path "s3://bucket/foo/baz
```

The target filesystem is derived automatically from the provided path:
- `s3://`             for S3 storage
- `/`                 for Unix filesystem
- `hdfs://<url:port>` for HDFS filesystem

### Complete list of allowed arguments:
```
Usage: spark-metadata-tool [fix-paths|merge] [options]

Command: fix-paths [options]
Fix paths in Spark metadata files to match current location
  -p, --path <value>       full path to the data folder, including filesystem (e.g. s3://bucket/foo/root)


Command: merge [options]
Merge Spark metadata files from 2 directories
  -o, --old <value>        full path to the old data folder, including filesystem (e.g. s3://bucket/foo/old)
  -n, --new <value>        full path to the new data folder, including filesystem (e.g. s3://bucket/foo/new)

Command: compare-metadata-with-data [options]
Compares metadata records with data and log all inconsistencies
  -p, --path <value>       full path to the data folder, including filesystem (e.g. s3://bucket/foo/root)


Other options:
  -k, --keep-backup        persist backup files after successful run
  -v, --verbose            increase verbosity of application logging
  --log-to-file            enable logging to a file
  --dry-run                enable dry run mode
  --help                   print this usage text
```

### S3 Credentials
To be able to perform any operation on S3 you must provide AWS credentials. The easiest way to do so is to set environment variables
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. The application will read them automatically. For more information, as well as other
ways to provide credentials, see [Using credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html)

### HDFS Set up
To be able to perform any operation on HDFS you must set environment variable `HADOOP_CONF_DIR`.

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

## How to generate Code coverage report
```sbt
sbt jacoco
```
Code coverage will be generated on path:
```
{project-root}/target/scala-{scala_version}/jacoco/report/html
```
