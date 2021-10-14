# spark-metadata-tool
Tool to fix _spark_metadata from Structured Streaming queries

## Motivation
Spark Structured Streaming references data files using absolute paths, which makes it impossible to move the data to different location without breaking functionality.
The tool solves this issue by fixing all paths in Spark metadata files to point to current location of the data.

## Features
- Fixes all paths in metadata files to point to current location of the data
- Creates backup of each file before processing
- Backup is deleted after successfull run(can be overriden to keep the backup)
- Currently supported file systems:
    - S3
    - Unix

Note that the tool doesn't perform any validation and assumes files are in correct state

## Usage
### Obtaining
Application is being published as a standalone executable JAR. Simply download most recent version of the JAR from this repository.

### Running
Run the application by executing the JAR with desired arguments, e.g.
```
java -jar spark-metadata-tool-x.y.z.jar --path "s3://bucket/foo/baz
```

Target filesystem is derived automatically from provided path:
- `s3://`   for S3 storage
- `/`       for Unix filesystem

### Complete list of allowed arguments:
```
Usage: spark-metadata-tool [options]

  -p, --path <value>  full path to data folder, including filesystem (e.g. s3://bucket/foo/root)
  -k, --keep-backup   persist backup files after successful run
  -v, --verbose       increase verbosity of application messaging
  --log-to-file       enable logging to a file
  --dry-run           enable dry run mode
  --help              prints this usage text
```

### S3 Credentials
To be able to perform any operation on S3 you must provide AWS credentials. Easiest way to do so is to set environment variables
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Application will read them automatically. For more information, as well as other
ways to provide credentiels, see [Using credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html)

## Linking
This project is not meant to be linked against, as it is being published as an executable fat jar. 

## Publishing artifacts
Artifacts are published into [GitHub Packages Apache Maven registry](https://docs.github.com/en/packages/learn-github-packages/introduction-to-github-packages).

To publish a package, you will need Personal Access Token with `read: package` and `write: package` permissions, stored in environment variable `GITHUB_TOKEN`.
For other ways to provide PAT, see [sbt-github-packages](https://github.com/djspiewak/sbt-github-packages) plugin page.

- To create new release, checkout new release branch and push it to remote.
Then switch to SBT project `publishing` and run `release` Task:
```
sbt "project publishing" release
```

- To simply publish current version of the artifact, run
```
sbt "project publishing" publishSigned
```

Note that automatic overwriting of packages (including `-SNAPSHOT` versions) is currently not supported by [GitHub Packages](https://docs.github.com/en/packages/learn-github-packages/introduction-to-github-packages)
and will fail unless the previous package is manually deleted.
