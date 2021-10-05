# spark-metadata-tool
Tool to fix _spark_metadata from Structured Streaming queries

## Linking
This project is not meant to be linked against, as it is being published as an executable fat jar. 

## Publishing artifacts
Artifacts are published into [Github Packages Apache Maven registry](https://docs.github.com/en/packages/learn-github-packages/introduction-to-github-packages).

To publish a package, you will need Personal Access Token with `read: package` and `write: package` permissions, stored in environment variable `GITHUB_TOKEN`.
For other ways to provide PAT, see [sbt-github-packages](https://github.com/djspiewak/sbt-github-packages) plugin page.

You can then create a new release by running
```
sbt "project publishing" release
```
or simply publish current version of the artifact by running
```
sbt "project publishing" publishSigned
```

Note that automatic overwriting of packages (including `-SNAPSHOT` versions) is currently not supported by [Github Packages](https://docs.github.com/en/packages/learn-github-packages/introduction-to-github-packages)
and will fail unless the previous package is manually deleted.
