name := "data-io"

organization := "com.zeotap"

scalaVersion := "2.11.12"
version := "1.2.0"

import ReleaseTransformations._

val sparkVersion = "2.4.3"
val beamVersion = "2.33.0"

libraryDependencies ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % "2.8.11",
    "com.fasterxml.jackson.core" % "jackson-annotations" % "2.8.11",
    "com.fasterxml.jackson.module" % "jackson-module-paranamer" % "2.8.11",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.11",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.11",
    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.21.1",
    "com.zeotap" %% "spark-property-tests" % "2.4.3",
    "io.delta" %% "delta-core" % "0.6.1",
    "mysql" % "mysql-connector-java" % "8.0.26",
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
    "org.apache.beam" % "beam-sdks-java-io-jdbc" % beamVersion,
    "org.apache.beam" % "beam-sdks-java-io-parquet" % beamVersion,
    "org.apache.commons" % "commons-text" % "1.6",
    "org.apache.spark" %% "spark-avro" % sparkVersion,
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.postgresql" % "postgresql" % "42.2.11",
    "org.typelevel" %% "cats-core" % "2.0.0",
    "org.typelevel" %% "cats-free" % "2.0.0",
    "org.mockito" % "mockito-core" % "2.8.9" % Test,
    "org.testcontainers" % "mysql" % "1.16.0" % Test,
    "org.testcontainers" % "postgresql" % "1.16.0" % Test
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.11",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.8.11",
  "com.fasterxml.jackson.module" % "jackson-module-paranamer" % "2.8.11",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.11",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.11"
)

fork in Test := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false

releaseTagComment    := s" Releasing ${(version in ThisBuild).value}"
releaseCommitMessage := s"[skip ci] Setting version to ${(version in ThisBuild).value}"
releaseNextCommitMessage := s"[skip ci] Setting version to ${(version in ThisBuild).value}"

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion
)

credentials += Credentials(new File(Path.userHome.absolutePath + "/.sbt/.credentials"))

publishTo := {
  val nexus = "https://zeotap.jfrog.io/zeotap/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "libs-snapshot-local")
  else
    Some("releases"  at nexus + "libs-release-local")
}

publishConfiguration := publishConfiguration.value.withOverwrite(true)

releaseTagComment    := s" Releasing ${(version in ThisBuild).value}"
releaseCommitMessage := s"[skip ci] Setting version to ${(version in ThisBuild).value}"
releaseNextCommitMessage := s"[skip ci] Setting version to ${(version in ThisBuild).value}"

