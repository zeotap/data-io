name := "data-io"

organization := "com.zeotap"

scalaVersion := "2.12.14"
version := sys.env("SPARK_VERSION").asInstanceOf[String] + "_2.0.0"

import ReleaseTransformations._

val sparkVersionMap = Map(
  "3.0.1" -> Map(
    "sparkTestingBaseVersion" -> "3.0.1_1.4.7",
    "deltaName" -> "delta-core",
    "deltaVersion" -> "0.8.0",
  ),
  "3.1.2" -> Map(
    "sparkTestingBaseVersion" -> "3.1.2_1.4.7",
    "deltaName" -> "delta-core",
    "deltaVersion" -> "1.0.0",
  ),
  "3.3.1" -> Map(
    "sparkTestingBaseVersion" -> "3.3.1_1.4.7",
    "deltaName" -> "delta-core",
    "deltaVersion" -> "2.3.0",
  ),
  "3.4.1" -> Map(
    "sparkTestingBaseVersion" -> "3.4.1_1.4.7",
    "deltaName" -> "delta-core",
    "deltaVersion" -> "2.4.0",
  ),
  "3.5.0" -> Map(
    "sparkTestingBaseVersion" -> "3.5.0_1.4.7",
    "deltaName" -> "delta-spark",
    "deltaVersion" -> "3.0.0",
  ),
)

val sparkVersion = System.getenv("SPARK_VERSION")
val sparkTestingBaseVersion = sparkVersionMap(sparkVersion)("sparkTestingBaseVersion")
val deltaName = sparkVersionMap(sparkVersion)("deltaName")
val deltaVersion = sparkVersionMap(sparkVersion)("deltaVersion")
val beamVersion = "2.33.0"

libraryDependencies ++= Seq(
    "com.fasterxml.jackson.module" % "jackson-module-paranamer" % "2.12.1",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.1",
    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.21.1",
    "com.zeotap" %% "spark-property-tests" % sparkVersion,
    "io.delta" %% deltaName % deltaVersion,
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
    "org.mockito" % "mockito-core" % "2.22.0" % Test,
    "org.testcontainers" % "mysql" % "1.16.0" % Test,
    "org.testcontainers" % "postgresql" % "1.16.0" % Test
)

dependencyOverrides ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.9"
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

