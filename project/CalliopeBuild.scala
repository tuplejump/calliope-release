import sbt._
import sbt.Keys._

object CalliopeBuild extends Build {

  lazy val calliope = {
    val dependencies = libraryDependencies ++= Seq(
      "org.spark-project" % "spark-core_2.9.3" % "0.7.2" % "provided",
      "org.apache.cassandra" % "cassandra-all" % "1.2.5" intransitive(),
      "org.apache.cassandra" % "cassandra-thrift" % "1.2.5" intransitive(),
      "org.apache.thrift" % "libthrift" % "0.7.0" exclude("org.slf4j", "slf4j-api") exclude("javax.servlet", "servlet-api"),
      "org.scalatest" %% "scalatest" % "1.9.1" % "test"
    )
    Project(
      id = "calliope",

      base = file("."),

      settings = Project.defaultSettings ++ Seq(
        name := "calliope",
        organization := "com.tuplejump",
        version := "0.1-SNAPSHOT",
        scalaVersion := "2.9.3",
        dependencies,
        parallelExecution in Test := false
        // add other settings here
      )

    )
  }
}
