import sbt._
import sbt.Keys._

object CalliopeBuild extends Build {

  lazy val calliope = Project(
    id = "calliope",

    base = file("."),

    settings = Project.defaultSettings ++ Seq(
      name := "calliope",
      organization := "com.tuplejump",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.3",
      libraryDependencies ++= Seq(
        "org.spark-project" % "spark-core_2.9.3" % "0.7.2" % "provided",
        "org.apache.cassandra" % "cassandra-all" % "1.2.5" % "provided",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test"
      )
      // add other settings here
    )

  )
}
