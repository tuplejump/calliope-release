import sbt._
import sbt.Keys._

object CobaltBuild extends Build {

  lazy val cobalt = Project(
    id = "cobalt",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "cobalt",
      organization := "com.tuplejump",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.2",
      libraryDependencies ++= Seq("org.spark-project" % "spark-core_2.9.2" % "0.6.1",
        "org.apache.cassandra" % "cassandra-all" % "1.2.1")
      // add other settings here
    )
  )
}
