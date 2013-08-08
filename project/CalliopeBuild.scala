import sbt._
import sbt.Keys._
import scala.xml.NodeSeq

object CalliopeBuild extends Build {

  val VERSION = "0.7.4"
  val SCALA_VERSION = "2.9.3"
  val SPARK_VERSION = "0.7.3"
  val CAS_VERSION = "1.2.6"
  val THRIFT_VERSION = "0.7.0"

  lazy val calliope = {
    val dependencies = libraryDependencies ++= Seq(
      "org.spark-project" %% "spark-core" % SPARK_VERSION % "provided",
      "org.spark-project" %% "spark-streaming" % SPARK_VERSION % "provided",
      "org.apache.cassandra" % "cassandra-all" % CAS_VERSION intransitive(),
      "org.apache.cassandra" % "cassandra-thrift" % CAS_VERSION intransitive(),
      "org.apache.thrift" % "libthrift" % THRIFT_VERSION exclude("org.slf4j", "slf4j-api") exclude("javax.servlet", "servlet-api"),
      "org.slf4j" % "slf4j-jdk14" % "1.7.5",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test"
    )


    val pom = { <scm>
        <url>git@github.com:tuplejump/calliope.git</url>
        <connection>scm:git:git@github.com:tuplejump/calliope.git</connection>
      </scm>
        <developers>
          <developer>
            <id>milliondreams</id>
            <name>Rohit Rai</name>
            <url>https://twitter.com/milliondreams</url>
          </developer>
        </developers> }

    val calliopeSettings = Seq(
      name := "calliope",

      organization := "com.tuplejump",

      version := VERSION,

      scalaVersion := SCALA_VERSION,

      scalacOptions := Seq("-unchecked", "-deprecation"),

      dependencies,

      parallelExecution in Test := false,

      pomExtra := pom,

      publishArtifact in Test := false,

      pomIncludeRepository := {
        _ => false
      },

      publishMavenStyle := true,

      retrieveManaged := true,

      publishTo <<= version {
        (v: String) =>
          val nexus = "https://oss.sonatype.org/"
          if (v.trim.endsWith("SNAPSHOT"))
            Some("snapshots" at nexus + "content/repositories/snapshots")
          else
            Some("releases" at nexus + "service/local/staging/deploy/maven2")
      },

      licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),

      homepage := Some(url("https://github.com/tuplejump/calliope")),

      organizationName := "Tuplejump Software Pvt. Ltd.",

      organizationHomepage := Some(url("http://www.tuplejump.com"))
    )

    Project(
      id = "calliope",
      base = file("."),
      settings = Project.defaultSettings ++ calliopeSettings
    )
  }
}
