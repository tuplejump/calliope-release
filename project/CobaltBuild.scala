import sbt._
import sbt.Keys._

object CobaltBuild extends Build {

  lazy val cobalt = Project(

    id = "cobalt",

    base = file("."),

    settings = Project.defaultSettings ++ Seq(

      name := "cobalt",

      organization := "com.tuplejump",

      version := "0.0.2-SNAPSHOT",

      scalaVersion := "2.9.2",

      fork in test := true,

      libraryDependencies ++= Seq(
        "org.spark-project" % "spark-core_2.9.2" % "0.6.1",
        "org.apache.cassandra" % "cassandra-all" % "1.2.1",
        "org.specs2" %% "specs2" % "1.12.3" % "test",
        "com.twitter" % "util-logging" % "6.1.0",
        "org.cassandraunit" % "cassandra-unit" % "1.1.1.2" % "test",
        "org.hectorclient" % "hector-core" % "1.1-2"
      ),

      resolvers ++= Seq(
        "sonatype" at "http://oss.sonatype.org/content/repositories/releases",
        "Twitter's Repository" at "http://maven.twttr.com/"
      )
    )
  )
}
