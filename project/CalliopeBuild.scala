import sbt._
import sbt.Keys._
import sbt.Classpaths.publishTask

object CalliopeBuild extends Build {

  lazy val MavenCompile = config("m2r") extend (Compile)
  lazy val publishLocalBoth = TaskKey[Unit]("publish-local", "publish local for m2 and ivy")

  lazy val cobalt = Project(

    id = "calliope",

    base = file("."),

    settings = Project.defaultSettings ++ Seq(

      name := "calliope",

      organization := "com.tuplejump.calliope",

      version := "0.0.9-SNAPSHOT",

      scalaVersion := "2.9.2",

      fork in test := true,

      libraryDependencies ++= Seq(
        "org.spark-project" % "spark-core_2.9.2" % "0.6.2" % "provided",
        "org.apache.cassandra" % "cassandra-all" % "1.2.1" % "provided",
        "org.specs2" %% "specs2" % "1.12.3" % "test",
        "com.twitter" % "util-logging" % "6.1.0",
        "org.cassandraunit" % "cassandra-unit" % "1.1.1.2" % "test",
        "org.hectorclient" % "hector-core" % "1.1-2" exclude("org.apache.cassandra", "cassandra-thrift")
      ),

      resolvers <+= sbtResolver,
      otherResolvers := Seq(Resolver.file("dotM2", file(Path.userHome + "/.m2/repository"))),
      publishLocalConfiguration in MavenCompile <<= (packagedArtifacts, deliverLocal, ivyLoggingLevel) map {
        (arts, _, level) => new PublishConfiguration(None, "dotM2", arts, Seq(), level)
      },
      publishMavenStyle in MavenCompile := true,
      publishLocal in MavenCompile <<= publishTask(publishLocalConfiguration in MavenCompile, deliverLocal),
      publishLocalBoth <<= Seq(publishLocal in MavenCompile, publishLocal).dependOn,

      resolvers ++= Seq(
        //"sonatype" at "http://oss.sonatype.org/content/repositories/releases",
        "Twitter's Repository" at "http://maven.twttr.com/"
        //"akka" at "http://repo.akka.io/releases/",
        //"spray" at "http://repo.spray.cc/"
      )
    )
  )
}
