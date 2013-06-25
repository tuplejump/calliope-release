package com.tuplejump.calliope

import spark.{SparkContext, RDD}
import java.nio.ByteBuffer

object Implicits {
  implicit def RddToCassandraRDDFunctions[U](rdd: RDD[U]) =
    new CassandraRDDFunctions[U](rdd)

  implicit def SparkContext2ThriftCasSparkContext(sc: SparkContext) = new ThriftCassandraSparkContext(sc)

  implicit def SparkContext2Cql3CasSparkContext(sc: SparkContext) = new Cql3CassandraSparkContext(sc)
}

