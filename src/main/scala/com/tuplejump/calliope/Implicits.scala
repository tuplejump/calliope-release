package com.tuplejump.calliope

import spark.{SparkContext, RDD}
import java.nio.ByteBuffer
import com.tuplejump.calliope.thrift.ThriftCassandraSparkContext
import com.tuplejump.calliope.cql3.Cql3CassandraSparkContext

object Implicits {
  implicit def RddToCassandraRDDFunctions[U](rdd: RDD[U]) =
    new CassandraRDDFunctions[U](rdd)

  implicit def SparkContext2ThriftCasSparkContext(sc: SparkContext) = new ThriftCassandraSparkContext(sc)

  implicit def SparkContext2Cql3CasSparkContext(sc: SparkContext) = new Cql3CassandraSparkContext(sc)
}

