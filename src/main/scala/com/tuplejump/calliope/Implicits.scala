package com.tuplejump.calliope

import spark.{SparkContext, RDD}
import java.nio.ByteBuffer

object Implicits {
  implicit def RddToCassandraRDDFunctions[U](rdd: RDD[U]) =
    new CassandraRDDFunctions[U](rdd)

  implicit def SparkContext2CassandraAwareSparkContext(sc: SparkContext) = new CassandraAwareSparkContext(sc)
}

