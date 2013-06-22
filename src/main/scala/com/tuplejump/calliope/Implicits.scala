package com.tuplejump.calliope

import spark.RDD

object Implicits {
  implicit def rddToPairRDDFunctions[U](rdd: RDD[U]) =
    new CassandraRDDFunctions[U](rdd)
}
