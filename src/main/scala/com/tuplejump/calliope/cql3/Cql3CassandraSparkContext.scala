package com.tuplejump.calliope.cql3

import spark.{RDD, SparkContext}
import java.nio.ByteBuffer
import com.tuplejump.calliope.{Cql3CasBuilder, CasBuilder}

class Cql3CassandraSparkContext(self: SparkContext) {

  def cql3Cassandra[T](keyspace: String, columnFamily: String)
                      (implicit unmarshaller: (Map[String, ByteBuffer], Map[String, ByteBuffer]) => T,
                       tm: Manifest[T]): RDD[T] = {
    val cas = CasBuilder.cql3.withColumnFamily(keyspace, columnFamily)
    this.cql3Cassandra[T](cas)
  }

  def cql3Cassandra[K, V](keyspace: String, columnFamily: String)
                         (implicit keyUnmarshaller: Map[String, ByteBuffer] => K,
                          rowUnmarshaller: Map[String, ByteBuffer] => V,
                          km: Manifest[K], kv: Manifest[V]): RDD[(K, V)] = {
    val cas = CasBuilder.cql3.withColumnFamily(keyspace, columnFamily)
    this.cql3Cassandra[K, V](cas)
  }

  def cql3Cassandra[T](cas: Cql3CasBuilder)
                      (implicit unmarshaller: (Map[String, ByteBuffer], Map[String, ByteBuffer]) => T,
                       tm: Manifest[T]): RDD[T] = {
    new Cql3CassandraRDD[T](self, cas, unmarshaller)
  }

  def cql3Cassandra[K, V](cas: Cql3CasBuilder)
                         (implicit keyUnmarshaller: Map[String, ByteBuffer] => K,
                          rowUnmarshaller: Map[String, ByteBuffer] => V,
                          km: Manifest[K], kv: Manifest[V]): RDD[(K, V)] = {

    implicit def xmer = Cql3CasHelper.kvTransformer(keyUnmarshaller, rowUnmarshaller)
    this.cql3Cassandra[(K, V)](cas)
  }
}

private object Cql3CasHelper {
  def kvTransformer[K, V](keyUnmarshaller: Map[String, ByteBuffer] => K,
                          rowUnmarshaller: Map[String, ByteBuffer] => V) = {
    {
      (k: Map[String, ByteBuffer], v: Map[String, ByteBuffer]) => {
        (keyUnmarshaller(k), rowUnmarshaller(v))
      }
    }
  }
}