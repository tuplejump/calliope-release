package com.tuplejump.calliope.thrift

import spark.{RDD, SparkContext}
import java.nio.ByteBuffer
import com.tuplejump.calliope.{ThriftCasBuilder, CasBuilder}

class ThriftCassandraSparkContext(self: SparkContext) {

  def thriftCassandra[T](host: String, port: String, keyspace: String, columnFamily: String)
                        (implicit unmarshaller: (ByteBuffer, Map[ByteBuffer, ByteBuffer]) => T,
                         tm: Manifest[T]): RDD[T] = {
    val cas = CasBuilder.thrift.withColumnFamily(keyspace, columnFamily).onHost(host).onPort(port)
    this.thriftCassandra[T](cas)
  }

  def thriftCassandra[T](keyspace: String, columnFamily: String)
                        (implicit unmarshaller: (ByteBuffer, Map[ByteBuffer, ByteBuffer]) => T,
                         tm: Manifest[T]): RDD[T] = {
    val cas = CasBuilder.thrift.withColumnFamily(keyspace, columnFamily)
    this.thriftCassandra[T](cas)
  }

  def thriftCassandra[K, V](keyspace: String, columnFamily: String)
                           (implicit keyUnmarshaller: ByteBuffer => K,
                            rowUnmarshaller: Map[ByteBuffer, ByteBuffer] => V,
                            km: Manifest[K], kv: Manifest[V]): RDD[(K, V)] = {
    val cas = CasBuilder.thrift.withColumnFamily(keyspace, columnFamily)
    this.thriftCassandra[K, V](cas)
  }

  def thriftCassandra[T](cas: ThriftCasBuilder)
                        (implicit unmarshaller: (ByteBuffer, Map[ByteBuffer, ByteBuffer]) => T,
                         tm: Manifest[T]): RDD[T] = {
    new ThriftCassandraRDD[T](self, cas, unmarshaller)
  }

  def thriftCassandra[K, V](cas: ThriftCasBuilder)
                           (implicit keyUnmarshaller: ByteBuffer => K,
                            rowUnmarshaller: Map[ByteBuffer, ByteBuffer] => V,
                            km: Manifest[K], kv: Manifest[V]): RDD[(K, V)] = {

    implicit def xmer = ThriftCasHelper.kvTransformer(keyUnmarshaller, rowUnmarshaller)
    this.thriftCassandra[(K, V)](cas)
  }
}


private object ThriftCasHelper {
  def kvTransformer[K, V](keyUnmarshaller: ByteBuffer => K,
                          rowUnmarshaller: Map[ByteBuffer, ByteBuffer] => V) = {
    {
      (k: ByteBuffer, v: Map[ByteBuffer, ByteBuffer]) => {
        (keyUnmarshaller(k), rowUnmarshaller(v))
      }
    }
  }
}