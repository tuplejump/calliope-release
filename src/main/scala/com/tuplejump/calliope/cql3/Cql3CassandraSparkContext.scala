package com.tuplejump.calliope.cql3

import spark.{RDD, SparkContext}
import java.nio.ByteBuffer
import com.tuplejump.calliope.{Cql3CasBuilder, CasBuilder}

class Cql3CassandraSparkContext(self: SparkContext) {


  /**
   *
   * Create a RDD[T] from data fetched from the mentioned Cassandra keyspace and column family accessible
   * at mentioned host and port
   *
   * @param host Host for the initial cassandra connection
   * @param port Port for the initial cassandra connection
   * @param keyspace Keyspace to read from
   * @param columnFamily Column Family to read from
   * @param unmarshaller The transformer to use
   * @tparam T The type of RDD to return
   * @return RDD[T]
   */
  def cql3Cassandra[T](host: String, port: String, keyspace: String, columnFamily: String)
                      (implicit unmarshaller: (Map[String, ByteBuffer], Map[String, ByteBuffer]) => T,
                       tm: Manifest[T]): RDD[T] = {
    val cas = CasBuilder.cql3.withColumnFamily(keyspace, columnFamily).onHost(host).onPort(port)
    this.cql3Cassandra[T](cas)
  }


  /**
   *
   * Create a RDD[T] from data fetched from the mentioned Cassandra keyspace and column family accessible
   * at localhost:9160
   *
   * @param keyspace Keyspace to read from
   * @param columnFamily Column Family to read from
   * @param unmarshaller The transformer to use
   * @tparam T The type of RDD to return
   * @return RDD[T]
   */
  def cql3Cassandra[T](keyspace: String, columnFamily: String)
                      (implicit unmarshaller: (Map[String, ByteBuffer], Map[String, ByteBuffer]) => T,
                       tm: Manifest[T]): RDD[T] = {
    val cas = CasBuilder.cql3.withColumnFamily(keyspace, columnFamily)
    this.cql3Cassandra[T](cas)
  }

  /**
   *
   * Create a RDD[K, V] from data fetched from the mentioned Cassandra keyspace and column family accessible
   * at localhost:9160
   *
   * @param keyspace Keyspace to read from
   * @param columnFamily Column Family to read from
   * @param keyUnmarshaller Transformer to get the key from the Cassandra data
   * @param rowUnmarshaller Tansformer to get the value from the Cassandra data
   * @tparam K Type of the Key
   * @tparam V Type of the Value
   * @return RDD[K, V]
   */
  def cql3Cassandra[K, V](keyspace: String, columnFamily: String)
                         (implicit keyUnmarshaller: Map[String, ByteBuffer] => K,
                          rowUnmarshaller: Map[String, ByteBuffer] => V,
                          km: Manifest[K], kv: Manifest[V]): RDD[(K, V)] = {
    val cas = CasBuilder.cql3.withColumnFamily(keyspace, columnFamily)
    this.cql3Cassandra[K, V](cas)
  }


  /**
   *
   * Create a RDD[K, V] from data fetched from the mentioned Cassandra keyspace and column family accessible
   * at mentioned host and port
   *
   * @param host Cassandra node for initial connection
   * @param port Port for the initial cassandra connection
   * @param keyspace Keyspace to read from
   * @param columnFamily Column Family to read from
   * @param keyUnmarshaller Transformer to get the key from the Cassandra data
   * @param rowUnmarshaller Tansformer to get the value from the Cassandra data
   * @tparam K Type of the Key
   * @tparam V Type of the Value
   * @return RDD[K, V]
   */
  def cql3Cassandra[K, V](host: String, port: String, keyspace: String, columnFamily: String)
                         (implicit keyUnmarshaller: Map[String, ByteBuffer] => K,
                          rowUnmarshaller: Map[String, ByteBuffer] => V,
                          km: Manifest[K], kv: Manifest[V]): RDD[(K, V)] = {
    val cas = CasBuilder.cql3.withColumnFamily(keyspace, columnFamily).onHost(host).onPort(port)
    this.cql3Cassandra[K, V](cas)
  }

  /**
   * Create a RDD[T] from data fetched from the configured  Cassandra keyspace and column family accessible
   * at configured host and port.
   *
   * @param cas The configuration to use with Cassandra
   * @param unmarshaller The transformer to use
   * @tparam T The type of RDD to return
   * @return RDD[T]
   */
  def cql3Cassandra[T](cas: Cql3CasBuilder)
                      (implicit unmarshaller: (Map[String, ByteBuffer], Map[String, ByteBuffer]) => T,
                       tm: Manifest[T]): RDD[T] = {
    new Cql3CassandraRDD[T](self, cas, unmarshaller)
  }

  /**
   * Create a RDD[K, V] from data fetched from the configured Cassandra keyspace and column family accessible
   * at configured host and port.
   *
   * @param cas The configuration to use with Cassandra
   * @param keyUnmarshaller Transformer to get the key from the Cassandra data
   * @param rowUnmarshaller Tansformer to get the value from the Cassandra data
   * @tparam K Type of the Key
   * @tparam V Type of the Value
   * @return RDD[K, V]
   */
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