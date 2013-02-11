package com.tuplejump.cobalt

import spark.RDD
import scala.Predef._
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.serializers.AbstractSerializer

//import me.prettyprint.cassandra.serializers.StringSerializer

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 2/10/13
 * Time: 1:03 AM
 * To change this template use File | Settings | File Templates.
 */

class CassandraRDD[T](rdd: RDD[T]) extends Serializable {

  def saveToCassandra[K, N, V](clusterName: String,
                               keyspaceName: String, columnFamily: String)
                              (rowMapper: (T => List[(K, (N, V))]))
                              (implicit keySerializer: Serializer[K]) {
    saveToCassandra[K, N, V]("localhost", "9160", clusterName, keyspaceName, columnFamily)(rowMapper)(keySerializer)
  }

  def saveToCassandra[K, N, V](host: String, port: String, clusterName: String,
                               keyspaceName: String, columnFamily: String)
                              (rowMapper: (T => List[(K, (N, V))]))
                              (implicit keySerializer: Serializer[K]) {
    val newRdd = rdd.map {
      row => rowMapper(row)
    }

    newRdd.context.runJob[List[(K, (N, V))], Unit](newRdd, {
      x: Iterator[List[(K, (N, V))]] => x.foreach(writeToCassandra _)
    })



    def writeToCassandra(rowEntries: List[(K, (N, V))]) {
      val cluster = HFactory.getOrCreateCluster(clusterName, new CassandraHostConfigurator(host + ":" + port))
      val keyspace = HFactory.createKeyspace(keyspaceName, cluster)
      val mutator = HFactory.createMutator(keyspace, keySerializer)
      rowEntries.foreach(
        col =>
          mutator.insert(col._1, columnFamily, HFactory.createColumn(col._2._1, col._2._1))
      )
    }

  }


}

object CassandraRDD {

  implicit val keySerializer = new CobaltStringSerializer()

  implicit def RDD2CassandraRDD[T](rdd: RDD[T]) = new CassandraRDD[T](rdd)
}

class CobaltStringSerializer extends AbstractSerializer[String] with Serializable {

  import scala.Predef.String
  import java.nio.charset.Charset
  import java.nio.ByteBuffer
  import me.prettyprint.hector.api.ddl.ComparatorType
  import me.prettyprint.hector.api.ddl.ComparatorType._

  private val UTF_8: String = "UTF-8"

  def toByteBuffer(obj: String): ByteBuffer = {

    val charset: Charset = Charset.forName(UTF_8)
    if (obj == null) {
      return null
    }
    return ByteBuffer.wrap(obj.getBytes(charset))
  }

  def fromByteBuffer(byteBuffer: ByteBuffer): String = {
    val charset: Charset = Charset.forName(UTF_8)
    if (byteBuffer == null) {
      return null
    }
    return charset.decode(byteBuffer).toString
  }

  override def getComparatorType: ComparatorType = {
    return UTF8TYPE
  }
}


