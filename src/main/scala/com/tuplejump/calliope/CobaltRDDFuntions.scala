package com.tuplejump.cobalt.calliope

import spark.RDD
import scala.Predef._
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.serializers.AbstractSerializer

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 2/10/13
 * Time: 1:03 AM
 * To change this template use File | Settings | File Templates.
 */

class CobaltRDDFuntions[T](self: RDD[T]) extends Serializable {

  def saveToCassandra[K, X, Y](clusterName: String,
                               keyspaceName: String, columnFamily: String)
                              (implicit rowMapper: (T => (K, Map[X, Y])),
                               keySerializer: Serializer[K]) {
    saveToCassandra[K, X, Y]("localhost", "9160", clusterName, keyspaceName, columnFamily, rowMapper, keySerializer)
  }

  def saveToCassandra[K, X, Y](host: String, port: String, clusterName: String,
                               keyspaceName: String, columnFamily: String, rowMapper: (T => (K, Map[X, Y])),
                               keySerializer: Serializer[K]) {

    val newRdd = self.map {
      rowMapper(_)
    }

    newRdd.context.runJob[(K, Map[X, Y]), Unit](newRdd,
      writeToCassandra _
    )

    def writeToCassandra(i: Iterator[(K, Map[X, Y])]) {

      val cluster = HFactory.getOrCreateCluster(clusterName, new CassandraHostConfigurator(host + ":" + port))
      val keyspace = HFactory.createKeyspace(keyspaceName, cluster)
      val mutator = HFactory.createMutator(keyspace, keySerializer)

      i.foreach {
        case (rowKey, colMap) =>
          colMap.map {
            case (cname, cval) =>
              mutator.addInsertion(rowKey, columnFamily, HFactory.createColumn(cname, cval))
          }
      }

      mutator.execute()
    }

  }


}

object CobaltRDDFuntions {

  implicit val keySerializer = new CobaltStringSerializer()

  implicit def RDD2CobaltRDD[T](rdd: RDD[T]) = new CobaltRDDFuntions[T](rdd)
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
