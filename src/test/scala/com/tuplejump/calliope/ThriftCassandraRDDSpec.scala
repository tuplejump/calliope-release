package com.tuplejump.calliope

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import spark.{Partition, SparkContext}
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import java.nio.ByteBuffer
import com.tuplejump.calliope.utils.RichByteBuffer
import RichByteBuffer._

import Implicits._

/**
 * To run this test you need a Cassandra cluster up and running
 * and run the data-script.cli in it to create the data.
 *
 */
class ThriftCassandraRDDSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {

  val CASSANDRA_NODE_COUNT = 3
  val CASSANDRA_NODE_LOCATIONS = List("127.0.0.1", "127.0.0.2", "127.0.0.3")
  val TEST_KEYSPACE = "casSparkTest"
  val TEST_INPUT_COLUMN_FAMILY = "Words"


  info("Describes the functionality provided by the Cassandra RDD")

  val sc = new SparkContext("local", "castest")

  describe("Thrift Cassandra RDD") {

    it("should be able to build and process RDD[K,V]") {
      val cas = CasBuilder.thrift.withColumnFamily(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)

      val casrdd = sc.thriftCassandra[String, Map[String, String]](cas)
      //This is same as calling,
      //val casrdd = sc.cassandra[String, Map[String, String]](TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)

      val result = casrdd.collect().toMap

      val resultKeys = result.keys

      resultKeys must be(Set("3musk001", "thelostworld001", "3musk003", "3musk002", "thelostworld002"))

    }
  }

  override def afterAll() {
    sc.stop()
  }
}

private object ThriftCRDDTransformers {

  import RichByteBuffer._

  implicit def row2String(key: ByteBuffer, row: Map[ByteBuffer, ByteBuffer]): List[String] = {
    row.keys.toList
  }

  implicit def cql3Row2Mapss(keys: Map[String, ByteBuffer], values: Map[String, ByteBuffer]): (Map[String, String], Map[String, String]) = {
    (keys, values)
  }
}