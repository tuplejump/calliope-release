package com.tuplejump.calliope

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSpec}
import spark.{Partition, SparkContext}
import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.thrift.{SliceRange, SlicePredicate}
import org.slf4j.LoggerFactory
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import java.nio.ByteBuffer
import com.tuplejump.calliope.RichByteBuffer._

import Implicits._

/**
 * To run this test you need a Cassandra cluster up and running
 * and run the data-script.cli in it to create the data.
 *
 */
class CassandraRDDSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {

  val CASSANDRA_NODE_COUNT = 3
  val CASSANDRA_NODE_LOCATIONS = List("127.0.0.1", "127.0.0.2", "127.0.0.3")
  val TEST_KEYSPACE = "casSparkTest"
  val TEST_INPUT_COLUMN_FAMILY = "Words"


  info("Describes the functionality provided by the Cassandra RDD")

  val sc = new SparkContext("local", "castest")

  describe("Cassandra RDD") {

    it("should be able to get data partitions") {
      val cas = CasBuilder.thrift.withColumnFamily(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)
      val casrdd = sc.cassandra[String, Map[String, String]](cas)

      val partitions: Array[Partition] = casrdd.getPartitions

      assert(partitions.length == CASSANDRA_NODE_COUNT)
    }

    it("should be able to give preferred locations for partitions") {
      val cas = CasBuilder.thrift.withColumnFamily(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)

      val casrdd = sc.cassandra[String, Map[String, String]](cas)

      val partitions: Array[Partition] = casrdd.getPartitions

      partitions.map {
        p => casrdd.getPreferredLocations(p) must not be (null)
      }

    }

    it("should be able to build and process RDD[K,V]") {
      val cas = CasBuilder.thrift.withColumnFamily(TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)

      val casrdd = sc.cassandra[String, Map[String, String]](cas)
      //This is same as calling,
      //val casrdd = sc.cassandra[String, Map[String, String]](TEST_KEYSPACE, TEST_INPUT_COLUMN_FAMILY)

      val result = casrdd.collect().toMap

      val resultKeys = result.keys

      resultKeys must be(Set("3musk001", "thelostworld001", "3musk003", "3musk002", "thelostworld002"))

    }

    //TODO: Add wide row support test case
    /* it("should be able to build and process RDD[U]") {
      val cas = CasBuilder.thrift.withColumnFamily("caswidetest", "clicks")

      import CRDDTransformers._
      //val casrdd = new CassandraRDD[String, Map[String, String]](sc, cas)
      //val newcas = CasBuilder.thrift.withColumnFamily("rohit_tuplejumpcom_newapp002", "endpoint0001").forWideRows(true)
      val casrdd = sc.cassandra[List[String]](cas)

      val result = casrdd.collect().toList

      println(result)
    }  */

  }

  override def afterAll() {
    sc.stop()
  }
}

object CRDDTransformers {

  import RichByteBuffer._

  implicit def row2String(key: ByteBuffer, row: Map[ByteBuffer, ByteBuffer]): List[String] = {
    row.keys.toList
  }
}