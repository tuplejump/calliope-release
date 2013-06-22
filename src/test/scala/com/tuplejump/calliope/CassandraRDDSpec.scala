package com.tuplejump.calliope

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSpec}
import spark.{Partition, SparkContext}
import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.thrift.{SliceRange, SlicePredicate}
import org.slf4j.LoggerFactory
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import java.nio.ByteBuffer

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
      import com.tuplejump.calliope.RichByteBuffer._


      val cas = CasHelper.thrift.useKeyspace(TEST_KEYSPACE).fromColumnFamily(TEST_INPUT_COLUMN_FAMILY)

      val casrdd = new CassandraRDD[String, Map[String, String]](sc, cas)

      val partitions: Array[Partition] = casrdd.getPartitions

      assert(partitions.length == CASSANDRA_NODE_COUNT)
    }

    it("should be able to give preferred locations for partitions") {
      import com.tuplejump.calliope.RichByteBuffer._

      val cas = CasHelper.thrift.useKeyspace(TEST_KEYSPACE).fromColumnFamily(TEST_INPUT_COLUMN_FAMILY)

      val casrdd = new CassandraRDD[String, Map[String, String]](sc, cas)

      val partitions: Array[Partition] = casrdd.getPartitions

      partitions.map {
        p => casrdd.getPreferredLocations(p) must not be (null)
      }

    }

    it("should be able to perform compute on partitions") {

      val cas = CasHelper.thrift.useKeyspace(TEST_KEYSPACE).fromColumnFamily(TEST_INPUT_COLUMN_FAMILY)

      import com.tuplejump.calliope.RichByteBuffer._
      val casrdd = new CassandraRDD[String, Map[String, String]](sc, cas)

      val result = casrdd.collect().toMap

      val resultKeys = result.keys

      resultKeys must be(Set("3musk001", "thelostworld001", "3musk003", "3musk002", "thelostworld002"))

    }
  }

  override def afterAll() {
    sc.stop()
  }
}
