package com.tuplejump.calliope

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSpec}
import spark.{Partition, SparkContext}
import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.thrift.{SliceRange, SlicePredicate}
import org.slf4j.LoggerFactory

/**
 * To run this test you need a Cassandra cluter up and running
 * and update the constants to map to your setup.
 *
 */
class CassandraRDDSpec extends FunSpec with BeforeAndAfterAll {

  val CASSANDRA_NODE_COUNT = 3
  val CASSANDRA_NODE_LOCATIONS = List("127.0.0.1", "127.0.0.2", "127.0.0.3")

  info("Describes the functionality provided by the Cassandra RDD")

  val sc = new SparkContext("local", "castest")

  describe("Cassandra RDD") {
    it("should be able to get data partitions") {

      val job = new Job()


      ConfigHelper.setInputColumnFamily(job.getConfiguration, "casDemo", "Words")

      val predicate = new SlicePredicate()
      val sliceRange = new SliceRange()
      sliceRange.setStart(Array.empty[Byte])
      sliceRange.setFinish(Array.empty[Byte])
      predicate.setSlice_range(sliceRange)
      ConfigHelper.setInputSlicePredicate(job.getConfiguration, predicate)

      ConfigHelper.setInputInitialAddress(job.getConfiguration, "127.0.0.1")

      ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner")

      val casrdd = new CassandraRDD[String, String](sc, job.getConfiguration)

      val partitions: Array[Partition] = casrdd.getPartitions

      assert(partitions.length == CASSANDRA_NODE_COUNT)
    }

    it("should be able to give preferred locations for partitions") {

    }

    it("should be able to perform compute on partitions")(pending)
  }

  override def afterAll() {
    sc.stop()
  }
}
