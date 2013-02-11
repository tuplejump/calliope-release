package com.tuplejump.cobalt

import spark.{RDD, SparkContext}

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.io._
import org.apache.cassandra.hadoop._
import org.apache.cassandra.thrift._
import java.nio.ByteBuffer
import java.util.SortedMap
import org.apache.cassandra.db.IColumn
import com.twitter.logging.Logger

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 2/6/13
 * Time: 10:24 AM
 * To change this template use File | Settings | File Templates.
 */
class CobaltContext(sc: SparkContext) {
  private val logger = Logger.get(getClass)

  /**
   *
   * @param host
   * @param port
   * @param keyspace
   * @param columnFamily
   */
  def cassandraRDD(host: String, port: String, keyspace: String,
                   columnFamily: String): RDD[(ByteBuffer, SortedMap[ByteBuffer, IColumn])] = {
    val conf = buildConf(host, port, keyspace, columnFamily)
      .getJobConf()

    logger.debug("Creating cassandra connection to %s:%s for keyspace - %s and column family - %s",
      host, port, keyspace, columnFamily)

    sc.newAPIHadoopRDD(conf,
      classOf[ColumnFamilyInputFormat],
      classOf[ByteBuffer],
      classOf[SortedMap[ByteBuffer, IColumn]])
  }


  private def buildConf(host: String, port: String, keyspace: String, columnFamily: String): ConfBuilder = {

    new ConfBuilder()
      .setInputInitialAddress(host)
      .setInputRpcPort(port)
      .setInputColumnFamily(keyspace, columnFamily)
      .configureDefaultInputSlicePredicate()
      .setInputPartitioner("Murmur3Partitioner")
  }

  def cassandraRDD(host: String, port: String, keyspace: String, columnFamily: String,
                   keyspaceUserName: String, keyspacePassword: String): RDD[(ByteBuffer, SortedMap[ByteBuffer, IColumn])] = {
    val conf = buildConf(host, port, keyspace, columnFamily)
      .setInputKeyspaceUserName(keyspaceUserName)
      .setInputKeyspacePassword(keyspacePassword)
      .getJobConf()

    sc.newAPIHadoopRDD(conf, classOf[ColumnFamilyInputFormat], classOf[ByteBuffer], classOf[SortedMap[ByteBuffer, IColumn]])

  }

  /**
   *
   * @param keyspace
   * @param columnFamily
   */
  def cassandraRDD(keyspace: String, columnFamily: String): RDD[(ByteBuffer, SortedMap[ByteBuffer, IColumn])] = {
    this.cassandraRDD("localhost", "9160", keyspace, columnFamily)
  }

}

object CobaltContext {
  implicit def SparkContext2CobaltContext(sc: SparkContext) = new CobaltContext(sc)
}

private class ConfBuilder() {
  val job = new Job()
  job.setInputFormatClass(classOf[ColumnFamilyInputFormat])

  def setInputInitialAddress(host: String) = {
    ConfigHelper.setInputInitialAddress(job.getConfiguration(), host)
    this
  }

  def setInputRpcPort(port: String) = {
    ConfigHelper.setInputRpcPort(job.getConfiguration(), port)
    this
  }

  def setInputColumnFamily(keyspace: String, columnFamily: String) = {
    ConfigHelper.setInputColumnFamily(job.getConfiguration(), keyspace, columnFamily)
    this
  }

  def configureDefaultInputSlicePredicate() = {
    val predicate = new SlicePredicate()
    val sliceRange = new SliceRange()
    sliceRange.setStart(Array.empty[Byte])
    sliceRange.setFinish(Array.empty[Byte])
    predicate.setSlice_range(sliceRange)
    setInputSlicePredicate(predicate)
  }

  def setInputSlicePredicate(predicate: SlicePredicate) = {
    ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate)
    this
  }

  def setInputPartitioner(partitioner: String) = {
    ConfigHelper.setInputPartitioner(job.getConfiguration(), partitioner)
    this
  }

  def setInputKeyspaceUserName(username: String) = {
    ConfigHelper.setInputKeyspaceUserName(job.getConfiguration(), username)
    this
  }

  def setInputKeyspacePassword(password: String) = {
    ConfigHelper.setInputKeyspacePassword(job.getConfiguration(), password)
    this
  }

  def getJobConf() = {
    job.getConfiguration();
  }
}

