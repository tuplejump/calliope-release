package com.tuplejump.cobalt

import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.hadoop.{ConfigHelper, ColumnFamilyInputFormat}
import org.apache.cassandra.thrift.{IndexExpression, SliceRange, SlicePredicate}
import query.Query
import scala.collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 3/2/13
 * Time: 1:12 PM
 * To change this template use File | Settings | File Templates.
 */

protected[cobalt] class ConfBuilder() {
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

  def setInputRange(filter: List[IndexExpression]) = {
    ConfigHelper.setInputRange(job.getConfiguration(), filter)
  }

  def getJobConf() = {
    job.getConfiguration();
  }
}
