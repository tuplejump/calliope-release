package com.tuplejump.calliope

import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.thrift.{SliceRange, SlicePredicate}
import spark.Partition
import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.utils.ByteBufferUtil

import scala.collection.JavaConversions._
import com.tuplejump.calliope.queries.{FinalQuery, ThriftQuery}
import org.apache.hadoop.conf.Configuration

trait CasHelper {
  def getConfiguration: Configuration
}

object CasHelper {
  def thrift = new CasThriftHelper()
}

class CasThriftHelper(keyspace: String,
                      columnFamily: String,
                      hasWideRows: Boolean = false,
                      host: String = "localhost",
                      port: String = "9160",
                      partitioner: CasPartitioners.Value = CasPartitioners.Murmur3Partitioner,
                      columns: Option[List[String]] = None,
                      username: Option[String] = None,
                      password: Option[String] = None,
                      query: Option[FinalQuery] = None,
                      colSliceFrom: Array[Byte] = Array.empty[Byte],
                      colSliceTo: Array[Byte] = Array.empty[Byte]
                       ) extends CasHelper {

  def this() = this("", "")

  def useKeyspace(ks: String) = new CasThriftHelper(
    ks, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  def fromColumnFamily(cf: String) = new CasThriftHelper(
    keyspace, cf, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  def onHost(h: String) = new CasThriftHelper(
    keyspace, columnFamily, hasWideRows, h, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  def onPort(p: String) = new CasThriftHelper(
    keyspace, columnFamily, hasWideRows, host, p, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  def patitionedUsing(p: CasPartitioners.Value) = new CasThriftHelper(
    keyspace, columnFamily, hasWideRows, host, port, p, columns, username, password, query, colSliceFrom, colSliceTo)

  def columns(c: List[String]) = new CasThriftHelper(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, Some(c), username, password, query, colSliceFrom, colSliceTo)

  def columns(c: String*) = new CasThriftHelper(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, Some(c.toList), username, password, query, colSliceFrom, colSliceTo)

  def columnsInRange(start: Array[Byte], finish: Array[Byte]) = new CasThriftHelper(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, start, finish)

  def authAs(user: String) = new CasThriftHelper(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, Some(user), password, query, colSliceFrom, colSliceTo)

  def withPassword(pass: String) = new CasThriftHelper(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, Some(pass), query, colSliceFrom, colSliceTo)

  def where(q: FinalQuery) = new CasThriftHelper(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, Some(q), colSliceFrom, colSliceTo)

  def getConfiguration = {
    val job = new Job()

    ConfigHelper.setInputColumnFamily(job.getConfiguration, keyspace, columnFamily, hasWideRows)

    ConfigHelper.setInputInitialAddress(job.getConfiguration, host)

    ConfigHelper.setInputRpcPort(job.getConfiguration, port)

    ConfigHelper.setInputPartitioner(job.getConfiguration(), partitioner.toString)

    ConfigHelper.setOutputInitialAddress(job.getConfiguration, host)

    ConfigHelper.setOutputInitialAddress(job.getConfiguration, host)

    ConfigHelper.setOutputPartitioner(job.getConfiguration(), partitioner.toString)


    ConfigHelper.setInputColumnFamily(job.getConfiguration, keyspace, columnFamily)

    val predicate = new SlicePredicate()

    val sliceRange = new SliceRange()
    sliceRange.setStart(colSliceFrom)
    sliceRange.setFinish(colSliceTo)


    columns map {
      case colList: List[String] =>
        predicate.setColumn_names(colList.map(col => ByteBufferUtil.bytes(col)))
    }

    predicate.setSlice_range(sliceRange)
    ConfigHelper.setInputSlicePredicate(job.getConfiguration, predicate)

    username map {
      case user: String => ConfigHelper.setInputKeyspaceUserName(job.getConfiguration, user)
    }

    password map {
      case pass: String => ConfigHelper.setInputKeyspacePassword(job.getConfiguration, pass)
    }

    query map {
      case q: FinalQuery => ConfigHelper.setInputRange(job.getConfiguration, q.getExpressions)
    }
    job.getConfiguration
  }

}

object CasPartitioners extends Enumeration {
  type CasPartitioner = Value
  val Murmur3Partitioner = Value("Murmur3Partitioner")
  val RandomPartitioner = Value("RandomPartitioner")
  val ByteOrderedPartitioner = Value("ByteOrderedPartitioner")
}
