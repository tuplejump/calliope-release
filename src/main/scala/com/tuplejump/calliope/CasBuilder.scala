package com.tuplejump.calliope

import com.tuplejump.calliope.hadoop.ConfigHelper
import org.apache.cassandra.thrift.{SliceRange, SlicePredicate}
import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.utils.ByteBufferUtil

import scala.collection.JavaConversions._
import com.tuplejump.calliope.queries.FinalQuery
import org.apache.hadoop.conf.Configuration

trait CasBuilder {
  def configuration: Configuration
}

object CasBuilder {
  def cql3 = new BaseCasBuilder()

  def thrift = new BaseCasBuilder()
}

class BaseCasBuilder {
  def withColumnFamily(keyspace: String, columnFamily: String) = new ThriftCasBuilder(keyspace, columnFamily)
}

class ThriftCasBuilder(keyspace: String,
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
                        ) extends CasBuilder {

  def onHost(h: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, h, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  def onPort(p: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, p, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  def patitionedUsing(p: CasPartitioners.Value) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, p, columns, username, password, query, colSliceFrom, colSliceTo)

  def columns(c: List[String]) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, Some(c), username, password, query, colSliceFrom, colSliceTo)

  def columns(c: String*) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, Some(c.toList), username, password, query, colSliceFrom, colSliceTo)

  def forWideRows(hwr: Boolean) = new ThriftCasBuilder(
    keyspace, columnFamily, hwr, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  def columnsInRange(start: Array[Byte], finish: Array[Byte]) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, start, finish)

  def authAs(user: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, Some(user), password, query, colSliceFrom, colSliceTo)

  def withPassword(pass: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, Some(pass), query, colSliceFrom, colSliceTo)

  def where(q: FinalQuery) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, Some(q), colSliceFrom, colSliceTo)

  def configuration = {
    val job = new Job()

    //For Input

    ConfigHelper.setInputColumnFamily(job.getConfiguration, keyspace, columnFamily, hasWideRows)
    ConfigHelper.setInputInitialAddress(job.getConfiguration, host)
    ConfigHelper.setInputRpcPort(job.getConfiguration, port)
    ConfigHelper.setInputPartitioner(job.getConfiguration(), partitioner.toString)

    //For Output
    ConfigHelper.setOutputColumnFamily(job.getConfiguration, keyspace, columnFamily)
    ConfigHelper.setOutputInitialAddress(job.getConfiguration, host)
    ConfigHelper.setOutputRpcPort(job.getConfiguration, port)
    ConfigHelper.setOutputPartitioner(job.getConfiguration(), partitioner.toString)


    val predicate = new SlicePredicate()
    columns map {
      case colList: List[String] =>
        predicate.setColumn_names(colList.map(col => ByteBufferUtil.bytes(col)))
    }

    val sliceRange = new SliceRange()
    sliceRange.setStart(colSliceFrom)
    sliceRange.setFinish(colSliceTo)
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
