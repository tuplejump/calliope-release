package com.tuplejump.calliope

import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.thrift.{SliceRange, SlicePredicate}
import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.utils.ByteBufferUtil

import scala.collection.JavaConversions._
import com.tuplejump.calliope.queries.FinalQuery
import org.apache.hadoop.conf.Configuration
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper

trait CasBuilder {
  def configuration: Configuration
}

object CasBuilder {
  def cql3 = new BaseCql3CasBuilder()

  def thrift = new BaseThriftCasBuilder()
}

class BaseThriftCasBuilder {
  def withColumnFamily(keyspace: String, columnFamily: String) = new ThriftCasBuilder(keyspace, columnFamily)
}


class BaseCql3CasBuilder {
  def withColumnFamily(keyspace: String, columnFamily: String) = new Cql3CasBuilder(keyspace, columnFamily)
}

abstract class CommonCasBuilder(keyspace: String,
                                columnFamily: String,
                                hasWideRows: Boolean = false,
                                host: String = "localhost",
                                port: String = "9160",
                                partitioner: CasPartitioners.Value = CasPartitioners.Murmur3Partitioner,
                                columns: Option[List[String]] = None,
                                username: Option[String] = None,
                                password: Option[String] = None
                                 ) extends CasBuilder {
  protected def configure(job: Job) = {
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

    username map {
      case user: String => ConfigHelper.setInputKeyspaceUserName(job.getConfiguration, user)
    }

    password map {
      case pass: String => ConfigHelper.setInputKeyspacePassword(job.getConfiguration, pass)
    }

  }
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
                        ) extends CommonCasBuilder(keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password) {

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

  override def configuration = {
    val job = new Job()

    configure(job)

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

    query map {
      case q: FinalQuery => ConfigHelper.setInputRange(job.getConfiguration, q.getExpressions)
    }
    job.getConfiguration
  }

}

class Cql3CasBuilder(keyspace: String,
                     columnFamily: String,
                     host: String = "localhost",
                     port: String = "9160",
                     partitioner: CasPartitioners.Value = CasPartitioners.Murmur3Partitioner,
                     columns: Option[List[String]] = None,
                     username: Option[String] = None,
                     password: Option[String] = None,
                     pageSize: Option[Long] = None,
                     whereClause: Option[String] = None,
                     preparedSaveQuery: Option[String] = None) extends CommonCasBuilder(keyspace, columnFamily, false, host, port, partitioner, columns, username, password) {

  def onHost(h: String) = new Cql3CasBuilder(
    keyspace, columnFamily, h, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery)

  def onPort(p: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, p, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery)

  def patitionedUsing(p: CasPartitioners.Value) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, p, columns, username, password, pageSize, whereClause, preparedSaveQuery)

  def columns(c: List[String]) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, Some(c), username, password, pageSize, whereClause, preparedSaveQuery)

  def columns(c: String*) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, Some(c.toList), username, password, pageSize, whereClause, preparedSaveQuery)

  def authAs(user: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, Some(user), password, pageSize, whereClause, preparedSaveQuery)

  def withPassword(pass: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, Some(pass), pageSize, whereClause, preparedSaveQuery)

  def setPageSize(size: Long) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, Some(size), whereClause, preparedSaveQuery)

  def where(clause: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, Some(clause), preparedSaveQuery)

  def saveWithQuery(query: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, Some(query))


  override def configuration = {
    val job = new Job()

    configure(job)

    CqlConfigHelper.setInputColumns(job.getConfiguration, columns.mkString(","))
    pageSize map {
      case ps: Long => CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration, ps.toString)
    }

    whereClause map {
      case wc: String => CqlConfigHelper.setInputWhereClauses(job.getConfiguration, wc)
    }

    preparedSaveQuery map {
      case pql: String => CqlConfigHelper.setOutputCql(job.getConfiguration, pql)
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
