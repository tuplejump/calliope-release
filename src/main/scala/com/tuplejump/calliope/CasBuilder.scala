/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
  /**
   * Get a CQL3 based configuration builder
   * @return BaseCql3CasBuilder
   */
  def cql3 = new BaseCql3CasBuilder()

  /**
   * Get a Thrift based configuration builder
   * @return BaseThriftCasBuilder
   */
  def thrift = new BaseThriftCasBuilder()
}

class BaseThriftCasBuilder {
  /**
   * Configure the cassandra keyspace and column family to read from
   * @param keyspace Keyspace name
   * @param columnFamily Column family name
   * @return ThrifCasBuilder
   */
  def withColumnFamily(keyspace: String, columnFamily: String) = new ThriftCasBuilder(keyspace, columnFamily)
}


class BaseCql3CasBuilder {
  /**
   * Configure the cassandra keyspace and column family to read from
   * @param keyspace Keyspace name
   * @param columnFamily Column family name
   * @return Cql3CasBuilder
   */
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

  /**
   * Configure the Cassandra node to use for initial connection. This must be accessible from Spark Context.
   * @param host
   */
  def onHost(host: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  /**
   * Configure the port to use for initial connection
   * @param port
   */
  def onPort(port: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  /**
   * The partitioner to use, Random/Ordered/Murmur3
   * @param partitioner
   * @return
   */
  def patitionedUsing(partitioner: CasPartitioners.Value) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  /**
   * Columns (as List[String]) to read from Cassandra
   * @param columns
   */
  def columns(columns: List[String]) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, Some(columns), username, password, query, colSliceFrom, colSliceTo)

  /**
   * Columns to read from Cassandra
   * @param columns
   */
  def columns(columns: String*) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, Some(columns.toList), username, password, query, colSliceFrom, colSliceTo)

  /**
   * Whether the column family is wide row.
   * @param hasWideRows
   */
  def forWideRows(hasWideRows: Boolean) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, colSliceFrom, colSliceTo)

  /**
   * Range of columns to fetch
   * @param start
   * @param finish
   */
  def columnsInRange(start: Array[Byte], finish: Array[Byte]) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, query, start, finish)

  /**
   * User to login to cassandra cluster
   * @param user
   */
  def authAs(user: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, Some(user), password, query, colSliceFrom, colSliceTo)

  /**
   * Password for user to authenticate with cassandra
   * @param password
   */
  def withPassword(password: String) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, Some(password), query, colSliceFrom, colSliceTo)

  /**
   * Query to filter using secondary indexes
   * @param query
   */
  def where(query: FinalQuery) = new ThriftCasBuilder(
    keyspace, columnFamily, hasWideRows, host, port, partitioner, columns, username, password, Some(query), colSliceFrom, colSliceTo)

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

  /**
   * Cassandra node for initial connection. Must be reachable from Spark Context.
   * @param host
   */
  def onHost(host: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery)

  /**
   * Port to use for initial cassandra connection
   * @param port
   */
  def onPort(port: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery)

  /**
   * The partitioner being used by this column family
   * @param partitioner
   */
  def patitionedUsing(partitioner: CasPartitioners.Value) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, whereClause, preparedSaveQuery)

  /**
   * List of columns to be read
   * @param columns
   */
  def columns(columns: List[String]) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, Some(columns), username, password, pageSize, whereClause, preparedSaveQuery)

  /**
   * The columns to be read from Cassandra
   * @param columns
   */
  def columns(columns: String*) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, Some(columns.toList), username, password, pageSize, whereClause, preparedSaveQuery)

  /**
   * User to authenticate with to Cassandra
   * @param user
   */
  def authAs(user: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, Some(user), password, pageSize, whereClause, preparedSaveQuery)

  /**
   * Password to use for authenticating the user with cassandra
   * @param pass
   */
  def withPassword(pass: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, Some(pass), pageSize, whereClause, preparedSaveQuery)

  /**
   * The number of rows to be fetched from cassandra in a single iterator. This should be  as large as possible but not larger.
   * @param size
   */
  def setPageSize(size: Long) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, Some(size), whereClause, preparedSaveQuery)

  /**
   * The CQL3 where predicate to use for filtering on secondary indexes in cassandra
   * @param clause
   */
  def where(clause: String) = new Cql3CasBuilder(
    keyspace, columnFamily, host, port, partitioner, columns, username, password, pageSize, Some(clause), preparedSaveQuery)

  /**
   * The CQL3 Update query to be used while persisting data to Cassandra
   * @param query
   */
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
