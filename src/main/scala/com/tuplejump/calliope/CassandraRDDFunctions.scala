/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
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

import spark.{Logging, RDD}
import org.apache.hadoop.mapreduce.HadoopMapReduceUtil
import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{Column, Mutation, ColumnOrSuperColumn}
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat
import scala.collection.JavaConversions._

import spark.SparkContext._
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.Predef._

import Types._

class CassandraRDDFunctions[U](self: RDD[U])
  extends Logging with HadoopMapReduceUtil with Serializable {

  private final val OUTPUT_KEYSPACE_CONFIG: String = "cassandra.output.keyspace"
  private final val OUTPUT_CQL: String = "cassandra.output.cql"

  /**
   * Save the RDD to the given keyspace and column family to a cassandra cluster accessible at localhost:9160
   *
   * @param keyspace Keyspace to save the RDD
   * @param columnFamily ColumnFamily to save the RDD
   * @param keyMarshaller The Marshaller, that takes in an RDD entry:U and gives a row key
   * @param rowMarshaller The Marshaller, that takes in an RDD entry:U and gives a map for columns
   *
   */
  def thriftSaveToCassandra(keyspace: String, columnFamily: String)
                           (implicit keyMarshaller: U => ThriftRowKey, rowMarshaller: U => ThriftRowMap) {
    thriftSaveToCassandra(CasBuilder.thrift.withColumnFamily(keyspace, columnFamily))
  }

  /**
   *
   * Save the RDD to the given keyspace and column family to a cassandra cluster accessible at host:port
   *
   * @param host Host to connect to from the SparkContext. The actual tasks will use their assigned hosts
   * @param port RPC port to use from the SparkContext.
   * @param keyspace Keyspace to save the RDD
   * @param columnFamily ColumnFamily to save the RDD
   * @param keyMarshaller The Marshaller, that takes in an RDD entry:U and gives a row key
   * @param rowMarshaller The Marshaller, that takes in an RDD entry:U and gives a map for columns
   *
   */
  def thriftSaveToCassandra(host: String, port: String, keyspace: String, columnFamily: String)
                           (implicit keyMarshaller: U => ThriftRowKey, rowMarshaller: U => ThriftRowMap) {
    thriftSaveToCassandra(CasBuilder.thrift.withColumnFamily(keyspace, columnFamily).onHost(host).onPort(port))
  }

  /**
   *
   * Save the RDD using the given configuration
   *
   * @param cas The configuration to use to connect to the cluster
   * @param keyMarshaller The Marshaller, that takes in an RDD entry:U and gives a row key
   * @param rowMarshaller The Marshaller, that takes in an RDD entry:U and gives a map for columns
   *
   * @see ThriftCasBuilder
   *
   */
  def thriftSaveToCassandra(cas: ThriftCasBuilder)
                           (implicit keyMarshaller: U => ThriftRowKey, rowMarshaller: U => ThriftRowMap) {


    val conf = cas.configuration


    self.map[(ByteBuffer, java.util.List[Mutation])] {
      x => (x, mapToMutations(x))
    }.saveAsNewAPIHadoopFile(
      conf.get(OUTPUT_KEYSPACE_CONFIG),
      classOf[ByteBuffer],
      classOf[List[Mutation]],
      classOf[ColumnFamilyOutputFormat],
      conf)

    def mapToMutations(m: Map[ByteBuffer, ByteBuffer]): List[Mutation] = {
      m.map {
        case (k, v) =>
          val column = new Column()
          column.setName(k)
          column.setValue(v)
          column.setTimestamp(System.currentTimeMillis)

          val mutation = new Mutation()
          mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column))
      }.toList
    }
  }

  /**
   *
   * Saves the RDD data to Cassandra using CQL3 update statement to the given keyspace and comlumn family to
   * a cassandra cluster accessible at localhost:9160
   *
   * @param keyspace Keyspace to save the RDD
   * @param columnFamily ColumnFamily to save the RDD
   * @param updateCql The CQL Update query to use to update the entry in cassandra. This MUST be an update query of the form,
   *                  UPDATE <keyspace>.<columnfamily> SET <field1> = ?, <field2> = ?
   *                  WHERE <primarykey1> = ? and <primarykey2> = ?
   * @param keyMarshaller A function that accepts a rdd entry:U and returns a Map of KeyName:String -> KeyValue:ByteBuffer
   * @param rowMarshaller A function that accepts a rdd entry:U and returns a List of field values:ByteBuffer
   *                      in the field order in query
   *
   */

  def cql3SaveToCassandra(keyspace: String, columnFamily: String, updateCql: String)
                         (implicit keyMarshaller: U => CQLRowKeyMap, rowMarshaller: U => CQLRowValues) {
    cql3SaveToCassandra(CasBuilder.cql3.withColumnFamily(keyspace, columnFamily).saveWithQuery(updateCql))
  }

  /**
   * Saves the RDD data to Cassandra using CQL3 update statement to the given keyspace and comlumn family to
   * a cassandra cluster accessible at host:port
   *
   * @param host Host to connect to from the SparkContext. The actual tasks will use their assigned hosts
   * @param port RPC port to use from the SparkContext.
   * @param keyspace Keyspace to save the RDD
   * @param columnFamily ColumnFamily to save the RDD
   * @param updateCql The CQL Update query to use to update the entry in cassandra. This MUST be an update query of the form,
   *                  UPDATE <keyspace>.<columnfamily> SET <field1> = ?, <field2> = ?
   *                  WHERE <primarykey1> = ? and <primarykey2> = ?
   * @param keyMarshaller A function that accepts a rdd entry:U and returns a Map of KeyName:String -> KeyValue:ByteBuffer
   * @param rowMarshaller A function that accepts a rdd entry:U and returns a List of field values:ByteBuffer
   *                      in the field order in query
   *
   */
  def cql3SaveToCassandra(host: String, port: String, keyspace: String, columnFamily: String, updateCql: String)
                         (implicit keyMarshaller: U => CQLRowKeyMap, rowMarshaller: U => CQLRowValues) {
    cql3SaveToCassandra(CasBuilder.cql3.withColumnFamily(keyspace, columnFamily)
      .onHost(host)
      .onPort(port)
      .saveWithQuery(updateCql))
  }

  /**
   *
   * @param cas The configuration to use for saving rdd to Cassandra. This should atleast configure the keyspace,
   *            columnfamily and the query to save the entry with.
   * @param keyMarshaller A function that accepts a rdd entry:U and returns a Map of KeyName:String -> KeyValue:ByteBuffer
   * @param rowMarshaller A function that accepts a rdd entry:U and returns a List of field values:ByteBuffer
   *                      in the field order in query
   *
   * @see Cql3CasBuilder
   *
   */
  def cql3SaveToCassandra(cas: Cql3CasBuilder)
                         (implicit keyMarshaller: U => CQLRowKeyMap, rowMarshaller: U => CQLRowValues) {
    val conf = cas.configuration

    require(conf.get(OUTPUT_CQL) != null && !conf.get(OUTPUT_CQL).isEmpty,
      "Query to save the records to cassandra must be set using saveWithQuery on cas")

    self.map[(java.util.Map[String, ByteBuffer], java.util.List[ByteBuffer])] {
      row => (mapAsJavaMap(keyMarshaller(row)), seqAsJavaList(rowMarshaller(row)))
    }.saveAsNewAPIHadoopFile(
      conf.get(OUTPUT_KEYSPACE_CONFIG),
      classOf[java.util.Map[String, ByteBuffer]],
      classOf[java.util.List[ByteBuffer]],
      classOf[CqlOutputFormat],
      conf
    )
  }

  def simpleSavetoCas(keyspace: String, columnFamily: String, keyCols: List[CQLKeyColumnName], valueCols: List[CQLColumnName])
                     (implicit marshaller: U => CQLRowMap) {
    import com.tuplejump.calliope.Implicits._
    val valPart = valueCols.map(_ + " = ?").mkString(",")

    val cas = CasBuilder.cql3.withColumnFamily(keyspace, columnFamily)
      .saveWithQuery("UPDATE " + keyspace + "." + columnFamily +
      " set " + valPart)

    val mappedSelf = self.map {
      row =>
        val rowMap = marshaller(row)
        val keysMap = rowMap -- valueCols
        val valuesMap = rowMap -- keyCols
        (keysMap, valuesMap)
    }

    implicit def keyMarshaller(r: (CQLRowKeyMap, CQLRowMap)) = r._1
    implicit def valueMarshaller(r: (CQLRowKeyMap, CQLRowMap)) = r._2.values.toList

    mappedSelf.cql3SaveToCassandra(cas)
  }
}
