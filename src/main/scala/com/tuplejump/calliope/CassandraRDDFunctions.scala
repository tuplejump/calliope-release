package com.tuplejump.calliope

import spark.{Logging, RDD}
import org.apache.hadoop.mapreduce.HadoopMapReduceUtil
import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{Column, Mutation, ColumnOrSuperColumn}
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat
import scala.collection.JavaConversions._

import spark.SparkContext._
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat

class CassandraRDDFunctions[U](self: RDD[U])
  extends Logging with HadoopMapReduceUtil with Serializable {

  private final val OUTPUT_KEYSPACE_CONFIG: String = "cassandra.output.keyspace"
  private final val OUTPUT_CQL: String = "cassandra.output.cql"

  def thriftSaveToCassandra(cas: ThriftCasBuilder)
                           (implicit keyMarshaller: U => ByteBuffer, rowMarshaller: U => Map[ByteBuffer, ByteBuffer],
                            um: ClassManifest[U]) {


    val conf = cas.configuration


    self.map[(ByteBuffer, java.util.List[Mutation])] {
      case x: U =>
        (x, mapToMutations(x))
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

  def cql3SaveToCassandra(cas: Cql3CasBuilder)
                         (implicit keyMarshaller: U => Map[String, ByteBuffer], rowMarshaller: U => List[ByteBuffer],
                          um: ClassManifest[U]) {
    val conf = cas.configuration

    require(conf.get(OUTPUT_CQL) != null && !conf.get(OUTPUT_CQL).isEmpty,
      "Query to save the records to cassandra must be set using saveWithQuery on cas")

    import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._
    self.map[(java.util.Map[String, ByteBuffer], java.util.List[ByteBuffer])] {
      case row: U =>
        (mapAsJavaMap(keyMarshaller(row)), seqAsJavaList(rowMarshaller(row)))
    }.saveAsNewAPIHadoopFile(
      conf.get(OUTPUT_KEYSPACE_CONFIG),
      classOf[java.util.Map[String, ByteBuffer]],
      classOf[java.util.List[ByteBuffer]],
      classOf[CqlOutputFormat],
      conf
    )
  }


}
