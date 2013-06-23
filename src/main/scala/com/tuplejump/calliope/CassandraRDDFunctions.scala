package com.tuplejump.calliope

import spark.{Logging, RDD}
import org.apache.hadoop.mapreduce.HadoopMapReduceUtil
import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{Column, Mutation, ColumnOrSuperColumn}
import com.tuplejump.calliope.hadoop.ColumnFamilyOutputFormat
import scala.collection.JavaConversions._

import spark.SparkContext._

class CassandraRDDFunctions[U](self: RDD[U])
  extends Logging with HadoopMapReduceUtil with Serializable {

  val INPUT_KEYSPACE_CONFIG: String = "cassandra.input.keyspace"

  def saveToCassandra(cas: CasThriftBuilder)
                     (implicit keyMarshaller: U => ByteBuffer, rowMarshaller: U => Map[ByteBuffer, ByteBuffer],
                      um: ClassManifest[U]) {


    val conf = cas.configuration


    self.map[(ByteBuffer, java.util.List[Mutation])] {
      case x: U =>
        (x, mapToMutations(x))
    }.saveAsNewAPIHadoopFile(
      conf.get(INPUT_KEYSPACE_CONFIG),
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
}
