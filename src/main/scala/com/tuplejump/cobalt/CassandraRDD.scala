package com.tuplejump.cobalt

import spark.rdd.NewHadoopRDD
import spark.SparkContext
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat
import java.nio.ByteBuffer
import java.util.SortedMap
import org.apache.cassandra.db.IColumn
import scala.Predef._

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 2/10/13
 * Time: 1:03 AM
 * To change this template use File | Settings | File Templates.
 */
class CassandraRDD(sc: SparkContext,
                   @transient conf: Configuration)
  extends NewHadoopRDD[ByteBuffer, SortedMap[ByteBuffer, IColumn]](
    sc,
    classOf[ColumnFamilyInputFormat],
    classOf[ByteBuffer],
    classOf[SortedMap[ByteBuffer, IColumn]],
    conf) {

  def saveToCassandra(columnFamily: String) = {

  }
}
