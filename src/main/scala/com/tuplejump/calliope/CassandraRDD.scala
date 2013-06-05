package com.tuplejump.calliope

import spark._
import org.apache.hadoop.mapreduce.{JobID, HadoopMapReduceUtil, InputSplit}
import org.apache.hadoop.io.Writable
import org.apache.cassandra.hadoop.{ColumnFamilyRecordReader, ConfigHelper, ColumnFamilyInputFormat}
import org.apache.hadoop.conf.Configuration

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 6/3/13
 * Time: 1:23 AM
 * To change this template use File | Settings | File Templates.
 */
class CassandraRDD[K, V](sc: SparkContext, @transient conf: Configuration)
  extends RDD[(K, V)](sc, Nil)
  with HadoopMapReduceUtil
  with Logging {

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))
  // private val serializableConf = new SerializableWritable(conf)

  @transient val jobId = new JobID(System.currentTimeMillis().toString, id)

  def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = new Iterator[(K, V)] {
    val conf = confBroadcast.value.value
    val format = new ColumnFamilyInputFormat()
    //Set configuration

    val reader = new ColumnFamilyRecordReader

    context.addOnCompleteCallback(() => close())

    def hasNext: Boolean = false

    def next(): (K, V) = null

    private def close() {
      try {
        reader.close()
      } catch {
        case e: Exception => logWarning("Exception in RecordReader.close()", e)
      }
    }
  }

  protected[calliope] def getPartitions: Array[Partition] = {

    val jc = newJobContext(conf, jobId)
    val inputFormat = new ColumnFamilyInputFormat()
    val rawSplits = inputFormat.getSplits(jc).toArray()
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new CassandraPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[CassandraPartition].rawSplit.getLocations
  }
}


case class CassandraPartition(rddId: Int, val index: Int, @transient rawSplit: InputSplit with Writable) extends Partition