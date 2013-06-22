package com.tuplejump.calliope

import spark._
import org.apache.hadoop.mapreduce.{TaskAttemptID, JobID, HadoopMapReduceUtil, InputSplit}
import org.apache.hadoop.io.Writable
import org.apache.cassandra.hadoop.{ColumnFamilyRecordReader, ConfigHelper, ColumnFamilyInputFormat}
import org.apache.hadoop.conf.Configuration
import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import com.tuplejump.calliope.RichByteBuffer._
import java.text.SimpleDateFormat
import java.util.Date


class CassandraRDD[K, V](sc: SparkContext, @transient cas: CasBuilder)
                        (implicit keyUnmarshaller: ByteBuffer => K, rowUnmarshaller: Map[ByteBuffer, ByteBuffer] => V)
  extends RDD[(K, V)](sc, Nil)
  with HadoopMapReduceUtil
  with Logging {

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  @transient private val conf = cas.configuration
  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))

  @transient val jobId = new JobID(System.currentTimeMillis().toString, id)

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = new Iterator[(K, V)] {
    val conf = confBroadcast.value.value
    val format = new ColumnFamilyInputFormat
    val split = theSplit.asInstanceOf[CassandraPartition]
    //Set configuration
    val attemptId = new TaskAttemptID(jobtrackerId, id, true, split.index, 0)
    val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)


    val reader = format.createRecordReader(
      split.inputSplit.value, hadoopAttemptContext)

    reader.initialize(split.inputSplit.value, hadoopAttemptContext)
    context.addOnCompleteCallback(() => close())

    var havePair = false
    var finished = false

    override def hasNext: Boolean = {
      if (!finished && !havePair) {
        finished = !reader.nextKeyValue
        havePair = !finished
      }
      !finished
    }

    override def next: (K, V) = {
      if (!hasNext) {
        throw new java.util.NoSuchElementException("End of stream")
      }
      havePair = false
      val rowAsMap = reader.getCurrentValue.map {
        case (name, column) => column.name() -> column.value()
      }.toMap

      return (reader.getCurrentKey, rowAsMap)
    }

    private def close() {
      try {
        reader.close()
      } catch {
        case e: Exception => logWarning("Exception in RecordReader.close()", e)
      }
    }
  }

  def getPartitions: Array[Partition] = {

    val jc = newJobContext(conf, jobId)
    val inputFormat = new ColumnFamilyInputFormat()
    val rawSplits = inputFormat.getSplits(jc).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new CassandraPartition(id, i, rawSplits(i).asInstanceOf[InputSplit])
    }
    result
  }

  override protected[calliope] def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[CassandraPartition].s.getLocations
  }
}


case class CassandraPartition(rddId: Int, val idx: Int, @transient s: InputSplit) extends Partition {

  val inputSplit = new SerializableWritable(s.asInstanceOf[InputSplit with Writable])

  override def hashCode(): Int = (41 * (41 + rddId) + idx)

  override val index: Int = idx
}
