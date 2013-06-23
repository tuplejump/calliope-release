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


class CassandraRDD[T: Manifest](sc: SparkContext,
                                @transient cas: CasBuilder,
                                marshaller: (ByteBuffer, Map[ByteBuffer, ByteBuffer]) => T)
  extends RDD[T](sc, Nil)
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

  def compute(theSplit: Partition, context: TaskContext): Iterator[T] = new Iterator[T] {
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

    override def next: T = {
      if (!hasNext) {
        throw new java.util.NoSuchElementException("End of stream")
      }
      havePair = false
      val rowAsMap = reader.getCurrentValue.map {
        case (name, column) => column.name() -> column.value()
      }.toMap

      return marshaller(reader.getCurrentKey, rowAsMap)
      //return (keyUnmarshaller(reader.getCurrentKey), rowUnmarshaller(rowAsMap))
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


class CassandraAwareSparkContext(self: SparkContext) {

  def cassandra[T](keyspace: String, columnFamily: String)
                  (implicit unmarshaller: (ByteBuffer, Map[ByteBuffer, ByteBuffer]) => T,
                   tm: Manifest[T]) = {
    val cas = CasBuilder.thrift.withColumnFamily(keyspace, columnFamily)
    this.cassandra[T](cas)
  }

  def cassandra[K, V](keyspace: String, columnFamily: String)
                     (implicit keyUnmarshaller: ByteBuffer => K,
                      rowUnmarshaller: Map[ByteBuffer, ByteBuffer] => V,
                      km: Manifest[K], kv: Manifest[V]) = {
    val cas = CasBuilder.thrift.withColumnFamily(keyspace, columnFamily)
    this.cassandra[K, V](cas)
  }

  def cassandra[T](cas: CasBuilder)
                  (implicit unmarshaller: (ByteBuffer, Map[ByteBuffer, ByteBuffer]) => T,
                   tm: Manifest[T]) = {
    new CassandraRDD[T](self, cas, unmarshaller)
  }

  def cassandra[K, V](cas: CasBuilder)
                     (implicit keyUnmarshaller: ByteBuffer => K,
                      rowUnmarshaller: Map[ByteBuffer, ByteBuffer] => V,
                      km: Manifest[K], kv: Manifest[V]) = {

    implicit def xmer = CasHelper.kvTransformer(keyUnmarshaller, rowUnmarshaller)
    this.cassandra[(K, V)](cas)
  }


}

object CasHelper {
  def kvTransformer[K, V](keyUnmarshaller: ByteBuffer => K,
                          rowUnmarshaller: Map[ByteBuffer, ByteBuffer] => V) = {
    {
      (k: ByteBuffer, v: Map[ByteBuffer, ByteBuffer]) => {
        (keyUnmarshaller(k), rowUnmarshaller(v))
      }
    }
  }
}
