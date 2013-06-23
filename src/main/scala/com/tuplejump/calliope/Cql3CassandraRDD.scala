package com.tuplejump.calliope

import spark._
import org.apache.hadoop.mapreduce.{TaskAttemptID, JobID, HadoopMapReduceUtil, InputSplit}
import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import java.text.SimpleDateFormat
import java.util.Date
import com.tuplejump.calliope.hadoop.cql3.CqlPagingInputFormat


class Cql3CassandraRDD[T: Manifest](sc: SparkContext,
                                    @transient cas: CasBuilder,
                                    unmarshaller: (Map[String, ByteBuffer], Map[String, ByteBuffer]) => T)
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
    val format = new CqlPagingInputFormat
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

      unmarshaller(reader.getCurrentKey.toMap, reader.getCurrentValue.toMap)
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
    val inputFormat = new CqlPagingInputFormat
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


class Cql3CassandraSparkContext(self: SparkContext) {

  def cql3Cassandra[T](keyspace: String, columnFamily: String)
                      (implicit unmarshaller: (Map[String, ByteBuffer], Map[String, ByteBuffer]) => T,
                       tm: Manifest[T]) = {
    val cas = CasBuilder.cql3.withColumnFamily(keyspace, columnFamily)
    this.cql3Cassandra[T](cas)
  }

  def cql3Cassandra[K, V](keyspace: String, columnFamily: String)
                         (implicit keyUnmarshaller: Map[String, ByteBuffer] => K,
                          rowUnmarshaller: Map[String, ByteBuffer] => V,
                          km: Manifest[K], kv: Manifest[V]) = {
    val cas = CasBuilder.cql3.withColumnFamily(keyspace, columnFamily)
    this.cql3Cassandra[K, V](cas)
  }

  def cql3Cassandra[T](cas: CasBuilder)
                      (implicit unmarshaller: (Map[String, ByteBuffer], Map[String, ByteBuffer]) => T,
                       tm: Manifest[T]) = {
    new Cql3CassandraRDD[T](self, cas, unmarshaller)
  }

  def cql3Cassandra[K, V](cas: CasBuilder)
                         (implicit keyUnmarshaller: Map[String, ByteBuffer] => K,
                          rowUnmarshaller: Map[String, ByteBuffer] => V,
                          km: Manifest[K], kv: Manifest[V]) = {

    implicit def xmer = Cql3CasHelper.kvTransformer(keyUnmarshaller, rowUnmarshaller)
    this.cql3Cassandra[(K, V)](cas)
  }


}

private object Cql3CasHelper {
  def kvTransformer[K, V](keyUnmarshaller: Map[String, ByteBuffer] => K,
                          rowUnmarshaller: Map[String, ByteBuffer] => V) = {
    {
      (k: Map[String, ByteBuffer], v: Map[String, ByteBuffer]) => {
        (keyUnmarshaller(k), rowUnmarshaller(v))
      }
    }
  }
}
