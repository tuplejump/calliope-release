package com.tuplejump.calliope

import org.apache.hadoop.mapreduce.InputSplit
import spark.{SerializableWritable, Partition}
import org.apache.hadoop.io.Writable

case class CassandraPartition(rddId: Int, val idx: Int, @transient s: InputSplit) extends Partition {

  val inputSplit = new SerializableWritable(s.asInstanceOf[InputSplit with Writable])

  override def hashCode(): Int = (41 * (41 + rddId) + idx)

  override val index: Int = idx
}