package com.tuplejump.calliope.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{JobContext, JobID, TaskAttemptContext, TaskAttemptID}

trait SparkHadoopMapReduceUtil {
  def newJobContext(conf: Configuration, jobId: JobID): JobContext = {
    val klass = firstAvailableClass(
      "org.apache.hadoop.mapreduce.task.JobContextImpl", // hadoop2, hadoop2-yarn
      "org.apache.hadoop.mapreduce.JobContext") // hadoop1
    val ctor = klass.getDeclaredConstructor(classOf[Configuration], classOf[JobID])
    ctor.newInstance(conf, jobId).asInstanceOf[JobContext]
  }

  def newTaskAttemptContext(conf: Configuration, attemptId: TaskAttemptID): TaskAttemptContext = {
    val klass = firstAvailableClass(
      "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl", // hadoop2, hadoop2-yarn
      "org.apache.hadoop.mapreduce.TaskAttemptContext") // hadoop1
    val ctor = klass.getDeclaredConstructor(classOf[Configuration], classOf[TaskAttemptID])
    ctor.newInstance(conf, attemptId).asInstanceOf[TaskAttemptContext]
  }

  private def firstAvailableClass(first: String, second: String): Class[_] = {
    try {
      Class.forName(first)
    } catch {
      case e: ClassNotFoundException =>
        Class.forName(second)
    }
  }

}
