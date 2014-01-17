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

package com.tuplejump.calliope.examples


import org.apache.spark.streaming.{DStream, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import com.tuplejump.calliope.utils.RichByteBuffer._
import com.tuplejump.calliope.Implicits._
import com.tuplejump.calliope.CasBuilder
import java.nio.ByteBuffer
import com.tuplejump.calliope.Types.{CQLRowValues, CQLRowKeyMap}

/**
 * A dummy network stream can be run using a unix tool Netcat like this
 * $ nc -l 8443
 * Then if the cassandra server is already running locally and has cf casdemo.words
 * $ ./run com.tuplejump.calliope.examples.PersistDStream local[2] localhost 8443
 */
object PersistDStream {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: PersistDStream <master> <hostname> <port>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    val cas = CasBuilder.cql3.withColumnFamily("casdemo", "words")
      .saveWithQuery("update casdemo.words set count = ?")

    val ssc = new StreamingContext(args(0), "NetworkStreamToCassandra", Seconds(10), System.getenv("SPARK_HOME"))

    val lines = ssc.socketTextStream(args(1), args(2).toInt)
    val words = lines.flatMap(_.split(" "))

    val wordCounts: DStream[(String, Int)] = words.map(x => (x, 1)).reduceByKey(_ + _)

    // Provide row and key marshaller
    implicit def keyMarshaller(x: (String, Int)): CQLRowKeyMap = Map("word" -> x._1)
    implicit def rowMarshaller(x: (String, Int)): CQLRowValues = List(x._2)

    wordCounts.foreach(_ cql3SaveToCassandra cas)
    ssc.start()
  }
}
