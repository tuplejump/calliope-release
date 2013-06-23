package com.tuplejump.calliope

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import spark.SparkContext
import java.nio.ByteBuffer
import java.util.UUID
import org.apache.cassandra.utils.ByteBufferUtil
import RichByteBuffer._
import com.tuplejump.calliope.Implicits._
import CRDDFuncTransformers._

class CassandraRDDFunctionsSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {

  val TEST_KEYSPACE = "casSparkTest"
  val TEST_OUTPUT_COLUMN_FAMILY = "TheLords"

  val sc = new SparkContext("local", "castest")

  describe("Cassandra RDD Function") {
    it("should allow persistence of any RDD to cassandra") {
      val data = List(
        ("Frodo", 24, "hobbit", "shire"),
        ("Samwise", 35, "hobbit", "shire"),
        ("Gandalf", 200, "wizard", "no one knows")
      )

      val rdd = sc.parallelize(data)

      val cas = CasBuilder.thrift.withColumnFamily(TEST_KEYSPACE, TEST_OUTPUT_COLUMN_FAMILY)

      rdd.saveToCassandra(cas)

      val casrdd = sc.cassandra[String, (String, Int, String, String)](cas)

      val results = casrdd.map {
        case (k, v) => v
      }.collect()

      results.contains(("Frodo", 24, "hobbit", "shire")) must be(true)
    }
  }

  override def afterAll() {
    sc.stop()
  }
}

object CRDDFuncTransformers {

  import RichByteBuffer._

  implicit def rddToKey(x: (String, Int, String, String)): ByteBuffer = {
    UUID.nameUUIDFromBytes((x._1 + x._2 + x._3 + x._4).getBytes()).toString
  }

  implicit def lordsToColumns(x: (String, Int, String, String)): Map[ByteBuffer, ByteBuffer] = {
    Map[ByteBuffer, ByteBuffer](
      "name" -> x._1,
      "age" -> x._2,
      "tribe" -> x._3,
      "from" -> x._4
    )
  }

  implicit def columnsToLords(m: Map[ByteBuffer, ByteBuffer]): (String, Int, String, String) = {
    (m.getOrElse[ByteBuffer]("name", "NO_NAME"),
      m.getOrElse[ByteBuffer]("age", 0),
      m.getOrElse[ByteBuffer]("tribe", "NOT KNOWN"),
      m.getOrElse[ByteBuffer]("from", "a land far far away"))
  }
}
