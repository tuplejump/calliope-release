package com.tuplejump.calliope

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import spark.SparkContext
import java.nio.ByteBuffer
import java.util.UUID

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

      import com.tuplejump.calliope.Implicits._
      import Transformers._
      val cas = CasHelper.thrift.useKeyspace(TEST_KEYSPACE).fromColumnFamily(TEST_OUTPUT_COLUMN_FAMILY)

      rdd.saveToCassandra(cas)



    }
  }

  override def afterAll() {
    sc.stop()
  }
}

object Transformers {

  import RichByteBuffer._

  implicit def rddToKey(x: (String, Int, String, String)): ByteBuffer = {
    UUID.nameUUIDFromBytes((x._1 + x._2 + x._3 + x._4).getBytes()).toString
  }

  implicit def rddToColumns(x: (String, Int, String, String)): Map[ByteBuffer, ByteBuffer] = {
    Map[ByteBuffer, ByteBuffer](
      "name" -> x._1,
      "age" -> x._2,
      "tribe" -> x._3,
      "from" -> x._4
    )
  }
}
