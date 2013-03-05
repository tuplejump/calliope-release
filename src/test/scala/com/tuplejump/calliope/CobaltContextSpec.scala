package com.tuplejump.cobalt.calliope

import org.specs2.mutable._
import spark.SparkContext
import com.twitter.logging.Logger
import java.nio.ByteBuffer
import org.specs2.specification.AfterEach
import helpers.CasHelper

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 2/8/13
 * Time: 1:25 AM
 * To change this template use File | Settings | File Templates.
 */
class CobaltContextSpec extends Specification {
  private val logger = Logger.get(getClass)

  trait SparkTestContext extends AfterEach {
    @transient var sc: SparkContext = _

    def after: Any = {
      if (sc != null) {
        sc.stop()
        System.clearProperty("spark.driver.port")
      }
    }
  }

  step {
    CasHelper.initCassandra
  }


  "CobaltContext" should {

    "enable cassandraRDD on spark context" in new SparkTestContext {
      sc = new SparkContext("local[1]", "cobaltTest")


      SparkHelper.testcassandrRDD(sc) must not beNull

      sc must not beNull

    }

    "must be able to run an spark action on cassandra data" in new SparkTestContext {
      sc = new SparkContext("local[1]", "cobaltTest")

      SparkHelper.testSparkActionsOnCas(sc) must not beNull
    }

    "must be able to fetch correct data from cassandra" in new SparkTestContext {
      sc = new SparkContext("local[1]", "cobaltTest")

      val data = SparkHelper.testFetchData(sc)

      data(0)._1 must beEqualTo("keyOne")
      data(1)._1 must beEqualTo("keyTwo")


      val firstRow = data(0)._2
      firstRow.name must beEqualTo("John")
      firstRow.age must beEqualTo(12)
      firstRow.country must beEqualTo("USA")

      val secRow = data(1)._2
      secRow.name must beEqualTo("Jane")
      secRow.age must beEqualTo(11)
      secRow.country must beEqualTo("UK")

    }

  }

  "CobaltRDDFunctions" should {
    "enable saving to cassandra" in new SparkTestContext {
      sc = new SparkContext("local[1]", "cobaltTest")

      val data = SparkHelper.testSaveToCasandra(sc)

      data(0)._1 must beEqualTo("key001")
      data(1)._1 must beEqualTo("key002")


      val firstRow = data(0)._2
      firstRow.name must beEqualTo("Rob")
      firstRow.age must beEqualTo(20)
      firstRow.country must beEqualTo("USA")

      val secRow = data(1)._2
      secRow.name must beEqualTo("Dave")
      secRow.age must beEqualTo(19)
      secRow.country must beEqualTo("France")

    }
  }
}

case class CasData(name: String, age: Long, country: String)

object SparkHelper {

  import RichByteBuffer._
  import CobaltContext._

  implicit def map2casData(map: Map[ByteBuffer, ByteBuffer]): CasData = CasData(
    map.getOrElse[ByteBuffer]("name", "NA"),
    map.getOrElse[ByteBuffer]("age", 0L),
    map.getOrElse[ByteBuffer]("country", "NA"))

  def testcassandrRDD(sc: SparkContext) = {
    sc.cassandraRDD[String, CasData]("localhost:9160/cobaltTestKs/cocoFamilyOne")
  }

  def testSparkActionsOnCas(sc: SparkContext) = {
    val rdd = sc.cassandraRDD[String, CasData]("localhost:9160/cobaltTestKs/cocoFamilyOne")
    rdd.count()
  }

  def testFetchData(sc: SparkContext) = {
    import SparkContext._

    val rdd = sc.cassandraRDD[String, CasData]("localhost:9160/cobaltTestKs/cocoFamilyOne")

    rdd.sortByKey().collect()
  }

  def testSaveToCasandra(sc: SparkContext) = {
    import CobaltContext._
    import CobaltRDDFuntions._
    import SparkContext._

    val parList = sc.parallelize(List(
      ("key001", CasData("Rob", 20L, "USA")),
      ("key002", CasData("Dave", 19L, "France"))
    ))

    implicit def casData2Map(t: (String, CasData)): (String, Map[String, Any]) = {
      t match {
        case (k, v) => (k, Map("name" -> v.name, "age" -> v.age, "country" -> v.country))
      }
    }

    parList.saveToCassandra[String, String, Any]("Test_Cluster", "cobaltTestKs", "cocoFamilyTwo")

    sc.cassandraRDD[String, CasData]("cobaltTestKs/cocoFamilyTwo").sortByKey().collect()
  }
}