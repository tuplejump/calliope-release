package com.tuplejump.cobalt

import org.specs2.mutable._
import spark.SparkContext
import org.apache.cassandra.service.CassandraDaemon
import java.util.concurrent.Executors
import org.apache.commons.io.FileUtils
import java.io.File
import com.twitter.util.CountDownLatch
import com.twitter.logging.Logger
import org.cassandraunit.DataLoader
import org.cassandraunit.dataset.json.ClassPathJsonDataSet

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 2/8/13
 * Time: 1:25 AM
 * To change this template use File | Settings | File Templates.
 */
class CobaltContextSpec extends Specification {
  private val logger = Logger.get(getClass)

  step {
    initCassandra
  }


  def initCassandra {
    val casDir: File = new File("target/cassandra")
    if (casDir.exists() && casDir.isDirectory()) {
      FileUtils.deleteDirectory(casDir)
    }

    val executor = Executors.newCachedThreadPool()
    val cassandra = new CassandraDaemon()

    logger.info("STARTING CASSANDRA. . .")
    val latch = new CountDownLatch(1)

    executor.execute(new CassandraRunner(cassandra, latch))
    latch.await()
    logger.info("CASSANDRA STARTED")

    val dl = new DataLoader("Test_Cluster", "localhost")
    dl.load(new ClassPathJsonDataSet("cobaltContext1.json"))
  }

  "CobaltContext" should {
    import com.tuplejump.cobalt.CobaltContext._

    "enable cassandraRDD on spark context" in {

      lazy val sc = new SparkContext("local[1]", "cobaltTest")
      val rdd = sc.cassandraRDD("localhost", "9160", "cobaltTestKs", "cocoFamilyOne")
      rdd must not beNull

    }

    "must be able to run an spark action on cassandra data" in {

      lazy val sc = new SparkContext("local[1]", "cobaltTest")
      val rdd = sc.cassandraRDD("localhost", "9160", "cobaltTestKs", "cocoFamilyOne")
      rdd must not beNull

      val res = rdd.count()
      res must beEqualTo(2)
    }
  }

  "CobaltRDDFunctions" should {
    "enable saving to cassandra" in {
      import com.tuplejump.cobalt.CobaltContext._
      import com.tuplejump.cobalt.CobaltRDDFuntions._

      lazy val sc = new SparkContext("local[1]", "cobaltTest")

      val parList = sc.parallelize(List(
        ("key001", Map("col1" -> "val1", "col2" -> "val2")),
        ("key002", Map("col1" -> "val1", "col2" -> "val2", "col3" -> "val3"))
      ))


      parList.saveToCassandra("Test_Cluster", "cobaltTestKs", "cocoFamilyTwo") {
        case (row, cols) =>
          cols.map {
            case (colName, colValue) =>
              (row, (colName, colValue))
          }.toList
      }

      sc.cassandraRDD("cobaltTestKs", "cocoFamilyTwo").count() must beEqualTo(3)

    }
  }
}

class CassandraRunner(cassandra: CassandraDaemon, latch: CountDownLatch) extends Runnable {
  def run() {
    cassandra.init(null)
    cassandra.start()
    latch.countDown()
  }

}
