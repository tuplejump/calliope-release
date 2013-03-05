package helpers

import java.net.ServerSocket
import java.io.{File, IOException}
import org.apache.commons.io.FileUtils
import java.util.concurrent.Executors
import org.apache.cassandra.service.CassandraDaemon
import com.twitter.util.CountDownLatch
import org.cassandraunit.DataLoader
import org.cassandraunit.dataset.json.ClassPathJsonDataSet
import com.twitter.logging.Logger

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 3/3/13
 * Time: 3:34 PM
 * To change this template use File | Settings | File Templates.
 */
object CasHelper {
  private val logger = Logger.get(getClass)

  def initCassandra {
    var portAvailable = true;

    var socket: ServerSocket = null

    try {
      socket = new ServerSocket(9160)
    } catch {
      case io: IOException =>
        portAvailable = false
    } finally {
      if (socket != null) {
        try {
          socket.close()
        } catch {
          case _ =>
          //Do nothing
        }
      }
    }

    if (portAvailable) {
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
  }
}

class CassandraRunner(cassandra: CassandraDaemon, latch: CountDownLatch) extends Runnable {
  def run() {
    cassandra.init(null)
    cassandra.start()
    latch.countDown()
  }

}
