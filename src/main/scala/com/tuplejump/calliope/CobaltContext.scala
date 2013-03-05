package com.tuplejump.cobalt.calliope

import spark.{RDD, SparkContext}

import org.apache.cassandra.hadoop._
import org.apache.cassandra.thrift._
import java.nio.ByteBuffer
import java.util.SortedMap
import org.apache.cassandra.db.IColumn
import com.twitter.logging.Logger
import scala.collection.JavaConversions._
import com.tuplejump.cobalt.query.InitializedQuery

//import collection.mutable.Map


/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 2/6/13
 * Time: 10:24 AM
 * To change this template use File | Settings | File Templates.
 */
class CobaltContext(sc: SparkContext) {
  private val logger = Logger.get(getClass)

  /**
   * Create a RDD from an Cassandra Column family restricting the columns in it and
   * pre filter using cassandra's secondary indexes.
   *
   * @param from The path to keyspace (either host:port/ksName or just the ksName)
   *
   * @param selectColumns The columns to fetch from the rows. Use only if you are sure that restricting the columns will
   *                      benefit you enough to counter the additional deserialization and serialization in cassandra to
   *                      fetch the row, parse and select the columns. This is more true if the column family rows are
   *                      memory cached. Fetching the whole row will be much faster than restricting to columns.
   *
   * @param filter  The filter expressions to match the rows against. Suggested use is to provide a single "EQ" condition
   *                against an indexed column. Using GT, GTE, LT and LTE or multiple EQ will cause cassandra to loop over the rows to
   *                filter them, which may very well be done in spark. Use the [[com.tuplejump.cobalt.query.Query]] object to
   *                build the filter.
   *
   * @example
   * Query.where("colName").eq("value")
   *
   * @param keySerializer The function to read the key value from a ByteBuffer. Standard converters are provided in
   *                      [[RichByteBuffer]] so importing the implicits from this object should be sufficient.
   *
   * @example
   * import com.tuplejump.cobalt.RichByteBuffer._
   *
   * @param tuplizer This is the function that take a row and maps it to a you expect in return.
   *                 The row is a Map[ByteBuffer, ByteBuffer] for columnName -> columnValue. We call this the tuplizer,
   *                 as it is the recommended approach to take the response as a tuple or a case class (Product). A simple
   *                 can be created with the help of[[RichByteBuffer]]. It can be used to create the
   *                 RDDs from selected columns too.
   *
   * @example
   * //Simple list to tuple
   * implicit def list2myTuple(l:List[ByteBuffer]):Tuple3[String, Int, Int] = Tuple3[String, Int, Int](l(0), l(1), l(2))
   *
   * //List to tuple restricted to column 0, 5 and 8
   * implicit def list2myTuple(l:List[ByteBuffer]):Tuple3[String, Int, Int] = Tuple3[String, Int, Int](l(0), l(5), l(8))
   *
   * //Using case class
   * case class Sales(outletId:String, productId:String, year:Int, salesUnit: Int, salesValue:Int)
   * implicit def list2myTuple(l:List[ByteBuffer]):Sales = Sales(l(0), l(1), l(2), l(3), l(4))
   *
   * @tparam K The expected Type for Row Key
   * @tparam V The expected Type for the Row Value
   *
   * @return
   */
  def cassandraRDD[K, V](from: String, selectColumns: List[ByteBuffer], filter: InitializedQuery)
                        (implicit keySerializer: ByteBuffer => K, tuplizer: (Map[ByteBuffer, ByteBuffer]) => V): RDD[(K, V)] = {

    val (host, port, keyspace, colFamily) = extractPath(from)

    logger.debug("Creating cassandra connection to %s:%s for keyspace - %s and column family - %s",
      host, port, keyspace, colFamily)

    val builder = buildConf(host, port, keyspace, colFamily)

    if (selectColumns != null) {
      restrictColumns(builder, selectColumns)
    }

    if (filter != null) {
      setFilter(builder, filter)
    }

    makeRDD(builder, keySerializer, tuplizer)
  }

  /**
   * Create a RDD from Cassandra column family
   *
   * @param from The path to keyspace (either host:port/ksName or just the ksName)
   *
   * @param keySerializer The function to read the key value from a ByteBuffer. Standard converters are provided in
   *                      [[RichByteBuffer]] so importing the implicits from this object should be sufficient.
   *
   * @example
   * import com.tuplejump.cobalt.RichByteBuffer._
   *
   * @param tuplizer This is the function that take a row and maps it to a you expect in return.
   *                 The row is a Map[ByteBuffer, ByteBuffer] for columnName -> columnValue. We call this the tuplizer,
   *                 as it is the recommended approach to take the response as a tuple or a case class (Product). A simple
   *                 can be created with the help of[[RichByteBuffer]]. It can be used to create the
   *                 RDDs from selected columns too.
   *
   * @example
   * //Simple list to tuple
   * implicit def list2myTuple(l:List[ByteBuffer]):Tuple3[String, Int, Int] = Tuple3[String, Int, Int](l(0), l(1), l(2))
   *
   * //List to tuple restricted to column 0, 5 and 8
   * implicit def list2myTuple(l:List[ByteBuffer]):Tuple3[String, Int, Int] = Tuple3[String, Int, Int](l(0), l(5), l(8))
   *
   * //Using case class
   * case class Sales(outletId:String, productId:String, year:Int, salesUnit: Int, salesValue:Int)
   * implicit def list2myTuple(l:List[ByteBuffer]):Sales = Sales(l(0), l(1), l(2), l(3), l(4))
   *
   * @tparam K The expected Type for Row Key
   * @tparam V The expected Type for the Row Value
   *
   * @return
   *
   */
  def cassandraRDD[K, V](from: String)
                        (implicit keySerializer: ByteBuffer => K, tuplizer: (Map[ByteBuffer, ByteBuffer]) => V): RDD[(K, V)] = {
    cassandraRDD(from, null, null)
  }

  /**
   * Create a RDD from an Cassandra Column family restricting the columns.
   *
   * @param from The path to keyspace (either host:port/ksName or just the ksName)
   *
   *
   * @param selectColumns The columns to fetch from the rows. Use only if you are sure that restricting the columns will
   *                      benefit you enough to counter the additional deserialization and serialization in cassandra to
   *                      fetch the row, parse and select the columns. This is more true if the column family rows are
   *                      memory cached. Fetching the whole row will be much faster than restricting to columns.
   *
   * @param keySerializer The function to read the key value from a ByteBuffer. Standard converters are provided in
   *                      [[RichByteBuffer]] so importing the implicits from this object should be sufficient.
   *
   * @example
   * import com.tuplejump.cobalt.RichByteBuffer._
   *
   * @param tuplizer This is the function that take a row and maps it to a you expect in return.
   *                 The row is a Map[ByteBuffer, ByteBuffer] for columnName -> columnValue. We call this the tuplizer,
   *                 as it is the recommended approach to take the response as a tuple or a case class (Product). A simple
   *                 can be created with the help of[[RichByteBuffer]]. It can be used to create the
   *                 RDDs from selected columns too.
   *
   * @example
   * //Simple list to tuple
   * implicit def list2myTuple(l:List[ByteBuffer]):Tuple3[String, Int, Int] = Tuple3[String, Int, Int](l(0), l(1), l(2))
   *
   * //List to tuple restricted to column 0, 5 and 8
   * implicit def list2myTuple(l:List[ByteBuffer]):Tuple3[String, Int, Int] = Tuple3[String, Int, Int](l(0), l(5), l(8))
   *
   * //Using case class
   * case class Sales(outletId:String, productId:String, year:Int, salesUnit: Int, salesValue:Int)
   * implicit def list2myTuple(l:List[ByteBuffer]):Sales = Sales(l(0), l(1), l(2), l(3), l(4))
   *
   * @tparam K The expected Type for Row Key
   * @tparam V The expected Type for the Row Value
   *
   * @return
   */
  def cassandraRDD[K, V](from: String, selectColumns: List[ByteBuffer])
                        (implicit keySerializer: ByteBuffer => K, tuplizer: (Map[ByteBuffer, ByteBuffer]) => V): RDD[(K, V)] = {
    cassandraRDD(from, selectColumns, null)
  }

  /**
   * Create a RDD from an Cassandra Column family  pre filtered using cassandra's secondary indexes.
   *
   * @param from The path to keyspace (either host:port/ksName or just the ksName)
   *
   * @param filter  The filter expressions to match the rows against. Suggested use is to provide a single "EQ" condition
   *                against an indexed column. Using GT, GTE, LT and LTE or multiple EQ will cause cassandra to loop over the rows to
   *                filter them, which may very well be done in spark. Use the [[com.tuplejump.cobalt.query.Query]] object to
   *                build the filter.
   *
   * @example
   * Query.where("colName").eq("value")
   *
   * @param keySerializer The function to read the key value from a ByteBuffer. Standard converters are provided in
   *                      [[RichByteBuffer]] so importing the implicits from this object should be sufficient.
   *
   * @example
   * import com.tuplejump.cobalt.RichByteBuffer._
   *
   * @param tuplizer This is the function that take a row and maps it to a you expect in return.
   *                 The row is a Map[ByteBuffer, ByteBuffer] for columnName -> columnValue. We call this the tuplizer,
   *                 as it is the recommended approach to take the response as a tuple or a case class (Product). A simple
   *                 can be created with the help of[[RichByteBuffer]]. It can be used to create the
   *                 RDDs from selected columns too.
   *
   * @example
   * //Simple list to tuple
   * implicit def list2myTuple(l:List[ByteBuffer]):Tuple3[String, Int, Int] = Tuple3[String, Int, Int](l(0), l(1), l(2))
   *
   * //List to tuple restricted to column 0, 5 and 8
   * implicit def list2myTuple(l:List[ByteBuffer]):Tuple3[String, Int, Int] = Tuple3[String, Int, Int](l(0), l(5), l(8))
   *
   * //Using case class
   * case class Sales(outletId:String, productId:String, year:Int, salesUnit: Int, salesValue:Int)
   * implicit def list2myTuple(l:List[ByteBuffer]):Sales = Sales(l(0), l(1), l(2), l(3), l(4))
   *
   * @tparam K The expected Type for Row Key
   * @tparam V The expected Type for the Row Value
   *
   * @return
   */
  def cassandraRDD[K, V](from: String, filter: InitializedQuery)
                        (implicit keySerializer: ByteBuffer => K, tuplizer: (Map[ByteBuffer, ByteBuffer]) => V): RDD[(K, V)] = {
    cassandraRDD(from, null, filter)
  }

  private def makeRDD[V, K](conf: ConfBuilder, keySerializer: (ByteBuffer) => K, tuplizer: (Map[ByteBuffer, ByteBuffer]) => V): RDD[(K, V)] = {
    sc.newAPIHadoopRDD(conf.getJobConf(),
      classOf[ColumnFamilyInputFormat],
      classOf[ByteBuffer],
      classOf[SortedMap[ByteBuffer, IColumn]]).map {
      case (rowkey, row) => {
        import RichByteBuffer._
        val rowId = ByteBuffer2String(rowkey)
        val r = row.map {
          case (colName, col) => {
            (colName -> col.value())
          }
        }
        (keySerializer(rowkey), tuplizer(r.toMap))
      }
    }
  }

  private def setFilter[V, K](builder: ConfBuilder, filter: InitializedQuery) {
    import com.tuplejump.cobalt.query.FinalQuery._
    builder.setInputRange(filter.getExpressions())
  }

  private def restrictColumns[V, K](builder: ConfBuilder, selectColumns: List[ByteBuffer]) {
    val sp = new SlicePredicate()
    selectColumns.foreach {
      colName =>
        sp.addToColumn_names(colName)
    }
    builder.setInputSlicePredicate(sp)
  }

  private def extractPath[V, K](from: String) = {
    val hasHost = "(.*):(.*)/(.*)/(.*)".r
    val noHost = "(.*)/(.*)".r


    from match {
      case hasHost(h, p, k, c) => {
        (h, p, k, c)
      }
      case noHost(k, c) => {
        ("localhost", "9160", k, c)
      }
    }
  }

  private def buildConf(host: String, port: String, keyspace: String, columnFamily: String): ConfBuilder = {

    new ConfBuilder()
      .setInputInitialAddress(host)
      .setInputRpcPort(port)
      .setInputColumnFamily(keyspace, columnFamily)
      .configureDefaultInputSlicePredicate()
      .setInputPartitioner("Murmur3Partitioner")
  }

}

object CobaltContext {
  implicit def SparkContext2CobaltContext(sc: SparkContext) = new CobaltContext(sc)
}


