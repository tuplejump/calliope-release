package com.tuplejump.calliope

import org.scalatest.{BeforeAndAfterAll, FunSpec}
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}
import java.nio.ByteBuffer
import com.tuplejump.calliope.utils.RichByteBuffer
import org.apache.spark.SparkContext

import Implicits._
import com.tuplejump.calliope.Types.{CQLRowMap, CQLRowKeyMap, ThriftRowMap, ThriftRowKey}
import org.apache.cassandra.thrift.CqlRow

/**
 * To run this test you need a Cassandra cluster up and running
 * and run the data-script.cli in it to create the data.
 *
 */
class Cql3CassandraRDDSpec extends FunSpec with BeforeAndAfterAll with ShouldMatchers with MustMatchers {


  import Cql3CRDDTransformers._

  val CASSANDRA_NODE_COUNT = 3
  val CASSANDRA_NODE_LOCATIONS = List("127.0.0.1", "127.0.0.2", "127.0.0.3")
  val TEST_KEYSPACE = "casSparkTest"
  val TEST_INPUT_COLUMN_FAMILY = "Words"


  info("Describes the functionality provided by the Cassandra RDD")

  val sc = new SparkContext("local[1]", "castest")

  describe("Cql3 Cassandra RDD") {
    it("should be able to build and process RDD[U]") {

      val cas = CasBuilder.cql3.withColumnFamily("cql3_test", "emp_read_test")

      val casrdd = sc.cql3Cassandra[Employee](cas)

      val result = casrdd.collect().toList

      result must have length (5)
      result should contain(Employee(20, 105, "jack", "carpenter"))
      result should contain(Employee(20, 106, "john", "grumpy"))
    }

    it("should be able to use secodary indexes") {
      val cas = CasBuilder.cql3.withColumnFamily("cql3_test", "emp_read_test").where("first_name = 'john'")

      val casrdd = sc.cql3Cassandra[Employee](cas)

      val result = casrdd.collect().toList

      result must have length (1)
      result should contain(Employee(20, 106, "john", "grumpy"))

      result should not contain (Employee(20, 105, "jack", "carpenter"))
    }


  }

  override def afterAll() {
    sc.stop()
  }
}

private object Cql3CRDDTransformers {

  import RichByteBuffer._

  implicit def row2String(key: ThriftRowKey, row: ThriftRowMap): List[String] = {
    row.keys.toList
  }

  implicit def cql3Row2Emp(keys: CQLRowKeyMap, values: CQLRowMap): Employee =
    Employee(keys.get("deptid").get, keys.get("empid").get, values.get("first_name").get, values.get("last_name").get)
}

case class Employee(deptId: Int, empId: Int, firstName: String, lastName: String)