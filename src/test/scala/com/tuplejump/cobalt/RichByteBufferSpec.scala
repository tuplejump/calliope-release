package com.tuplejump.cobalt

import org.specs2.mutable._
import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 2/23/13
 * Time: 11:57 AM
 * To change this template use File | Settings | File Templates.
 */
class RichByteBufferSpec extends Specification {
  "RichByteBuffer" should {

    import RichByteBuffer._

    "implicitly enhance ByteBuffer" in {
      val b: ByteBuffer = ByteBufferUtil.bytes("Test")
      val r: RichByteBuffer = b
      r must not beNull

      r.getClass.getSimpleName must beEqualTo("RichByteBuffer")
    }

    "add implicit conversion of ByteBuffer to String" in {
      val b: ByteBuffer = ByteBufferUtil.bytes("Test")

      b.length mustEqual (4) //Should come from test
      "Test".equalsIgnoreCase(b) must beTrue
    }

    "add implicit conversion of ByteBuffer to Int" in {
      val b: ByteBuffer = ByteBufferUtil.bytes(100)

      (100 % b) must beEqualTo(0)
    }

    "add implicit conversion of ByteBuffer to Double" in {
      val b: ByteBuffer = ByteBufferUtil.bytes(100)

      (300d - b) must beEqualTo(200d)
    }

    "add implicit conversion of ByteBuffer to Long" in {
      val b: ByteBuffer = ByteBufferUtil.bytes(100)

      (300l - b) must beEqualTo(200l)
    }

    "must ease the conversion of list to case class" in {
      case class Person(name: String, age: Int)
      val l: List[ByteBuffer] = List("Joey", 10)

      def list2Person(list: List[ByteBuffer]) = Person(list(0), list(1))
      val p = list2Person(l)

      p.getClass.toString mustEqual (classOf[Person].toString)

      p.name mustEqual ("Joey")
      p.age mustEqual (10)
    }

    "must ease the conversion to typed Tuple" in {
      val l: List[ByteBuffer] = List("Joey", 10)

      def list2Tuple2(list: List[ByteBuffer]) = new Tuple2[String, Int](list(0), list(1))
      val p = list2Tuple2(l)

      p.getClass.toString mustEqual (classOf[Tuple2[String, Int]].toString)

      p._1 mustEqual ("Joey")
      p._2 mustEqual (10)
    }

  }
}



