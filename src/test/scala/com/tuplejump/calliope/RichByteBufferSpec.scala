package com.tuplejump.calliope

import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import org.scalatest.FunSpec
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}

class RichByteBufferSpec extends FunSpec with ShouldMatchers with MustMatchers {
  describe("RichByteBuffer") {

    import RichByteBuffer._

    it("should should add implicit conversion of ByteBuffer to String") {
      val b = ByteBufferUtil.bytes("Test")

      b.length must be(4) //Should come from test
      "Test".equalsIgnoreCase(b) must be(true)
    }

    it("should add implicit conversion of ByteBuffer to Int") {
      val b: ByteBuffer = ByteBufferUtil.bytes(100)

      100 % b must be(0)
    }

    it("should add implicit conversion of ByteBuffer to Double") {
      val b: ByteBuffer = ByteBufferUtil.bytes(100)

      300d - b must be(200d)
    }

    it("should add implicit conversion of ByteBuffer to Long") {
      val b: ByteBuffer = ByteBufferUtil.bytes(100)

      300l - b must be(200l)
    }

    it("should ease the conversion of list to case class") {
      case class Person(name: String, age: Int)
      val l: List[ByteBuffer] = List("Joey", 10)

      def list2Person(list: List[ByteBuffer]) = Person(list(0), list(1)) //One line boiler plate

      val p = list2Person(l)

      p.isInstanceOf[Person] must be(true)

      p.name must be("Joey")
      p.age must be(10)
    }

    it("should ease the conversion to typed Tuple") {
      val l: List[ByteBuffer] = List("Joey", 10)

      def list2Tuple2(list: List[ByteBuffer]) = new Tuple2[String, Int](list(0), list(1)) //One line boiler plate

      val p = list2Tuple2(l)

      p.isInstanceOf[Tuple2[String, Int]] must be(true)

      p._1 must be("Joey")
      p._2 must be(10)
    }

  }
}


