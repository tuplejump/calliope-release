package com.tuplejump.calliope

import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import java.nio.charset.Charset

object RichByteBuffer {

  /* ByteBuffer to Typed Objects */
  implicit def ByteBuffer2Int(buffer: ByteBuffer): Int = ByteBufferUtil.toInt(buffer)

  implicit def ByteBuffer2Double(buffer: ByteBuffer): Double = ByteBufferUtil.toDouble(buffer)

  implicit def ByteBuffer2Float(buffer: ByteBuffer): Float = ByteBufferUtil.toFloat(buffer)

  implicit def ByteBuffer2Long(buffer: ByteBuffer): Long = ByteBufferUtil.toLong(buffer)

  implicit def ByteBuffer2String(buffer: ByteBuffer): String = ByteBufferUtil.string(buffer)

  //implicit def ByteBuffer2String(buffer: ByteBuffer, charset: Charset): String = ByteBufferUtil.string(buffer, charset)

  implicit def MapBB2MapSS(m: Map[ByteBuffer, ByteBuffer]) = m.map {
    case (k, v) => new Tuple2[String, String](k, v)
  }.toMap

  implicit def MapBB2MapSB(m: Map[ByteBuffer, ByteBuffer]) = m.map {
    case (k, v) => new Tuple2[String, ByteBuffer](k, v)
  }.toMap


  /* Typed objects to ByteBuffer */
  implicit def String2ByteBuffer(str: String): ByteBuffer = ByteBufferUtil.bytes(str)

  implicit def Int2ByteBuffer(i: Int): ByteBuffer = ByteBufferUtil.bytes(i)

  implicit def Double2ByteBuffer(d: Double): ByteBuffer = ByteBufferUtil.bytes(d)

  implicit def String2ByteBuffer(f: Float): ByteBuffer = ByteBufferUtil.bytes(f)

  implicit def Long2ByteBuffer(l: Long): ByteBuffer = ByteBufferUtil.bytes(l)

  implicit def TupleSS2TupleBB(t: (String, String)): (ByteBuffer, ByteBuffer) = (t._1, t._2)

  implicit def TupleSI2TupleBB(t: (String, Int)): (ByteBuffer, ByteBuffer) = (t._1, t._2)

  implicit def TupleSL2TupleBB(t: (String, Long)): (ByteBuffer, ByteBuffer) = (t._1, t._2)

  implicit def TupleSF2TupleBB(t: (String, Float)): (ByteBuffer, ByteBuffer) = (t._1, t._2)

  implicit def TupleSD2TupleBB(t: (String, Double)): (ByteBuffer, ByteBuffer) = (t._1, t._2)

}

