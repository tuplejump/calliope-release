package com.tuplejump.cobalt

import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import java.nio.charset.Charset
import com.twitter.logging.Logger

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 2/6/13
 * Time: 11:55 AM
 * To change this template use File | Settings | File Templates.
 */


object RichByteBuffer {

  /* ByteBuffer to Typed Objects */
  implicit def ByteBuffer2Int(buffer: ByteBuffer): Int = ByteBufferUtil.toInt(buffer)

  implicit def ByteBuffer2Double(buffer: ByteBuffer): Double = ByteBufferUtil.toDouble(buffer)

  implicit def ByteBuffer2Float(buffer: ByteBuffer): Float = ByteBufferUtil.toFloat(buffer)

  implicit def ByteBuffer2Long(buffer: ByteBuffer): Long = ByteBufferUtil.toLong(buffer)

  implicit def ByteBuffer2String(buffer: ByteBuffer): String = ByteBufferUtil.string(buffer)

  implicit def ByteBuffer2String(buffer: ByteBuffer, charset: Charset): String = ByteBufferUtil.string(buffer, charset)


  /* Typed objects to ByteBuffer */
  implicit def String2ByteBuffer(str: String): ByteBuffer = ByteBufferUtil.bytes(str)

  implicit def Int2ByteBuffer(i: Int): ByteBuffer = ByteBufferUtil.bytes(i)

  implicit def Double2ByteBuffer(d: Double): ByteBuffer = ByteBufferUtil.bytes(d)

  implicit def String2ByteBuffer(f: Float): ByteBuffer = ByteBufferUtil.bytes(f)

  implicit def Long2ByteBuffer(l: Long): ByteBuffer = ByteBufferUtil.bytes(l)

}

