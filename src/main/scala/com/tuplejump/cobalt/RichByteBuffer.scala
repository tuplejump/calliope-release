package com.tuplejump.cobalt

import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil

/**
 * Created with IntelliJ IDEA.
 * User: rohit
 * Date: 2/6/13
 * Time: 11:55 AM
 * To change this template use File | Settings | File Templates.
 */
class RichByteBuffer(byteBuffer: ByteBuffer) {
  def asString() = {
    ByteBufferUtil.string(byteBuffer)
  }

  def asInt() = {
    ByteBufferUtil.toInt(byteBuffer)
  }

  def asLong() = {
    ByteBufferUtil.toLong(byteBuffer)
  }

  def asDouble() = {
    ByteBufferUtil.toDouble(byteBuffer)
  }
}

object RichByteBuffer {
  implicit def ByteBuffer2RichByteBuffer(byteBuffer: ByteBuffer) = new RichByteBuffer(byteBuffer)
}
