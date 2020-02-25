package com.github.lmdb4s

import java.util

/**
 * Byte array proxy.
 *
 * Env#create(org.lmdbjava.BufferProxy).
 */
object ByteArrayProxyLike {

  /**
   * Lexicographically compare two byte arrays.
   *
   * @param o1 left operand (required)
   * @param o2 right operand (required)
   * @return as specified by { @link Comparable} interface
   */
  //@SuppressWarnings(Array("checkstyle:ReturnCount"))
  def compareArrays(o1: Array[Byte], o2: Array[Byte]): Int = {
    require(o1 != null)
    require(o2 != null)

    if (o1 eq o2) return 0
    val minLength = Math.min(o1.length, o2.length)

    var i = 0
    while (i < minLength) {
      val lw = java.lang.Byte.toUnsignedInt(o1(i))
      val rw = java.lang.Byte.toUnsignedInt(o2(i))
      val result = Integer.compareUnsigned(lw, rw)
      if (result != 0) return result

      i += 1
    }
    o1.length - o2.length
  }
}

trait ByteArrayProxyLike[P] extends BufferProxyLike[Array[Byte],P] {

  override final protected[lmdb4s] def allocate() = new Array[Byte](0)

  override final protected[lmdb4s] def compare(o1: Array[Byte], o2: Array[Byte]): Int = ByteArrayProxyLike.compareArrays(o1, o2)

  override final protected[lmdb4s] def deallocate(buff: Array[Byte]): Unit = {
    // byte arrays cannot be allocated
    // FIXME: throw exception???
  }

  override final protected[lmdb4s] def getBytes(buffer: Array[Byte]): Array[Byte] = util.Arrays.copyOf(buffer, buffer.length)

}

