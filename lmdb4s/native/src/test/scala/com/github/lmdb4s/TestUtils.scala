package com.github.lmdb4s

import scala.scalanative.unsafe._

/**
 * Static constants and methods that are convenient when writing LMDB-related tests.
 */
object TestUtils extends AbstractTestUtils {

  private[lmdb4s] val shutdownHookUtil: IShutdownHookUtil = ShutdownHookUtil

  private[lmdb4s] def getKVAsInt(key: Array[Byte]): Int = {
    val byteArrayPtr = key.asInstanceOf[scala.scalanative.runtime.ByteArray].at(0)
    byteArrayPtr.asInstanceOf[Ptr[Int]](0)
  }

  private[lmdb4s] def ba(value: Int): Array[Byte] = {
    val byteArray = scala.scalanative.runtime.ByteArray.alloc(4)
    val intPtr = byteArray.at(0).asInstanceOf[Ptr[Int]]
    intPtr.update(0, value)

    byteArray.asInstanceOf[Array[Byte]]
  }

  /*private[lmdb4s] def mdb(value: Int): UnsafeBuffer = {
    val b = new UnsafeBuffer(allocateDirect(BYTES))
    b.putInt(0, value)
    b
  }

  private[lmdb4s] def nb(value: Int): ByteBuf = {
    val b = DEFAULT.directBuffer(BYTES)
    b.writeInt(value)
    b
  }*/

}