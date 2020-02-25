package com.github.lmdb4s

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator.DEFAULT
import java.lang.Integer.BYTES
import java.nio.ByteBuffer.allocateDirect

import org.agrona.concurrent.UnsafeBuffer


/**
 * Static constants and methods that are convenient when writing LMDB-related tests.
 */
object TestUtils extends AbstractTestUtils {

  private[lmdb4s] val shutdownHookUtil: IShutdownHookUtil = ShutdownHookUtil

  private[lmdb4s] def ba(value: Int): Array[Byte] = {
    val b = new UnsafeBuffer(new Array[Byte](4))
    b.putInt(0, value)
    b.byteArray
  }

  /*private[lmdb4s] def invokePrivateConstructor(clazz: Class[_]): Unit = {
    try {
      val c = clazz.getDeclaredConstructor()
      c.setAccessible(true)
      c.newInstance()
    } catch {
      case e@(_: NoSuchMethodException | _: InstantiationException | _: IllegalAccessException | _: IllegalArgumentException | _: InvocationTargetException) =>
        throw new LmdbException("Private construction failed", e)
    }
  }*/

  private[lmdb4s] def mdb(value: Int): UnsafeBuffer = {
    val b = new UnsafeBuffer(allocateDirect(BYTES))
    b.putInt(0, value)
    b
  }

  private[lmdb4s] def nb(value: Int): ByteBuf = {
    val b = DEFAULT.directBuffer(BYTES)
    b.writeInt(value)
    b
  }

}