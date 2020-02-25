package com.github.lmdb4s

import java.lang.Long.reverseBytes
import java.lang.reflect.Field
import java.nio.Buffer
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocateDirect
import java.nio.ByteOrder.BIG_ENDIAN
import java.nio.ByteOrder.LITTLE_ENDIAN
import java.util.ArrayDeque
import java.util.Objects.requireNonNull

import jnr.ffi.{ Pointer => JnrPointer }

import Env.SHOULD_CHECK
import UnsafeAccess.UNSAFE


/**
 * ByteBuffer-based proxy.
 *
 * <p>
 * There are two concrete ByteBuffer proxy implementations available:
 * <ul>
 * <li>A "fast" implementation: UnsafeProxy</li>
 * <li>A "safe" implementation: ReflectiveProxy</li>
 * </ul>
 *
 * <p>
 * Users nominate which implementation they prefer by referencing the
 * #PROXY_OPTIMAL or #PROXY_SAFE field when invoking
 * Env#create(org.lmdbjava.BufferProxy).
 */
object ByteBufferProxy {

  /**
   * The fastest ByteBuffer proxy that is available on this platform.
   * This will always be the same instance as #PROXY_SAFE if the
   * UnsafeAccess#DISABLE_UNSAFE_PROP has been set to <code>true</code>
   * and/or UnsafeAccess is unavailable. Guaranteed to never be null.
   */
  val PROXY_OPTIMAL: BufferProxy[ByteBuffer, JnrPointer] = getProxyOptimal()
  /**
   * The safe, reflective ByteBuffer proxy for this system. Guaranteed
   * to never be null.
   */
  val PROXY_SAFE: BufferProxy[ByteBuffer, JnrPointer] = new ByteBufferProxy.ReflectiveProxy()

  private def getProxyOptimal(): BufferProxy[ByteBuffer, JnrPointer] = {
    try new ByteBufferProxy.UnsafeProxy()
    catch {
      case e: RuntimeException =>
        PROXY_SAFE
    }
  }

  /**
   * The buffer must be a direct buffer (not heap allocated).
   */
  @SerialVersionUID(1L)
  final class BufferMustBeDirectException() extends LmdbException("The buffer must be a direct buffer (not heap allocated") {}

  /**
   * Provides ByteBuffer pooling and address resolution for concrete
   * BufferProxy implementations.
   */
  private[lmdb4s] object AbstractByteBufferProxy {

    private[lmdb4s] val FIELD_NAME_ADDRESS = "address"
    private[lmdb4s] val FIELD_NAME_CAPACITY = "capacity"

    /**
     * A thread-safe pool for a given length. If the buffer found is valid (ie
     * not of a negative length) then that buffer is used. If no valid buffer is
     * found, a new buffer is created.
     */
    private val BUFFERS: ThreadLocal[ArrayDeque[ByteBuffer]] = new ThreadLocal[ArrayDeque[ByteBuffer]]{
      override def initialValue = new ArrayDeque[ByteBuffer](16)
    }

    /**
     * Lexicographically compare two buffers.
     *
     * @param o1 left operand (required)
     * @param o2 right operand (required)
     * @return as specified by { @link Comparable} interface
     */
    //@SuppressWarnings(Array(Array("checkstyle:ReturnCount", "PMD.CyclomaticComplexity")))
    def compareBuff(o1: ByteBuffer, o2: ByteBuffer): Int = {
      requireNonNull(o1)
      requireNonNull(o2)
      if (o1 == o2) return 0

      val minLength = Math.min(o1.limit, o2.limit)
      val minWords = minLength / java.lang.Long.BYTES
      val reverse1 = o1.order == LITTLE_ENDIAN
      val reverse2 = o1.order == LITTLE_ENDIAN

      var i = 0
      while (i < minWords * java.lang.Long.BYTES) {
        val lw = if (reverse1) reverseBytes(o1.getLong(i))
        else o1.getLong(i)
        val rw = if (reverse2) reverseBytes(o2.getLong(i))
        else o2.getLong(i)
        val diff = java.lang.Long.compareUnsigned(lw, rw)
        if (diff != 0) return diff

        i += java.lang.Long.BYTES
      }

      i = minWords * java.lang.Long.BYTES
      while (i < minLength) {
        val lw = java.lang.Byte.toUnsignedInt(o1.get(i))
        val rw = java.lang.Byte.toUnsignedInt(o2.get(i))
        val result = Integer.compareUnsigned(lw, rw)
        if (result != 0) return result

        i += 1
      }

      o1.capacity - o2.capacity
    }

    private[lmdb4s] def findField(c: Class[_], name: String): Field = {
      var clazz = c
      do try {
        val field = clazz.getDeclaredField(name)
        field.setAccessible(true)
        return field
      } catch {
        case e: NoSuchFieldException =>
          clazz = clazz.getSuperclass
      } while (clazz != null)

      throw new LmdbException(s"$name not found")
    }
  }

  abstract private[lmdb4s] class AbstractByteBufferProxy extends BufferProxy[ByteBuffer, JnrPointer] {

    //implicit def pointer2pointer(ptr: Env.LIB.AnyPointer): jnr.ffi.Pointer = ptr.asInstanceOf[jnr.ffi.Pointer]

    final protected[lmdb4s] def address(buffer: ByteBuffer): Long = {
      if (SHOULD_CHECK && !buffer.isDirect) throw new ByteBufferProxy.BufferMustBeDirectException
      buffer.asInstanceOf[sun.nio.ch.DirectBuffer].address + buffer.position
    }

    override final protected[lmdb4s] def allocate(): ByteBuffer = {
      val queue = AbstractByteBufferProxy.BUFFERS.get
      val buffer = queue.poll
      if (buffer != null && buffer.capacity >= 0) buffer
      else allocateDirect(0)
    }

    override final protected[lmdb4s] def compare(o1: ByteBuffer, o2: ByteBuffer): Int = AbstractByteBufferProxy.compareBuff(o1, o2)

    override final protected[lmdb4s] def deallocate(buff: ByteBuffer): Unit = {
      buff.order(BIG_ENDIAN)
      val queue = AbstractByteBufferProxy.BUFFERS.get
      queue.offer(buff)
    }

    override protected[lmdb4s] def getBytes(buffer: ByteBuffer): Array[Byte] = {
      val dest = new Array[Byte](buffer.limit)
      buffer.get(dest, 0, buffer.limit)
      dest
    }
  }

  /**
   * A proxy that uses Java reflection to modify byte buffer fields, and
   * official JNR-FFF methods to manipulate native pointers.
   */
  private object ReflectiveProxy {
    private val ADDRESS_FIELD = AbstractByteBufferProxy.findField(classOf[Buffer], AbstractByteBufferProxy.FIELD_NAME_ADDRESS)
    private val CAPACITY_FIELD = AbstractByteBufferProxy.findField(classOf[Buffer], AbstractByteBufferProxy.FIELD_NAME_CAPACITY)
  }

  final private class ReflectiveProxy extends AbstractByteBufferProxy {

    import BufferProxyLike._

    protected[lmdb4s] def in(buffer: ByteBuffer, ptr: JnrPointer, ptrAddr: Long): Unit = {
      ptr.putAddress(STRUCT_FIELD_OFFSET_DATA, address(buffer))
      ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, buffer.remaining)
    }

    protected[lmdb4s] def in(buffer: ByteBuffer, size: Int, ptr: JnrPointer, ptrAddr: Long): Unit = {
      ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, size)
      ptr.putAddress(STRUCT_FIELD_OFFSET_DATA, address(buffer))
    }

    protected[lmdb4s] def out(buffer: ByteBuffer, ptr: JnrPointer, ptrAddr: Long): ByteBuffer = {
      val addr = ptr.getAddress(STRUCT_FIELD_OFFSET_DATA)
      val size = ptr.getLong(STRUCT_FIELD_OFFSET_SIZE)
      try {
        ReflectiveProxy.ADDRESS_FIELD.set(buffer, addr)
        ReflectiveProxy.CAPACITY_FIELD.set(buffer, size.toInt)
      } catch {
        case e@(_: IllegalArgumentException | _: IllegalAccessException) =>
          throw new LmdbException("Cannot modify buffer", e)
      }
      buffer.clear
      buffer
    }
  }

  /**
   * A proxy that uses Java's "unsafe" class to directly manipulate byte buffer
   * fields and JNR-FFF allocated memory pointers.
   */
  private object UnsafeProxy {

    private var ADDRESS_OFFSET = 0L
    private var CAPACITY_OFFSET = 0L

    try {
      val address = AbstractByteBufferProxy.findField(classOf[Buffer], AbstractByteBufferProxy.FIELD_NAME_ADDRESS)
      val capacity = AbstractByteBufferProxy.findField(classOf[Buffer], AbstractByteBufferProxy.FIELD_NAME_CAPACITY)
      ADDRESS_OFFSET = UNSAFE.objectFieldOffset(address)
      CAPACITY_OFFSET = UNSAFE.objectFieldOffset(capacity)
    } catch {
      case e: SecurityException =>
        throw new LmdbException("Field access error", e)
    }

  }

  final private class UnsafeProxy extends AbstractByteBufferProxy {

    import BufferProxyLike._

    protected[lmdb4s] def in(buffer: ByteBuffer, ptr: JnrPointer, ptrAddr: Long): Unit = {
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, buffer.remaining)
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, address(buffer))
    }

    protected[lmdb4s] def in(buffer: ByteBuffer, size: Int, ptr: JnrPointer, ptrAddr: Long): Unit = {
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE, size)
      UNSAFE.putLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA, address(buffer))
    }

    protected[lmdb4s] def out(buffer: ByteBuffer, ptr: JnrPointer, ptrAddr: Long): ByteBuffer = {
      val addr = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_DATA)
      val size = UNSAFE.getLong(ptrAddr + STRUCT_FIELD_OFFSET_SIZE)
      UNSAFE.putLong(buffer, UnsafeProxy.ADDRESS_OFFSET, addr)
      UNSAFE.putInt(buffer, UnsafeProxy.CAPACITY_OFFSET, size.toInt)
      buffer.clear
      buffer
    }
  }

}

