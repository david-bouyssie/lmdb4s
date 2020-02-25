package com.github.lmdb4s

import scala.scalanative.libc.stdlib.malloc
import scala.scalanative.unsafe._

/**
 * Represents off-heap memory holding a key and value pair.
 *
 */
final class KeyVal[T] private[lmdb4s](val proxy: BufferProxy[T,Ptr[Byte]]) extends IKeyVal[T,Ptr[Byte]] {

  import BufferProxyLike._

  require(proxy != null, "proxy is null")

  private var k: T = proxy.allocate()
  private var v: T = proxy.allocate()
  private var closed = false

  private val ptrKey: Ptr[Byte] = malloc(MDB_VAL_STRUCT_SIZE)
  private val ptrKeyAddr = 0L // FIXME: is there a way to get the memory address as a Long value?
  private val ptrArray: Ptr[Byte] = malloc(MDB_VAL_STRUCT_SIZE * 2)
  private val ptrArrayAsLongPtr: Ptr[Long] = ptrArray.asInstanceOf[Ptr[Long]]
  private val ptrVal: Ptr[Byte] = ptrArray //.slice(0, MDB_VAL_STRUCT_SIZE)
  private val ptrValAsLongPtr: Ptr[Long] = ptrVal.asInstanceOf[Ptr[Long]]
  private val ptrValAddr = 0L // FIXME: is there a way to get the memory address as a Long value?

  override def close(): Unit = {
    if (closed) return
    closed = true
    proxy.deallocate(k)
    proxy.deallocate(v)
  }

  private[lmdb4s] def key: T = k

  private[lmdb4s] def getKeyAsBytes(): Array[Byte] = proxy.getBytes(key)

  private[lmdb4s] def keyIn(key: T): Unit = {
    proxy.in(key, ptrKey, ptrKeyAddr)
  }

  private[lmdb4s] def keyOut: T = {
    k = proxy.out(k, ptrKey, ptrKeyAddr)
    k
  }

  private[lmdb4s] def pointerKey: Ptr[Byte] = ptrKey

  private[lmdb4s] def pointerVal: Ptr[Byte] = ptrVal

  private[lmdb4s] def `val`: T = v

  private[lmdb4s] def valIn(`val`: T): Unit = {
    proxy.in(`val`, ptrVal, ptrValAddr)
  }

  private[lmdb4s] def valIn(size: Int): Unit = {
    proxy.in(v, size, ptrVal, ptrValAddr)
  }

  /**
   * Prepares an array suitable for presentation as the data argument to a
   * <code>MDB_MULTIPLE</code> put.
   *
   * <p>
   * The returned array is equivalent of two <code>MDB_val</code>s as follows:
   *
   * <ul>
   * <li>ptrVal1.data = pointer to the data address of passed buffer</li>
   * <li>ptrVal1.size = size of each individual data element</li>
   * <li>ptrVal2.data = unused</li>
   * <li>ptrVal2.size = number of data elements (as passed to this method)</li>
   * </ul>
   *
   * @param val      a user-provided buffer with data elements (required)
   * @param elements number of data elements the user has provided
   * @return a properly-prepared pointer to an array for the operation
   */
  private[lmdb4s] def valInMulti(`val`: T, elements: Int): Ptr[Byte] = {
    val ptrVal2SizeOff = MDB_VAL_STRUCT_SIZE + STRUCT_FIELD_OFFSET_SIZE
    ptrArrayAsLongPtr.update(ptrVal2SizeOff, elements) // ptrVal2.size

    proxy.in(`val`, ptrVal, ptrValAddr) // ptrVal1.data

    val totalBufferSize = ptrValAsLongPtr(STRUCT_FIELD_OFFSET_SIZE)
    val elemSize = totalBufferSize / elements
    ptrValAsLongPtr.update(STRUCT_FIELD_OFFSET_SIZE, elemSize) // ptrVal1.size

    ptrArray
  }

  private[lmdb4s] def valOut: T = {
    v = proxy.out(v, ptrVal, ptrValAddr)
    v
  }
}
