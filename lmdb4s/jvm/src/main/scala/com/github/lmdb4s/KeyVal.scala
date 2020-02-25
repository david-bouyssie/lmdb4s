package com.github.lmdb4s

import bindings.Library

import jnr.ffi.{ Pointer => JnrPointer }

object KeyVal {
  private val MEM_MGR = Library.RUNTIME.getMemoryManager
}

/**
 * Represents off-heap memory holding a key and value pair.
 *
 * @tparam T buffer type
 */
final class KeyVal[T] private[lmdb4s](val proxy: BufferProxy[T,JnrPointer]) extends IKeyVal[T,JnrPointer] {

  import BufferProxyLike._

  require(proxy != null, "proxy is null")

  private var k: T = proxy.allocate()
  private var v: T = proxy.allocate()
  private var closed = false

  private val ptrKey: JnrPointer = KeyVal.MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false)
  private val ptrKeyAddr = ptrKey.address()
  private val ptrArray: JnrPointer = KeyVal.MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE * 2, false)
  private val ptrVal: JnrPointer = ptrArray.slice(0, MDB_VAL_STRUCT_SIZE)
  private val ptrValAddr = ptrVal.address()

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

  private[lmdb4s] def pointerKey: JnrPointer = ptrKey

  private[lmdb4s] def pointerVal: JnrPointer = ptrVal

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
  private[lmdb4s] def valInMulti(`val`: T, elements: Int): JnrPointer = {
    val ptrVal2SizeOff = MDB_VAL_STRUCT_SIZE + STRUCT_FIELD_OFFSET_SIZE
    ptrArray.putLong(ptrVal2SizeOff, elements) // ptrVal2.size

    proxy.in(`val`, ptrVal, ptrValAddr) // ptrVal1.data

    val totalBufferSize = ptrVal.getLong(STRUCT_FIELD_OFFSET_SIZE)
    val elemSize = totalBufferSize / elements
    ptrVal.putLong(STRUCT_FIELD_OFFSET_SIZE, elemSize) // ptrVal1.size

    ptrArray
  }

  private[lmdb4s] def valOut: T = {
    v = proxy.out(v, ptrVal, ptrValAddr)
    v
  }
}
