package com.github.lmdb4s

import jnr.ffi.{ Pointer => JnrPointer }

import bindings.Library.RUNTIME

/**
 * Byte array proxy.
 *
 * Env#create(org.lmdbjava.BufferProxy).
 */
object ByteArrayProxy {

  /**
   * The byte array proxy. Guaranteed to never be null.
   */
  val PROXY_BA = new ByteArrayProxy()
  private val MEM_MGR = RUNTIME.getMemoryManager

}

class ByteArrayProxy extends BufferProxy[Array[Byte],JnrPointer] with ByteArrayProxyLike[JnrPointer] {

  import BufferProxyLike._

  override final protected[lmdb4s] def in(buffer: Array[Byte], ptr: JnrPointer, ptrAddr: Long): Unit = {
    val pointer = ByteArrayProxy.MEM_MGR.allocateDirect(buffer.length)
    pointer.put(0, buffer, 0, buffer.length)
    ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, buffer.length)
    ptr.putAddress(STRUCT_FIELD_OFFSET_DATA, pointer.address)
  }

  override final protected[lmdb4s] def in(buffer: Array[Byte], size: Int, ptr: JnrPointer, ptrAddr: Long): Unit = {
    // cannot reserve for byte arrays
    // FIXME: throw exception???
  }

  override final protected[lmdb4s] def out(buffer: Array[Byte], ptr: JnrPointer, ptrAddr: Long): Array[Byte] = {
    val addr = ptr.getAddress(STRUCT_FIELD_OFFSET_DATA)
    val size = ptr.getLong(STRUCT_FIELD_OFFSET_SIZE).toInt
    val pointer = ByteArrayProxy.MEM_MGR.newPointer(addr, size)
    val bytes = new Array[Byte](size)
    pointer.get(0, bytes, 0, size)
    bytes
  }
}

