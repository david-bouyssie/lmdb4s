package com.github.lmdb4s

import scala.scalanative.unsafe._

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

}

class ByteArrayProxy extends BufferProxy[Array[Byte],Ptr[Byte]] with ByteArrayProxyLike[Ptr[Byte]] {

  //import BufferProxyLike._

  override final protected[lmdb4s] def in(buffer: Array[Byte], ptr: Ptr[Byte], ptrAddr: Long): Unit = {
    val bufferLen = buffer.length

    val kvPtr = ptr.asInstanceOf[bindings.lmdb.KVPtr]
    kvPtr._1 = bufferLen

    if (bufferLen > 0) {
      kvPtr._2 = buffer.asInstanceOf[scala.scalanative.runtime.ByteArray].at(0)
    }

    /*ptr.asInstanceOf[Ptr[Long]].update(STRUCT_FIELD_OFFSET_SIZE, buffer.length) // STRUCT_FIELD_OFFSET_SIZE = 0

    val byteArrayPtr = buffer.asInstanceOf[scala.scalanative.runtime.ByteArray].at(0)
    scala.scalanative.libc.string.memcpy(ptr + sizeof[Long], byteArrayPtr, bufferLen) // STRUCT_FIELD_OFFSET_DATA = sizeof[Long]
    */

    /*val pointer = ByteArrayProxy.MEM_MGR.allocateDirect(buffer.length)
    pointer.put(0, buffer, 0, buffer.length)
    ptr.putLong(STRUCT_FIELD_OFFSET_SIZE, buffer.length)
    ptr.putAddress(STRUCT_FIELD_OFFSET_DATA, pointer.address)*/
  }

  override final protected[lmdb4s] def in(buffer: Array[Byte], size: Int, ptr: Ptr[Byte], ptrAddr: Long): Unit = {
    // cannot reserve for byte arrays
    // FIXME: throw exception???
  }

  override final protected[lmdb4s] def out(buffer: Array[Byte], ptr: Ptr[Byte], ptrAddr: Long): Array[Byte] = {

    val kvPtr = ptr.asInstanceOf[bindings.lmdb.KVPtr]
    val size = kvPtr._1

    val byteArray = scala.scalanative.runtime.ByteArray.alloc(size.toInt) // allocate memory
    val byteArrayPtr = byteArray.at(0)

    scala.scalanative.libc.string.memcpy(byteArrayPtr, kvPtr._2, size)

    byteArray.asInstanceOf[Array[Byte]]

    /*val size: Long = ptr.asInstanceOf[Ptr[Long]](STRUCT_FIELD_OFFSET_SIZE)

    val byteArray = scala.scalanative.runtime.ByteArray.alloc(size.toInt) // allocate memory
    val byteArrayPtr = byteArray.at(0)

    scala.scalanative.libc.string.memcpy(byteArrayPtr, ptr + sizeof[Long], size) // STRUCT_FIELD_OFFSET_DATA = sizeof[Long]

    byteArray.asInstanceOf[Array[Byte]]*/

    /*val addr = ptr.getAddress(STRUCT_FIELD_OFFSET_DATA)
    val size = ptr.getLong(STRUCT_FIELD_OFFSET_SIZE).toInt
    val pointer = ByteArrayProxy.MEM_MGR.newPointer(addr, size)
    val bytes = new Array[Byte](size)
    pointer.get(0, bytes, 0, size)
    bytes*/
  }
}

