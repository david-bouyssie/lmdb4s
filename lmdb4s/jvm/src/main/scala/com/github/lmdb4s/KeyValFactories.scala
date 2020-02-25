package com.github.lmdb4s

import java.nio.ByteBuffer
import java.util.Comparator

import jnr.ffi.{Pointer => JnrPointer}

import com.github.lmdb4s.bindings._

trait JnrKeyValFactory[V] extends IKeyValFactory[V,JnrPointer] {

  private[lmdb4s] val libraryWrapper: ILibraryWrapper[JnrPointer] = LibraryWrapper.getWrapper()

  def proxy: BufferProxy[V, JnrPointer]

  /**
   * Create a new KeyVal to hold pointers for the buffer proxy.
   *
   * @return a non-null key value holder
   */
  private[lmdb4s] def createKeyVal(): IKeyVal[V, JnrPointer] = proxy.createKeyVal()

  private[lmdb4s] def comparator(): Comparator[V] = proxy // key/value comparator (implemented by the proxy)

  private[lmdb4s] def compareKeys(keyA: JnrPointer, keyB: JnrPointer, comparator: Comparator[V]): Int = {
    val compKeyA = proxy.allocate()
    val compKeyB = proxy.allocate()
    proxy.out(compKeyA, keyA, keyA.address)
    proxy.out(compKeyB, keyB, keyB.address)
    val result = comparator.compare(compKeyA, compKeyB)
    proxy.deallocate(compKeyA)
    proxy.deallocate(compKeyB)
    result
  }

}

object ByteArrayKeyValFactory extends JnrKeyValFactory[Array[Byte]] {
  def proxy: ByteArrayProxy = ByteArrayProxy.PROXY_BA
}

object ByteBufferKeyValFactory extends JnrKeyValFactory[ByteBuffer] {
  def proxy: BufferProxy[ByteBuffer, JnrPointer] = ByteBufferProxy.PROXY_OPTIMAL
}