package com.github.lmdb4s

import java.util.Comparator

import scala.scalanative.unsafe._

import com.github.lmdb4s.bindings._

trait NativeKeyValFactory[V] extends IKeyValFactory[V,Ptr[Byte]] {

  private[lmdb4s] val libraryWrapper: ILibraryWrapper[Ptr[Byte]] = LibraryWrapper.getWrapper()

  def proxy: BufferProxy[V, Ptr[Byte]]

  /**
   * Create a new KeyVal to hold pointers for the buffer proxy.
   *
   * @return a non-null key value holder
   */
  private[lmdb4s] def createKeyVal(): IKeyVal[V, Ptr[Byte]] = proxy.createKeyVal()

  private[lmdb4s] def comparator(): Comparator[V] = proxy // key/value comparator (implemented by the proxy)

  private[lmdb4s] def compareKeys(keyA: Ptr[Byte], keyB: Ptr[Byte], comparator: Comparator[V]): Int = {
    val compKeyA = proxy.allocate()
    val compKeyB = proxy.allocate()
    proxy.out(compKeyA, keyA, 0L) // FIXME: provide pointer address
    proxy.out(compKeyB, keyB, 0L) // FIXME: provide pointer address
    val result = comparator.compare(compKeyA, compKeyB)
    proxy.deallocate(compKeyA)
    proxy.deallocate(compKeyB)
    result
  }

}

object ByteArrayKeyValFactory extends NativeKeyValFactory[Array[Byte]] {
  def proxy: ByteArrayProxy = ByteArrayProxy.PROXY_BA
}