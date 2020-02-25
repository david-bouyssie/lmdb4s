package com.github.lmdb4s

import java.util.Comparator

import jnr.ffi.{ Pointer => JnrPointer }

//@SuppressWarnings(Array("checkstyle:abstractclassname"))
abstract class BufferProxy[V,P] extends BufferProxyLike[V,P] with Comparator[V] {

  private val thisJnrProxy = this.asInstanceOf[BufferProxy[V,JnrPointer]]

  /**
   * Create a new KeyVal to hold pointers for this buffer proxy.
   *
   * @return a non-null key value holder
   */
  final private[lmdb4s] def createKeyVal(): IKeyVal[V,JnrPointer] = new KeyVal(thisJnrProxy)
}