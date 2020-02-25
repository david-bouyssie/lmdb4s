package com.github.lmdb4s

import java.util.Comparator

import scala.scalanative.unsafe._

import bindings.lmdb._

//@SuppressWarnings(Array("checkstyle:abstractclassname"))
abstract class BufferProxy[V,P] extends BufferProxyLike[V,P] with Comparator[V] {

  private val thisNativeProxy = this.asInstanceOf[BufferProxy[V,Ptr[Byte]]]

  /**
   * Create a new KeyVal to hold pointers for this buffer proxy.
   *
   * @return a non-null key value holder
   */
  final private[lmdb4s] def createKeyVal(): IKeyVal[V,Ptr[Byte]] = new KeyVal(thisNativeProxy)
}