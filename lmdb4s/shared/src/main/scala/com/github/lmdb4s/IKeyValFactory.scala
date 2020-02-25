package com.github.lmdb4s

import java.util.Comparator

import com.github.lmdb4s.bindings.ILibraryWrapper

// TODO: rename ILmdbContext???
trait IKeyValFactory[V,P >: Null] {

  private[lmdb4s] def libraryWrapper: ILibraryWrapper[P]

  private[lmdb4s] def createKeyVal(): IKeyVal[V, P]

  private[lmdb4s] def comparator(): Comparator[V] // key/value comparator (implemented by the proxy)

  private[lmdb4s] def compareKeys(keyA: P, keyB: P, comparator: Comparator[V]): Int

  /*
  Implem:
  new Object with Library.ComparatorCallback {
      def compare(keyA: jnr.ffi.Pointer, keyB: jnr.ffi.Pointer): Int = {
        val compKeyA = proxy.allocate()
        val compKeyB = proxy.allocate()
        proxy.out(compKeyA, keyA.asInstanceOf[Env.LIB.AnyPointer], keyA.address) // FIXME: replace asInstanceOf by the sealed trait pattern
        proxy.out(compKeyB, keyB.asInstanceOf[Env.LIB.AnyPointer], keyB.address) // FIXME: replace asInstanceOf by the sealed trait pattern
        val result = comparator.compare(compKeyA, compKeyB)
        proxy.deallocate(compKeyA)
        proxy.deallocate(compKeyB)
        result
      }
    }
   */
}
