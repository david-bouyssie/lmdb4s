package com.github.lmdb4s

import java.lang.Long.BYTES

import utest._

import bindings.Library
import Library.MDB_envinfo
import Library.RUNTIME
//import TestUtils.invokePrivateConstructor

/**
 * Test Library.
 */
object LibraryTest extends TestSuite {

  val tests = Tests {
    //'coverPrivateConstructors - coverPrivateConstructors()
    'structureFieldOrder - structureFieldOrder()
  }

  /*def coverPrivateConstructors(): Unit = {
    invokePrivateConstructor(classOf[bindings.Library])
    invokePrivateConstructor(classOf[UnsafeAccess])
  }*/

  def structureFieldOrder(): Unit = {
    val v = new MDB_envinfo(RUNTIME)
    assert(v.f0_me_mapaddr.offset == 0L)
    assert(v.f1_me_mapsize.offset == BYTES.toLong)
  }
}