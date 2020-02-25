package com.github.lmdb4s

import Meta.error

import utest._

/**
 * Test Meta.
 */
object MetaTest extends TestSuite {

  val tests = Tests {
    'errCode - errCode()
    'version - version()
  }

  /*@Test def coverPrivateConstructors(): Unit = {
    invokePrivateConstructor(classOf[Meta])
  }*/

  def errCode(): Unit = {
    assert(error(ResultCode.MDB_CORRUPTED) == "MDB_CORRUPTED: Located page was wrong type")
  }

  def version(): Unit = {
    val v = Meta.version()
    assert(v != null)
    assert(v.major == 0)
    assert(v.minor == 9)
  }
}