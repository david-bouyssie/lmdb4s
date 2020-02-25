package com.github.lmdb4s

import java.io.File
import java.io.IOException
import java.lang.Integer.BYTES
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import java.nio.ByteBuffer.allocateDirect
import java.nio.ByteOrder.BIG_ENDIAN
import java.nio.ByteOrder.LITTLE_ENDIAN

import jnr.ffi.{Pointer => JnrPointer}

import BufferProxyLike.MDB_VAL_STRUCT_SIZE
import ByteBufferProxy.AbstractByteBufferProxy.findField
import ByteBufferProxy.PROXY_OPTIMAL
import ByteBufferProxy.PROXY_SAFE
import DbiFlags.MDB_CREATE
import Lmdb.createEnv
import bindings.Library.RUNTIME
import TestUtils._
import UnsafeAccess.ALLOW_UNSAFE

import utest._


/**
 * Test ByteBufferProxy.
 */
object ByteBufferProxyTest extends TestSuite {

  private[lmdb4s] val MEM_MGR = RUNTIME.getMemoryManager

  val tests = Tests {
    'buffersMustBeDirect - buffersMustBeDirect()
    'byteOrderResets - byteOrderResets()
    'fieldNeverFound - fieldNeverFound()
    'fieldSuperclassScan - fieldSuperclassScan()
    'inOutBuffersProxyOptimal - inOutBuffersProxyOptimal()
    'inOutBuffersProxySafe - inOutBuffersProxySafe()
    'byteOrderResets - optimalAlwaysAvailable()
    'byteOrderResets - safeCanBeForced()
    'byteOrderResets - unsafeIsDefault()
  }

  //@Test(expected = classOf[ByteBufferProxy.BufferMustBeDirectException])
  def buffersMustBeDirect(): Unit = {
    val tmpDir = createTempDirectory(className(this.getClass))

    var env: Env[ByteBuffer,JnrPointer] = null
    try {
      val envBuilder = createEnv(ByteBufferKeyValFactory)
      env = envBuilder.setMaxReaders(1).open(tmpDir)

      val db = env.openDbi(DB_1, MDB_CREATE)

      val key = allocate(100)
      key.putInt(1).flip()
      val `val` = allocate(100)
      `val`.putInt(1).flip()

      intercept[ByteBufferProxy.BufferMustBeDirectException] {
        db.put(key, `val`) // error
      }

    } finally {
      if (env != null) env.close()
    }
  }

  def byteOrderResets(): Unit = {
    val retries = 100

    var i = 0
    while (i < retries) {
      val bb = PROXY_OPTIMAL.allocate()
      bb.order(LITTLE_ENDIAN)
      PROXY_OPTIMAL.deallocate(bb)

      i += 1
    }

    i = 0
    while (i < retries) {
      assert(PROXY_OPTIMAL.allocate().order == BIG_ENDIAN)

      i += 1
    }
  }

  /*
  @Test def coverPrivateConstructor(): Unit = {
    invokePrivateConstructor(classOf[ByteBufferProxy])
  }*/

  def fieldNeverFound(): Unit = {
    intercept[LmdbException] {
      findField(classOf[Exception], "notARealField")
    }
  }

  def fieldSuperclassScan(): Unit = {
    val f = findField(classOf[Env.ReadersFullException], "rc")
    assert(f != null)
  }

  def inOutBuffersProxyOptimal(): Unit = {
    checkInOut(PROXY_OPTIMAL)
  }

  def inOutBuffersProxySafe(): Unit = {
    checkInOut(PROXY_SAFE)
  }

  def optimalAlwaysAvailable(): Unit = {
    val v = PROXY_OPTIMAL
    assert(v != null)
  }

  def safeCanBeForced(): Unit = {
    val v = PROXY_SAFE
    assert(v != null)
    assert(v.getClass.getSimpleName.startsWith("Reflect"))
  }

  def unsafeIsDefault(): Unit = {
    assert(ALLOW_UNSAFE, true)
    
    val v = PROXY_OPTIMAL
    assert(v != null)
    assert(v!= PROXY_SAFE)
    assert(v.getClass.getSimpleName.startsWith("Unsafe"))
  }

  private def checkInOut(v: BufferProxy[ByteBuffer, jnr.ffi.Pointer]): Unit = { // allocate a buffer larger than max key size
    val b = allocateDirect(1000)
    b.putInt(1)
    b.putInt(2)
    b.putInt(3)
    b.flip
    b.position(BYTES) // skip 1

    val p = ByteBufferProxyTest.MEM_MGR.allocateTemporary(MDB_VAL_STRUCT_SIZE, false)
    v.in(b, p, p.address)
    val bb = allocateDirect(1)
    v.out(bb, p, p.address)
    assert(bb.getInt == 2)
    assert(bb.getInt == 3)
    assert(bb.remaining == 0)
  }
}
