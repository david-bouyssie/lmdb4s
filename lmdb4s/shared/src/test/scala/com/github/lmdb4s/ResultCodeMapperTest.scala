package com.github.lmdb4s

import java.lang.Integer.MAX_VALUE

import collection.mutable.HashSet

import ResultCode.MDB_CURSOR_FULL
import ResultCodeMapper.checkRc

import utest._

/**
 * Test ResultCodeMapper and LmdbException.
 */
object ResultCodeMapperTest extends TestSuite {

  // TODO: put in shared TestUtils
  // Emulate JUnit fail function
  @throws[Exception]
  protected def fail(msg: String): Unit = {
    throw new Exception(msg)
  }

  val tests = Tests {
    'checkErrAll - checkErrAll()
    'checkErrConstantDerived - checkErrConstantDerived()
    'checkErrConstantDerivedMessage - checkErrConstantDerivedMessage()
    'checkErrCursorFull - checkErrCursorFull()
    'checkErrUnknownResultCode - checkErrUnknownResultCode()
    'lmdbExceptionPreservesRootCause - lmdbExceptionPreservesRootCause()
    'mapperReturnsUnique - mapperReturnsUnique()
    'noDuplicateResultCodes - noDuplicateResultCodes()
  }

  // separate collection instances used to simplify duplicate RC detection
  lazy val EXCEPTIONS = Array(
    new Dbi.BadDbiException(),
    new Txn.BadReaderLockException(),
    new Txn.BadException(),
    new Dbi.BadValueSizeException(),
    new LmdbNativeException.PageCorruptedException(),
    new Cursor.FullException(),
    new Dbi.DbFullException(),
    new Dbi.IncompatibleException(),
    new Env.FileInvalidException(),
    new Dbi.KeyExistsException(),
    new Env.MapFullException(),
    new Dbi.MapResizedException(),
    new Dbi.KeyNotFoundException(),
    new LmdbNativeException.PageFullException(),
    new LmdbNativeException.PageNotFoundException(),
    new LmdbNativeException.PanicException(),
    new Env.ReadersFullException(),
    new LmdbNativeException.TlsFullException(),
    new Txn.TxFullException(),
    new Env.VersionMismatchException()
  )

  lazy val RESULT_CODES: Array[Int] = EXCEPTIONS.map(_.getResultCode())

  def checkErrAll(): Unit = {
    for (rc <- RESULT_CODES) {
      val e = intercept[LmdbNativeException] {
        checkRc(rc)
        fail("Exception expected for RC " + rc)
      }

      assert(e.getResultCode() == rc)
    }
  }


  def checkErrConstantDerived(): Unit = {
    intercept[LmdbNativeException.SystemException] {
      checkRc(20) // ENOENT (20): No such file or directory
    }
  }

  def checkErrConstantDerivedMessage(): Unit = {
    val ex = intercept[LmdbNativeException.SystemException] {
      checkRc(2) // ENOTDIR (2): Not a directory
      fail("Should have raised exception")
    }
    assert(ex.getMessage.contains("No such file or directory"))
  }

  def checkErrCursorFull(): Unit = {
    intercept[Cursor.FullException] {
      checkRc(MDB_CURSOR_FULL)
    }
  }

  def checkErrUnknownResultCode(): Unit = {
    intercept[IllegalArgumentException] {
      checkRc(MAX_VALUE)
    }
  }

  /*@Test def coverPrivateConstructors(): Unit = {
    invokePrivateConstructor(classOf[ResultCodeMapper])
  }*/

  def lmdbExceptionPreservesRootCause(): Unit = {
    val cause = new IllegalStateException("root cause")
    val e = new LmdbException("test", cause)
    assert(e.getCause == cause)
    assert(e.getMessage == "test")
  }

  def mapperReturnsUnique(): Unit = {
    val seen = new HashSet[LmdbNativeException]()

    for (rc <- RESULT_CODES) {
      val ex = intercept[LmdbNativeException] {
        checkRc(rc)
      }
      assert(ex != null)
      seen.add(ex)
    }
    assert(seen.size == RESULT_CODES.length)
  }

  def noDuplicateResultCodes(): Unit = {
    assert(RESULT_CODES.length == ResultCodeMapperTest.EXCEPTIONS.length)
  }
}