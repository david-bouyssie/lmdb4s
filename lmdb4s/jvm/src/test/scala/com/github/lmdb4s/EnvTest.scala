package com.github.lmdb4s

import java.io.File
import java.io.IOException
import java.nio.ByteBuffer.allocateDirect
import java.util.Random

import CopyFlags.MDB_CP_COMPACT
import DbiFlags.MDB_CREATE
import Env.Builder.MAX_READERS_DEFAULT
import EnvFlags.MDB_NOSUBDIR
import EnvFlags.MDB_RDONLY_ENV
import Lmdb.createEnv
import Lmdb.openEnv
import TestUtils._

import utest._


/**
 * Test Env.
 */
object EnvTest extends TestSuite {

  private val thisClassName = className(this.getClass)

  val tests = Tests {
    'byteUnit - byteUnit()
    'cannotChangeMapSizeAfterOpen - cannotChangeMapSizeAfterOpen()
    'cannotChangeMaxDbsAfterOpen - cannotChangeMaxDbsAfterOpen()
    'cannotChangeMaxReadersAfterOpen - cannotChangeMaxReadersAfterOpen()
    'cannotInfoOnceClosed - cannotInfoOnceClosed()
    'cannotOpenTwice - cannotOpenTwice()
    'cannotOverflowMapSize - cannotOverflowMapSize()
    'cannotStatOnceClosed - cannotStatOnceClosed()
    'cannotSyncOnceClosed - cannotSyncOnceClosed()
    'copy - copy()
    'copyRejectsFileDestination - copyRejectsFileDestination()
    'copyRejectsMissingDestination - copyRejectsMissingDestination()
    'copyRejectsNonEmptyDestination - copyRejectsNonEmptyDestination()
    'createAsDirectory - createAsDirectory()
    'createAsFile - createAsFile()
    'detectTransactionThreadViolation - detectTransactionThreadViolation()
    'info - info()
    'mapFull - mapFull()
    'readOnlySupported - readOnlySupported()
    'setMapSize - setMapSize()
    'stats - stats()
    'testDefaultOpen - testDefaultOpen()
  }

  def byteUnit(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)

    val env = createEnv().setMaxReaders(1).setMapSize(MEBIBYTES.toBytes(1)).open(path, MDB_NOSUBDIR)

    try {
      val info = env.info()
      assert(info.mapSize == MEBIBYTES.toBytes(1))
    } finally {
      if (env != null) env.close()
    }
  }

  def cannotChangeMapSizeAfterOpen(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)

    val builder = createEnv().setMaxReaders(1)
    intercept[Env.AlreadyOpenException] {
      val env = builder.open(path, MDB_NOSUBDIR)
      try builder.setMapSize(1)
      finally if (env != null) env.close()
    }
  }

  def cannotChangeMaxDbsAfterOpen(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)

    val builder = createEnv().setMaxReaders(1)
    intercept[Env.AlreadyOpenException] {
      val env = builder.open(path, MDB_NOSUBDIR)
      try builder.setMaxDbs(1)
      finally if (env != null) env.close()
    }
  }

  def cannotChangeMaxReadersAfterOpen(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)

    val builder = createEnv().setMaxReaders(1)
    intercept[Env.AlreadyOpenException] {
      val env = builder.open(path, MDB_NOSUBDIR)
      try builder.setMaxReaders(1)
      finally if (env != null) env.close()
    }
  }

  def cannotInfoOnceClosed(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)
    val env = createEnv().setMaxReaders(1).open(path, MDB_NOSUBDIR)
    env.close()

    intercept[Env.AlreadyClosedException] {
      env.info()
    }
  }

  def cannotOpenTwice(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)
    val builder = createEnv().setMaxReaders(1)
    builder.open(path, MDB_NOSUBDIR).close()

    intercept[Env.AlreadyOpenException] {
      builder.open(path, MDB_NOSUBDIR)
    }
  }

  def cannotOverflowMapSize(): Unit = {
    val builder = createEnv().setMaxReaders(1)
    val mb = 1024 * 1024
    val size = mb * 2048 // as per issue 18

    intercept[IllegalArgumentException] {
      builder.setMapSize(size)
    }
  }

  def cannotStatOnceClosed(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)
    val env = createEnv().setMaxReaders(1).open(path, MDB_NOSUBDIR)
    env.close()

    intercept[Env.AlreadyClosedException] {
      env.stat()
    }
  }

  def cannotSyncOnceClosed(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)
    val env = createEnv().setMaxReaders(1).open(path, MDB_NOSUBDIR)
    env.close()

    intercept[Env.AlreadyClosedException] {
      env.sync(false)
    }
  }

  def copy(): Unit = {
    val dest = createTempDirectory(thisClassName + "_dest")
    assert(dest.exists)
    assert(dest.isDirectory)
    assert(dest.list.length == 0)

    val src = createTempDirectory(thisClassName +"_src")
    val env = createEnv().setMaxReaders(1).open(src)

    try {
      env.copy(dest, MDB_CP_COMPACT)
      assert(dest.list.length == 1)
    } finally {
      if (env != null) env.close()
    }
  }

  @throws[IOException]
  def copyRejectsFileDestination(): Unit = {
    val dest = createTempLmdbFile(thisClassName + "_dest", deleteLockFileOnExit = false)
    val src = createTempDirectory(thisClassName  + "_src")

    val env = createEnv().setMaxReaders(1).open(src)

    try {
      intercept[Env.InvalidCopyDestination] {
        env.copy(dest, MDB_CP_COMPACT)
      }
    }
    finally {
      if (env != null) env.close()
    }
  }

  def copyRejectsMissingDestination(): Unit = {
    val dest = createTempDirectory(thisClassName + "_dest", deleteDirOnExit = false)
    assert(dest.delete)

    val src = createTempDirectory(thisClassName + "_src")

    val env = createEnv().setMaxReaders(1).open(src)

    try {
      intercept[Env.InvalidCopyDestination] {
        env.copy(dest, MDB_CP_COMPACT)
      }
    }
    finally {
      if (env != null) env.close()
    }
  }

  def copyRejectsNonEmptyDestination(): Unit = {
    val dest = createTempDirectory(thisClassName + "_dest")
    val subDir = new File(dest, "hello")
    assert(subDir.mkdir)

    val src = createTempDirectory(thisClassName + "_src")
    val env = createEnv().setMaxReaders(1).open(src)

    try {
      intercept[Env.InvalidCopyDestination] {
        env.copy(dest, MDB_CP_COMPACT)
      }
    }
    finally {
      if (env != null) env.close()
    }
  }

  def createAsDirectory(): Unit = {
    val path = createTempDirectory(thisClassName)
    val env = createEnv().setMaxReaders(1).open(path)
    assert(path.isDirectory)
    env.sync(false)
    env.close()
    assert(env.isClosed)
    env.close() // safe to repeat
  }

  def createAsFile(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)

    val env = createEnv().setMapSize(1024 * 1024).setMaxDbs(1).setMaxReaders(1).open(path, MDB_NOSUBDIR)
    try {
      env.sync(true)
      assert(path.isFile)
    } finally {
      if (env != null) env.close()
    }
  }

  def detectTransactionThreadViolation(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)

    val env = createEnv().setMaxReaders(1).open(path, MDB_NOSUBDIR)
    try {
      env.txnRead()
      intercept[Txn.BadReaderLockException] {
        env.txnRead()
      }
    } finally {
      if (env != null) env.close()
    }
  }

  def info(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)

    val env = createEnv().setMaxReaders(4).setMapSize(123456).open(path, MDB_NOSUBDIR)
    try {
      val info = env.info()
      assert(info != null)
      assert(info.lastPageNumber == 1L)
      assert(info.lastTransactionId == 0L)
      assert(info.mapAddress == 0L)
      assert(info.mapSize == 123456L)
      assert(info.maxReaders == 4)
      assert(info.numReaders == 0)
      assert(info.toString().contains("maxReaders="))
      assert(env.getMaxKeySize() == 511)
    } finally {
      if (env != null) env.close()
    }
  }

  def mapFull(): Unit = {
    val path = createTempDirectory(thisClassName)

    val k = new Array[Byte](500)
    val key = allocateDirect(500)
    val `val` = allocateDirect(1024)
    val rnd = new Random()

    val env = createEnv().setMaxReaders(1).setMapSize(MEBIBYTES.toBytes(1)).setMaxDbs(1).open(path) // DBO: changed from 8mb to 1mb
    try {
      val db = env.openDbi(DB_1, MDB_CREATE)

      intercept[Env.MapFullException] {
        while (true) {
          rnd.nextBytes(k)
          key.clear()
          key.put(k).flip()
          `val`.clear()
          db.put(key, `val`)
        }
      }

    } finally {
      if (env != null) env.close()
    }
  }

  def readOnlySupported(): Unit = {
    val path = createTempDirectory(thisClassName)

    val rwEnv = createEnv().setMaxReaders(1).open(path)
    try {
      val rwDb = rwEnv.openDbi(DB_1, MDB_CREATE)
      rwDb.put(bb(1), bb(42))
    } finally {
      if (rwEnv != null) rwEnv.close()
    }

    val roEnv = createEnv().setMaxReaders(1).open(path, MDB_RDONLY_ENV)
    try {
      val roDb = roEnv.openDbi(DB_1)
      val roTxn = roEnv.txnRead()
      try {
        assert(roDb.get(roTxn, bb(1)) != null)
      }
      finally {
        if (roTxn != null) roTxn.close()
      }
    } finally {
      if (roEnv != null) roEnv.close()
    }
  }

  def setMapSize(): Unit = {
    val path = createTempDirectory(thisClassName)

    val k = new Array[Byte](500)
    val key = allocateDirect(500)
    val `val` = allocateDirect(1024)
    val rnd = new Random()

    val env = createEnv().setMaxReaders(1).setMapSize(50000).setMaxDbs(1).open(path)

    try {
      val db = env.openDbi(DB_1, MDB_CREATE)
      db.put(bb(1), bb(42))

      def _fillRandomlyAndCheckMapNotFull(): Boolean = {
        try {
          var i = 0
          while (i < 30) {
            rnd.nextBytes(k)
            key.clear()
            key.put(k).flip()
            `val`.clear()
            db.put(key, `val`)

            i += 1
          }
        }
        catch {
          case mfE: Env.MapFullException =>
            return true
        }

        false
      }

      var mapFullExThrown = _fillRandomlyAndCheckMapNotFull()
      assert(mapFullExThrown)

      env.setMapSize(500000)

      val roTxn = env.txnRead()
      try {
        assert(db.get(roTxn, bb(1)) == bb(42))
      } finally {
        if (roTxn != null) roTxn.close()
      }

      mapFullExThrown = _fillRandomlyAndCheckMapNotFull()
      assert(!mapFullExThrown)

    } finally {
      if (env != null) env.close()
    }
  }

  def stats(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)

    val env = createEnv().setMaxReaders(1).open(path, MDB_NOSUBDIR)
    try {
      val stat = env.stat()
      assert(stat != null)
      assert(stat.branchPages == 0L)
      assert(stat.depth == 0)
      assert(stat.entries == 0L)
      assert(stat.leafPages == 0L)
      assert(stat.overflowPages == 0L)
      assert(stat.pageSize == 4096)
      assert(stat.toString().contains("pageSize="))
    } finally {
      if (env != null) env.close()
    }
  }

  def testDefaultOpen(): Unit = {
    val path = createTempDirectory(thisClassName)

    val env = openEnv(path, 10)
    try {
      val info = env.info()
      assert(info.maxReaders == MAX_READERS_DEFAULT)

      val db = env.openDbi("test", MDB_CREATE)
      db.put(allocateDirect(1), allocateDirect(1))

    } finally {
      if (env != null) env.close()
    }
  }
}
