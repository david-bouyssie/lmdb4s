package com.github.lmdb4s

import java.lang.Long.MAX_VALUE
import java.lang.System.getProperty
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocateDirect
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Collections.nCopies
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutionException, Executors, TimeoutException}
import java.util.{Comparator, Random}

import org.agrona.concurrent.UnsafeBuffer
import ByteBufferProxy.PROXY_OPTIMAL
import DbiFlags.MDB_CREATE
import EnvFlags.MDB_NOSUBDIR
import KeyRange.atMost
import Lmdb.createEnv
import PutFlags.MDB_NOOVERWRITE
import TestUtils._

import utest._

/**
 * Test Dbi.
 */
/*object DbiTest extends TestSuite {

  private val thisClassName = className(this.getClass)
  private var env: Env[ByteBuffer, jnr.ffi.Pointer] = _*/
object DbiTest extends TestSuite with AbstractDbiTest[ByteBuffer,jnr.ffi.Pointer] {

  protected lazy val dbName: String = DB_1
  protected lazy val thisClassName: String = className(this.getClass)
  protected lazy val kvProxyImpl = PROXY_OPTIMAL
  protected var env: Env[ByteBuffer, jnr.ffi.Pointer] = _

  protected def i2kv(value: Int): ByteBuffer = bb(value)
  protected def kv2i(kv: ByteBuffer): Int = kv.getInt()

  override def utestAfterEach(path: Seq[String]): Unit = {
    env.close()
  }

  override def utestBeforeEach(path: Seq[String]): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)
    env = createEnv().setMapSize(MEBIBYTES.toBytes(64)).setMaxReaders(2).setMaxDbs(2).open(path, MDB_NOSUBDIR)
  }

  val tests = Tests {
    'alreadyClosed - alreadyClosed()
    'customComparator - customComparator()
    'dbOpenMaxDatabases - dbOpenMaxDatabases()
    'dbiWithComparatorThreadSafety - dbiWithComparatorThreadSafety()
    'drop - drop()
    'dropAndDelete - dropAndDelete()
    'dropAndDeleteAnonymousDb - dropAndDeleteAnonymousDb()
    'getName - getName()
    'getNamesWhenDbisPresent - getNamesWhenDbisPresent()
    'getNamesWhenEmpty - getNamesWhenEmpty()
    'putAbortGet - putAbortGet()
    'putAndGetAndDeleteWithInternalTx - putAndGetAndDeleteWithInternalTx()
    'putCommitGet - putCommitGet()
    'putCommitGetByteArray - putCommitGetByteArray()
    'putDelete - putDelete()
    'putDuplicateDelete - putDuplicateDelete()
    'putReserve - putReserve()
    'putZeroByteValueForNonMdbDupSortDatabase - putZeroByteValueForNonMdbDupSortDatabase()
    'returnValueForNoDupData - returnValueForNoDupData()
    'returnValueForNoOverwrite - returnValueForNoOverwrite()
    'stats - stats()
    'testMapFullException - testMapFullException()
    //'testParallelWritesStress - testParallelWritesStress() // FIXME: infinite loop???
  }

  def customComparator(): Unit = {

    val reverseOrder = new Comparator[ByteBuffer] {
      def compare(o1: ByteBuffer, o2: ByteBuffer): Int = {
        val lexicalOrder = ByteBufferProxy.PROXY_OPTIMAL.compare(o1, o2)
        if (lexicalOrder == 0) return 0
        lexicalOrder * -1
      }
    }

    val db = env.openDbi(DB_1, reverseOrder, MDB_CREATE)
    var txn = env.txnWrite()
    try {
      assert(db.put(txn, bb(2), bb(3)))
      assert(db.put(txn, bb(4), bb(6)))
      assert(db.put(txn, bb(6), bb(7)))
      assert(db.put(txn, bb(8), bb(7)))
      txn.commit()
    } finally {
      if (txn != null) txn.close()
    }

    txn = env.txnRead()
    try {
      val iter = db.iterate(txn, atMost(bb(4)))
      assert(iter.next.key == bb(8))
      assert(iter.next.key == bb(6))
      assert(iter.next.key == bb(4))
    } finally {
      if (txn != null) txn.close()
    }
  }

  def dbiWithComparatorThreadSafety(): Unit = {
    val db = env.openDbi(dbName, kvProxyImpl, MDB_CREATE)
    //val keys = range(0, 1000).boxed.collect(toList)
    val keys = (0 until 1000).toList

    val pool = Executors.newCachedThreadPool()
    val proceed = new AtomicBoolean(true)

    val reader = pool.submit(new Runnable {
      def run(): Unit = {
        while (proceed.get) {
          val txn = env.txnRead()
          try db.get(txn, i2kv(50))
          finally if (txn != null) txn.close()
        }
      }
    })

    for (key <- keys) {
      val txn = env.txnWrite()
      try {
        db.put(txn, i2kv(key), i2kv(3))
        txn.commit()
      } finally {
        if (txn != null) txn.close()
      }
    }

    val txn = env.txnRead()
    try {
      val iter = db.iterate(txn)
      val result = new collection.mutable.ArrayBuffer[Int]()
      while (iter.hasNext) result += kv2i(iter.next.key)
      assert(result.containsSlice(keys)) // FIXME: check this is ok or call "keys.forall(result.contains)"
      //assert(result, Matchers.contains(keys.toArray(new Array[Integer](0))))
    } finally {
      if (txn != null) txn.close()
    }

    proceed.set(false)
    try {
      reader.get(1, SECONDS)
      pool.shutdown()
      pool.awaitTermination(1, SECONDS)
    } catch {
      case e@(_: ExecutionException | _: InterruptedException | _: TimeoutException) =>
        throw new IllegalStateException(e)
    }
  }

  def dropAndDelete(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    val nameDb = env.openDbi(null.asInstanceOf[Array[Byte]])
    val dbNameBytes = DB_1.getBytes(UTF_8)
    val dbNameBuffer = allocateDirect(dbNameBytes.length)
    dbNameBuffer.put(dbNameBytes).flip

    val txn = env.txnWrite()
    try {
      assert(nameDb.get(txn, dbNameBuffer) != null)
      db.drop(txn, delete = true)
      assert(nameDb.get(txn, dbNameBuffer) == null)
      txn.commit()
    } finally if (txn != null) txn.close()
  }

  def dropAndDeleteAnonymousDb(): Unit = {
    env.openDbi(DB_1, MDB_CREATE)

    val nameDb = env.openDbi(null.asInstanceOf[Array[Byte]])
    val dbNameBytes = DB_1.getBytes(UTF_8)
    val dbNameBuffer = allocateDirect(dbNameBytes.length)
    dbNameBuffer.put(dbNameBytes).flip

    val txn = env.txnWrite()
    try {
      assert(nameDb.get(txn, dbNameBuffer) != null)
      nameDb.drop(txn, delete = true)
      assert(nameDb.get(txn, dbNameBuffer) == null)
      txn.commit()
    } finally if (txn != null) txn.close()

    nameDb.close() // explicit close after drop is OK
  }

  def putCommitGetByteArray(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)

    val envBa = createEnv(ByteArrayKeyValFactory).setMapSize(MEBIBYTES.toBytes(64)).setMaxReaders(1).setMaxDbs(2).open(path, MDB_NOSUBDIR)
    try {
      val db = envBa.openDbi(DB_1, MDB_CREATE)

      var txn = envBa.txnWrite()
      try {
        db.put(txn, ba(5), ba(5))
        txn.commit()
      } finally {
        if (txn != null) txn.close()
      }

      txn = envBa.txnWrite()
      try {
        val found = db.get(txn, ba(5))
        assert(found != null)
        assert(new UnsafeBuffer(txn.value).getInt(0) == 5)
      } finally if (txn != null) txn.close()
    } finally if (envBa != null) envBa.close()
  }

  def putReserve(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    val key = bb(5)

    var txn = env.txnWrite()
    try {
      assert(db.get(txn, key) == null)
      val `val` = db.reserve(txn, key, 32, MDB_NOOVERWRITE)
      `val`.putLong(MAX_VALUE)
      assert(db.get(txn, key) != null)
      txn.commit()
    } finally if (txn != null) txn.close()

    txn = env.txnWrite()
    try {
      val value = db.get(txn, key)
      assert(value.capacity == 32)
      assert(value.getLong == MAX_VALUE)
      assert(value.getLong(8) == 0L)
    } finally {
      if (txn != null) txn.close()
    }
  }

  def putZeroByteValueForNonMdbDupSortDatabase(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)

    var txn = env.txnWrite()
    try {
      val `val` = allocateDirect(0)
      db.put(txn, bb(5), `val`)
      txn.commit()
    } finally {
      if (txn != null) txn.close()
    }

    txn = env.txnRead()
    try {
      val found = db.get(txn, bb(5))
      assert(found != null)
      assert(txn.value.capacity == 0)
    } finally {
      if (txn != null) txn.close()
    }
  }


  //@SuppressWarnings(Array("PMD.PreserveStackTrace"))
  def testMapFullException(): Unit = {
    val db = env.openDbi(DB_1, MDB_CREATE)
    val txn = env.txnWrite()

    try {
      intercept[Env.MapFullException] {
        try {
          val v = allocateDirect(1024 * 1024 * 1024)
          assert(v != null)
          db.put(txn, bb(1), v)
        }
        catch {
          case e: OutOfMemoryError =>
            // Travis CI OS X build cannot allocate this much memory, so assume OK
            throw new Env.MapFullException
        }
      }

    } finally {
      if (txn != null) txn.close()
    }
  }

  def testParallelWritesStress(): Unit = {
    if (getProperty("os.name").startsWith("Windows")) return // Windows VMs run this test too slowly

    val db = env.openDbi(DB_1, MDB_CREATE)

    // Travis CI has 1.5 cores for legacy builds
    nCopies(2, null).parallelStream.forEach(new java.util.function.Consumer[Any] {
      def accept(ignored: Any): Unit = {
        val random = new Random()
        var i = 0
        while (i < 15000) {
          db.put(bb(random.nextInt), bb(random.nextInt))

          i += 1
        }
      }
    })
  }
}
