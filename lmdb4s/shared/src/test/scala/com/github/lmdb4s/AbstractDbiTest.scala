package com.github.lmdb4s

import java.nio.charset.StandardCharsets.UTF_8

import com.github.lmdb4s.DbiFlags.{MDB_CREATE, MDB_DUPSORT}
import com.github.lmdb4s.GetOp.MDB_SET_KEY
import com.github.lmdb4s.PutFlags.{MDB_NODUPDATA, MDB_NOOVERWRITE}

import utest._

/**
 * Test Dbi.
 */
trait AbstractDbiTest[KVProxy >: Null,Pointer >: Null] {

  protected def dbName: String
  protected def kvProxyImpl: BufferProxyLike[KVProxy,Pointer]
  protected def env: Env[KVProxy, Pointer]

  protected def i2kv(value: Int): KVProxy
  protected def kv2i(kv: KVProxy): Int

  def alreadyClosed(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    db.put(i2kv(1), i2kv(42))
    db.close()

    intercept[LmdbNativeException.SystemException] {
      db.put(i2kv(2), i2kv(42)) // error
    }
  }

  /*def customComparator(): Unit = {

    val reverseOrder = new Comparator[ByteBuffer] {
      def compare(o1: ByteBuffer, o2: ByteBuffer): Int = {
        val lexicalOrder = ByteBufferProxy.PROXY_OPTIMAL.compare(o1, o2)
        if (lexicalOrder == 0) return 0
        lexicalOrder * -1
      }
    }

    val db = env.openDbi(dbName, reverseOrder, MDB_CREATE)
    var txn = env.txnWrite()
    try {
      assert(db.put(txn, i2kv(2), i2kv(3)))
      assert(db.put(txn, i2kv(4), i2kv(6)))
      assert(db.put(txn, i2kv(6), i2kv(7)))
      assert(db.put(txn, i2kv(8), i2kv(7)))
      txn.commit()
    } finally {
      if (txn != null) txn.close()
    }

    txn = env.txnRead()
    try {
      val iter = db.iterate(txn, atMost(i2kv(4)))
      assert(iter.next.key == i2kv(8))
      assert(iter.next.key == i2kv(6))
      assert(iter.next.key == i2kv(4))
    } finally {
      if (txn != null) txn.close()
    }
  }*/

  //@SuppressWarnings(Array("ResultOfObjectAllocationIgnored"))
  def dbOpenMaxDatabases(): Unit = {
    env.openDbi("db1 is OK", MDB_CREATE)
    env.openDbi("db2 is OK", MDB_CREATE)

    intercept[Dbi.DbFullException] {
      env.openDbi("db3 fails", MDB_CREATE)
    }
  }

  def drop(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    var txn = env.txnWrite()
    try {
      db.put(txn, i2kv(1), i2kv(42))
      db.put(txn, i2kv(2), i2kv(42))
      assert(db.get(txn, i2kv(1)) != null)
      assert(db.get(txn, i2kv(2)) != null)
      db.drop(txn)
      assert(db.get(txn, i2kv(1)) == null) // data gone

      assert(db.get(txn, i2kv(2)) == null)
      db.put(txn, i2kv(1), i2kv(42)) // ensure DB still works

      db.put(txn, i2kv(2), i2kv(42))
      assert(db.get(txn, i2kv(1)) != null)
      assert(db.get(txn, i2kv(2)) != null)
    } finally {
      if (txn != null) txn.close()
    }
  }

  /*def dropAndDelete(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    val nameDb = env.openDbi(null.asInstanceOf[Array[Byte]])
    val dbNameBytes = dbName.getBytes(UTF_8)
    val dbNameBuffer = allocateDirect(dbNameBytes.length)
    dbNameBuffer.put(dbNameBytes).flip

    val txn = env.txnWrite()
    try {
      assert(nameDb.get(txn, dbNameBuffer) != null)
      db.drop(txn, delete = true)
      assert(nameDb.get(txn, dbNameBuffer) == null)
      txn.commit()
    } finally if (txn != null) txn.close()
  }*/

  /*def dropAndDeleteAnonymousDb(): Unit = {
    env.openDbi(dbName, MDB_CREATE)

    val nameDb = env.openDbi(null.asInstanceOf[Array[Byte]])
    val dbNameBytes = dbName.getBytes(UTF_8)
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
  }*/

  def getName(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    assert(db.getName() sameElements dbName.getBytes(UTF_8))
  }

  def getNamesWhenDbisPresent(): Unit = {
    val dbHello = Array[Byte]('h', 'e', 'l', 'l', 'o')
    val dbWorld = Array[Byte]('w', 'o', 'r', 'l', 'd')
    env.openDbi(dbHello, MDB_CREATE)
    env.openDbi(dbWorld, MDB_CREATE)
    val dbiNames = env.getDbiNames()
    assert(dbiNames.length == 2)
    assert(dbiNames.head sameElements dbHello)
    assert(dbiNames(1) sameElements dbWorld)

    val dbiNamesAsStr = env.getDbiNames(UTF_8)
    assert(dbiNamesAsStr.length == 2)
    assert(dbiNamesAsStr.head == new String(dbHello,UTF_8))
    assert(dbiNamesAsStr(1) == new String(dbWorld,UTF_8))
  }

  def getNamesWhenEmpty(): Unit = {
    val dbiNames = env.getDbiNames()
    assert(dbiNames.isEmpty)
  }

  def putAbortGet(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)

    var txn = env.txnWrite()
    try {
      db.put(txn, i2kv(5), i2kv(5))
      txn.abort()
    } finally {
      if (txn != null) txn.close()
    }

    txn = env.txnWrite()
    try assert(db.get(txn, i2kv(5)) == null)
    finally if (txn != null) txn.close()
  }

  def putAndGetAndDeleteWithInternalTx(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    db.put(i2kv(5), i2kv(5))

    var txn = env.txnRead()
    try {
      val found = db.get(txn, i2kv(5))
      assert(found != null)
      assert(kv2i(txn.value) == 5)
    } finally {
      if (txn != null) txn.close()
    }

    assert(db.delete(i2kv(5)))
    assert(!db.delete(i2kv(5)))

    txn = env.txnRead()
    try assert(db.get(txn, i2kv(5)) == null)
    finally if (txn != null) txn.close()
  }

  def putCommitGet(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    var txn = env.txnWrite()
    try {
      db.put(txn, i2kv(5), i2kv(5))
      txn.commit()
    } finally {
      if (txn != null) txn.close()
    }

    txn = env.txnWrite()
    try {
      val found = db.get(txn, i2kv(5))
      assert(found != null)
      assert(kv2i(txn.value) == 5)
    } finally {
      if (txn != null) txn.close()
    }
  }

  /*def putCommitGetByteArray(): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)

    val envBa = createEnv(ByteArrayKeyValFactory).setMapSize(MEBIBYTES.toBytes(64)).setMaxReaders(1).setMaxDbs(2).open(path, MDB_NOSUBDIR)
    try {
      val db = envBa.openDbi(dbName, MDB_CREATE)

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
  }*/

  def putDelete(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    val txn = env.txnWrite()
    try {
      db.put(txn, i2kv(5), i2kv(5))
      assert(db.delete(txn, i2kv(5)))
      assert(db.get(txn, i2kv(5)) == null)
      txn.abort()
    } finally {
      if (txn != null) txn.close()
    }
  }

  def putDuplicateDelete(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE, MDB_DUPSORT)

    val txn = env.txnWrite()
    try {
      db.put(txn, i2kv(5), i2kv(5))
      db.put(txn, i2kv(5), i2kv(6))
      db.put(txn, i2kv(5), i2kv(7))
      assert(db.delete(txn, i2kv(5), i2kv(6)))
      assert(!db.delete(txn, i2kv(5), i2kv(6)))
      assert(db.delete(txn, i2kv(5), i2kv(5)))
      assert(!db.delete(txn, i2kv(5), i2kv(5)))

      val cursor = db.openCursor(txn)
      try {
        val key = i2kv(5)
        cursor.get(key, MDB_SET_KEY)
        assert(cursor.count == 1L)
      } finally {
        if (cursor != null) cursor.close()
      }

      txn.abort()
    } finally {
      if (txn != null) txn.close()
    }
  }

  /*def putReserve(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    val key = i2kv(5)

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
  }*/

  /*def putZeroByteValueForNonMdbDupSortDatabase(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)

    var txn = env.txnWrite()
    try {
      val `val` = allocateDirect(0)
      db.put(txn, i2kv(5), `val`)
      txn.commit()
    } finally {
      if (txn != null) txn.close()
    }

    txn = env.txnRead()
    try {
      val found = db.get(txn, i2kv(5))
      assert(found != null)
      assert(txn.value.capacity == 0)
    } finally {
      if (txn != null) txn.close()
    }
  }*/

  def returnValueForNoDupData(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE, MDB_DUPSORT)
    val txn = env.txnWrite()
    try { // ok
      assert(db.put(txn, i2kv(5), i2kv(6), MDB_NODUPDATA))
      assert(db.put(txn, i2kv(5), i2kv(7), MDB_NODUPDATA))
      assert(!db.put(txn, i2kv(5), i2kv(6), MDB_NODUPDATA))
    } finally {
      if (txn != null) txn.close()
    }
  }

  def returnValueForNoOverwrite(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    val txn = env.txnWrite()
    try {
      assert(db.put(txn, i2kv(5), i2kv(6), MDB_NOOVERWRITE))
      // fails, but gets exist val
      assert(!db.put(txn, i2kv(5), i2kv(8), MDB_NOOVERWRITE))
      assert(kv2i(txn.value) == 6)
    } finally {
      if (txn != null) txn.close()
    }
  }

  def stats(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    db.put(i2kv(1), i2kv(42))
    db.put(i2kv(2), i2kv(42))
    db.put(i2kv(3), i2kv(42))

    val txn = env.txnRead()

    val stat = try db.stat(txn)
    finally if (txn != null) txn.close()

    assert(stat != null)
    assert(stat.branchPages == 0L)
    assert(stat.depth == 1)
    assert(stat.entries == 3L)
    assert(stat.leafPages == 1L)
    assert(stat.overflowPages == 0L)
    assert(stat.pageSize == 4096)
  }

  //@SuppressWarnings(Array("PMD.PreserveStackTrace"))
  /*def testMapFullException(): Unit = {
    val db = env.openDbi(dbName, MDB_CREATE)
    val txn = env.txnWrite()

    try {
      intercept[Env.MapFullException] {
        try {
          val v = allocateDirect(1024 * 1024 * 1024)
          assert(v != null)
          db.put(txn, i2kv(1), v)
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
  }*/

  /*def testParallelWritesStress(): Unit = {
    if (getProperty("os.name").startsWith("Windows")) return // Windows VMs run this test too slowly

    val db = env.openDbi(dbName, MDB_CREATE)

    // Travis CI has 1.5 cores for legacy builds
    nCopies(2, null).parallelStream.forEach(new java.util.function.Consumer[Any] {
      def accept(ignored: Any): Unit = {
        val random = new Random()
        var i = 0
        while (i < 15000) {
          db.put(i2kv(random.nextInt), i2kv(random.nextInt))

          i += 1
        }
      }
    })
  }*/
}
