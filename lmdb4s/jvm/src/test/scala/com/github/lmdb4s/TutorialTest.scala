package com.github.lmdb4s

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocateDirect
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors.newCachedThreadPool
import java.util.concurrent.TimeUnit.SECONDS

import org.agrona.{DirectBuffer, MutableDirectBuffer}
import org.agrona.concurrent.UnsafeBuffer
import ByteBufferProxy.PROXY_OPTIMAL
import DbiFlags.MDB_CREATE
import DbiFlags.MDB_DUPSORT
import GetOp.MDB_SET
import Lmdb.createEnv
import SeekOp.MDB_FIRST
import SeekOp.MDB_LAST
import SeekOp.MDB_PREV
import TestUtils._

import utest._

/**
 * Welcome to LmdbJava!
 *
 * <p>
 * This short tutorial will walk you through using LmdbJava step-by-step.
 *
 * <p>
 * If you are using a 64-bit Windows, Linux or OS X machine, you can simply run
 * this tutorial by adding the LmdbJava JAR to your classpath. It includes the
 * required system libraries. If you are using another 64-bit platform, you'll
 * need to install the LMDB system library yourself. 32-bit platforms are not
 * supported.
 */
object TutorialTest extends TestSuite {

  private val DB_NAME = "my DB"
  private lazy val thisClassName: String = className(this.getClass)

  val tests = Tests {
    'tutorial1 - tutorial1()
    'tutorial2 - tutorial2()
    'tutorial3 - tutorial3()
    'tutorial4 - tutorial4()
    'tutorial5 - tutorial5()
    'tutorial6 - tutorial6()
  }

  private def createSimpleEnv() = {
    // We need a storage directory first.
    // The path cannot be on a remote file system.
    val path = createTempDirectory(thisClassName)

    // We always need an Env. An Env owns a physical on-disk storage file. One
    // Env can store many different databases (ie sorted maps).
    val env = createEnv()
      // LMDB also needs to know how large our DB might be. Over-estimating is OK.
      .setMapSize(10485760)
      // We can restrict the maximum number of concurrent readers
      .setMaxReaders(1)
      // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
      .setMaxDbs(1)
      // Now let's open the Env. The same path can be concurrently opened and
      // used in different processes, but do not open the same path twice in
      // the same process at the same time.
      .open(path)

    env
  }

  /**
   * In this first tutorial we will use LmdbJava with some basic defaults.
   *
   */
  //@SuppressWarnings(Array("ConvertToTryWithResources"))
  def tutorial1(): Unit = {

    val env = createSimpleEnv()

    // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
    // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
    val db = env.openDbi(DB_NAME, MDB_CREATE)

    // We want to store some data, so we will need a direct ByteBuffer.
    // Note that LMDB keys cannot exceed maxKeySize bytes (511 bytes by default).
    // Values can be larger.
    val key = allocateDirect(env.getMaxKeySize())
    val `val` = allocateDirect(700)
    key.put("greeting".getBytes(UTF_8)).flip()
    `val`.put("Hello world".getBytes(UTF_8)).flip()
    val valSize = `val`.remaining()

    // Now store it. Dbi.put() internally begins and commits a transaction (Txn).
    db.put(key, `val`)

    // To fetch any data from LMDB we need a Txn. A Txn is very important in
    // LmdbJava because it offers ACID characteristics and internally holds a
    // read-only key buffer and read-only value buffer. These read-only buffers
    // are always the same two Java objects, but point to different LMDB-managed
    // memory as we use Dbi (and Cursor) methods. These read-only buffers remain
    // valid only until the Txn is released or the next Dbi or Cursor call. If
    // you need data afterwards, you should copy the bytes to your own buffer.
    var txn = env.txnRead()
    try {
      val found = db.get(txn, key)
      assert(found != null)
      // The fetchedVal is read-only and points to LMDB memory
      val fetchedVal = txn.value
      assert(fetchedVal.remaining == valSize)
      // Let's double-check the fetched value is correct
      assert(UTF_8.decode(fetchedVal).toString == "Hello world")
    } finally {
      if (txn != null) txn.close()
    }

    // We can also delete. The simplest way is to let Dbi allocate a new Txn...
    db.delete(key)

    // Now if we try to fetch the deleted row, it won't be present
    txn = env.txnRead()
    try assert(db.get(txn, key) == null)
    finally if (txn != null) txn.close()

    env.close()
  }

  /**
   * In this second tutorial we'll learn more about LMDB's ACID Txns.
   *
   * @throws InterruptedException if executor shutdown interrupted
   */
  //@SuppressWarnings(Array(Array("ConvertToTryWithResources", "checkstyle:executablestatementcount")))
  def tutorial2(): Unit = {
    val env = createSimpleEnv()
    val db = env.openDbi(DB_NAME, MDB_CREATE)
    val key = allocateDirect(env.getMaxKeySize())
    val `val` = allocateDirect(700)

    // Let's write and commit "key1" via a Txn. A Txn can include multiple Dbis.
    // Note write Txns block other write Txns, due to writes being serialized.
    // It's therefore important to avoid unnecessarily long-lived write Txns.
    val txn = env.txnWrite()
    try {
      key.put("key1".getBytes(UTF_8)).flip()
      `val`.put("lmdb".getBytes(UTF_8)).flip()
      db.put(txn, key, `val`)

      // We can read data too, even though this is a write Txn.
      val found = db.get(txn, key)
      assert(found != null)

      // An explicit commit is required, otherwise Txn.close() rolls it back.
      txn.commit()

    } finally if (txn != null) txn.close()

    // Open a read-only Txn. It only sees data that existed at Txn creation time.
    val rtx = env.txnRead()

    // Our read Txn can fetch key1 without problem, as it existed at Txn creation.
    var found = db.get(rtx, key)
    assert(found != null)

    // Note that our main test thread holds the Txn. Only one Txn per thread is
    // typically permitted (the exception is a read-only Env with MDB_NOTLS).
    //
    // Let's write out a "key2" via a new write Txn in a different thread.
    val es = newCachedThreadPool()
    es.execute(new Runnable {
      def run(): Unit = {
        val txn = env.txnWrite()
        try {
          key.clear()
          key.put("key2".getBytes(UTF_8)).flip()
          db.put(txn, key, `val`)
          txn.commit()
        } finally {
          if (txn != null) txn.close()
        }
      }
    })
    es.shutdown()
    es.awaitTermination(10, SECONDS)

    // Even though key2 has been committed, our read Txn still can't see it.
    found = db.get(rtx, key)
    assert(found == null)

    // To see key2, we could create a new Txn. But a reset/renew is much faster.
    // Reset/renew is also important to avoid long-lived read Txns, as these
    // prevent the re-use of free pages by write Txns (ie the DB will grow).
    rtx.reset()

    // ... potentially long operation here ...
    rtx.renew()
    found = db.get(rtx, key)
    assert(found != null)

    // Don't forget to close the read Txn now we're completely finished. We could
    // have avoided this if we used a try-with-resources block, but we wanted to
    // play around with multiple concurrent Txns to demonstrate the "I" in ACID.
    rtx.close()
    env.close()
  }

  /**
   * In this third tutorial we'll have a look at the Cursor. Up until now we've
   * just used Dbi, which is good enough for simple cases but unsuitable if you
   * don't know the key to fetch, or want to iterate over all the data etc.
   *
   */
  //@SuppressWarnings(Array(Array("ConvertToTryWithResources", "checkstyle:executablestatementcount")))
  def tutorial3(): Unit = {
    val env = createSimpleEnv()
    val db = env.openDbi(DB_NAME, MDB_CREATE)
    val key = allocateDirect(env.getMaxKeySize())
    val `val` = allocateDirect(700)

    val txn = env.txnWrite()
    try { // A cursor always belongs to a particular Dbi.
      val c = db.openCursor(txn)
      // We can put via a Cursor. Note we're adding keys in a strange order,
      // as we want to show you that LMDB returns them in sorted order.
      key.put("zzz".getBytes(UTF_8)).flip()
      `val`.put("lmdb".getBytes(UTF_8)).flip()
      c.put(key, `val`)

      key.clear()
      key.put("aaa".getBytes(UTF_8)).flip()
      c.put(key, `val`)

      key.clear()
      key.put("ccc".getBytes(UTF_8)).flip()
      c.put(key, `val`)

      // We can read from the Cursor by key.
      c.get(key, MDB_SET)
      assert(UTF_8.decode(c.key).toString == "ccc")

      // Let's see that LMDB provides the keys in appropriate order....
      c.seek(MDB_FIRST)
      assert(UTF_8.decode(c.key).toString == "aaa")

      c.seek(MDB_LAST)
      assert(UTF_8.decode(c.key).toString == "zzz")

      c.seek(MDB_PREV)
      assert(UTF_8.decode(c.key).toString == "ccc")

      // Cursors can also delete the current key.
      c.delete()
      c.close()
      txn.commit()

    } finally {
      if (txn != null) txn.close()
    }

    // A read-only Cursor can survive its original Txn being closed. This is
    // useful if you want to close the original Txn (eg maybe you created the
    // Cursor during the constructor of a singleton with a throw-away Txn). Of
    // course, you cannot use the Cursor if its Txn is closed or currently reset.
    val tx1 = env.txnRead()
    val c = db.openCursor(tx1)
    tx1.close()

    // The Cursor becomes usable again by "renewing" it with an active read Txn.
    val tx2 = env.txnRead()
    c.renew(tx2)
    c.seek(MDB_FIRST)

    // As usual with read Txns, we can reset and renew them. The Cursor does
    // not need any special handling if we do this.
    tx2.reset()
    tx2.renew()
    c.seek(MDB_LAST)
    tx2.close()
    env.close()
  }

  /**
   * In this fourth tutorial we'll take a quick look at the iterators. These are
   * a more Java idiomatic form of using the Cursors we looked at in tutorial 3.
   *
   */
  //@SuppressWarnings(Array("ConvertToTryWithResources"))
  def tutorial4(): Unit = {
    import scala.collection.JavaConversions._

    val env = createSimpleEnv()
    val db = env.openDbi(DB_NAME, MDB_CREATE)

    val txn = env.txnWrite()
    try {
      val key = allocateDirect(env.getMaxKeySize())
      val `val` = allocateDirect(700)
      // Insert some data. Note that ByteBuffer order defaults to Big Endian.
      // LMDB does not persist the byte order, but it's critical to sort keys.
      // If your numeric keys don't sort as expected, review buffer byte order.
      `val`.putInt(100)
      key.putInt(1)
      db.put(txn, key, `val`)
      key.clear()
      key.putInt(2)
      db.put(txn, key, `val`)
      key.clear()
      // Each iterator uses a cursor and must be closed when finished.
      // Iterate forward in terms of key ordering starting with the first key.
      var it = db.iterate(txn, KeyRange.all)
      try {
        for (kv <- it.iterable()) {
          assert(kv.key != null)
          assert(kv.`val`!= null)
        }
      }
      finally {
        if (it != null) it.close()
      }

      // Iterate backward in terms of key ordering starting with the last key.
      it = db.iterate(txn, KeyRange.allBackward)
      try {
        for (kv <- it.iterable()) {
          assert(kv.key != null)
          assert(kv.`val` != null)
        }
      }
      finally {
        if (it != null) it.close()
      }

      // There are many ways to control the desired key range via KeyRange, such
      // as arbitrary start and stop values, direction etc. We've adopted Guava's
      // terminology for our range classes (see KeyRangeType for further details).
      key.putInt(1)

      val range = KeyRange.atLeastBackward(key)
      it = db.iterate(txn, range)
      try {
        for (kv <- it.iterable()) {
          assert(kv.key != null)
          assert(kv.`val` != null)
        }
      } finally {
        if (it != null) it.close()
      }

    } finally {
      if (txn != null) txn.close()
    }

    env.close()
  }

  /**
   * In this fifth tutorial we'll explore multiple values sharing a single key.
   *
   */
  //@SuppressWarnings(Array("ConvertToTryWithResources"))
  def tutorial5(): Unit = {
    val env = createSimpleEnv()

    // This time we're going to tell the Dbi it can store > 1 value per key.
    // There are other flags available if we're storing integers etc.
    val db = env.openDbi(DB_NAME, MDB_CREATE, MDB_DUPSORT)

    // Duplicate support requires both keys and values to be <= max key size.
    val key = allocateDirect(env.getMaxKeySize())
    val `val` = allocateDirect(env.getMaxKeySize())

    val txn = env.txnWrite()
    try {
      val c = db.openCursor(txn)
      // Store one key, but many values, and in non-natural order.
      key.put("key".getBytes(UTF_8)).flip()
      `val`.put("xxx".getBytes(UTF_8)).flip()
      c.put(key, `val`)
      `val`.clear()
      `val`.put("kkk".getBytes(UTF_8)).flip()
      c.put(key, `val`)
      `val`.clear()
      `val`.put("lll".getBytes(UTF_8)).flip()
      c.put(key, `val`)

      // Cursor can tell us how many values the current key has.
      val count = c.count()
      assert(count == 3L)

      // Let's position the Cursor. Note sorting still works.
      c.seek(MDB_FIRST)
      assert(UTF_8.decode(c.`val`).toString == "kkk")

      c.seek(MDB_LAST)
      assert(UTF_8.decode(c.`val`).toString == "xxx")

      c.seek(MDB_PREV)
      assert(UTF_8.decode(c.`val`).toString == "lll")

      c.close()
      txn.commit()

    } finally if (txn != null) txn.close()

    env.close()
  }

  /**
   * Next up we'll show you how to easily check your platform (operating system
   * and Java version) is working properly with LmdbJava and the embedded LMDB
   * native library.
   *
   */
  //@SuppressWarnings(Array("ConvertToTryWithResources"))
  def tutorial6(): Unit = {
    // Note we need to specify the Verifier's DBI_COUNT for the Env.
    val env = createEnv().setMapSize(10485760).setMaxDbs(Verifier.DBI_COUNT).open(createTempDirectory(thisClassName))
    // Create a Verifier (it's a Callable<Long> for those needing full control).
    val v = new Verifier(env)
    // We now run the verifier for 3 seconds; it raises an exception on failure.
    // The method returns the number of entries it successfully verified.
    v.runFor(3, SECONDS)
    env.close()
  }

  /*
  /**
   * In this final tutorial we'll look at using Agrona's DirectBuffer.
   *
   */
  //@SuppressWarnings(Array("ConvertToTryWithResources"))
  def tutorial7(): Unit = {
    // The critical difference is we pass the PROXY_DB field to Env.create().
    // There's also a PROXY_SAFE if you want to stop ByteBuffer's Unsafe use.
    // Aside from that and a different type argument, it's the same as usual...
    val env = create(PROXY_DB).setMapSize(10_485_760).setMaxDbs(1).open(tmp.newFolder)
    val db = env.openDbi(DB_NAME, MDB_CREATE)
    val keyBb = allocateDirect(env.getMaxKeySize)
    val key = new UnsafeBuffer(keyBb)
    val `val` = new UnsafeBuffer(allocateDirect(700))
    try {
      val txn = env.txnWrite
      try {
        try {
          val c = db.openCursor(txn)
          try { // Agrona is faster than ByteBuffer and its methods are nicer...
            `val`.putStringWithoutLengthUtf8(0, "The Value")
            key.putStringWithoutLengthUtf8(0, "yyy")
            c.put(key, `val`)
            key.putStringWithoutLengthUtf8(0, "ggg")
            c.put(key, `val`)
            c.seek(MDB_FIRST)
            assertThat(c.key.getStringWithoutLengthUtf8(0, env.getMaxKeySize), startsWith("ggg"))
            c.seek(MDB_LAST)
            assertThat(c.key.getStringWithoutLengthUtf8(0, env.getMaxKeySize), startsWith("yyy"))
            // DirectBuffer has no position concept. Often you don't want to store
            // the unnecessary bytes of a varying-size buffer. Let's have a look...
            val keyLen = key.putStringWithoutLengthUtf8(0, "12characters")
            assertThat(keyLen, is(12))
            assertThat(key.capacity, is(env.getMaxKeySize))
            // To only store the 12 characters, we simply call wrap:
            key.wrap(key, 0, keyLen)
            assertThat(key.capacity, is(keyLen))
            c.put(key, `val`)
            c.seek(MDB_FIRST)
            assertThat(c.key.capacity, is(keyLen))
            assertThat(c.key.getStringWithoutLengthUtf8(0, c.key.capacity), is("12characters"))
            // To store bigger values again, just wrap the original buffer.
            key.wrap(keyBb)
            assertThat(key.capacity, is(env.getMaxKeySize))
          } finally if (c != null) c.close()
        }
        txn.commit()
      } finally if (txn != null) txn.close()
    }
    env.close()
  }*/

  // You've finished! There are lots of other neat things we could show you (eg
  // how to speed up inserts by appending them in key order, using integer
  // or reverse ordered keys, using Env.DISABLE_CHECKS_PROP etc), but you now
  // know enough to tackle the JavaDocs with confidence. Have fun!
}

