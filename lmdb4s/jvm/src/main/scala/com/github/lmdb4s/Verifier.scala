package com.github.lmdb4s

import java.nio.ByteBuffer
import java.nio.ByteOrder.BIG_ENDIAN
import java.util.ArrayList
import java.util.Random
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CRC32

import jnr.ffi.{Pointer => JnrPointer}

import DbiFlags.MDB_CREATE


/**
 * Verifies correct operation of LmdbJava in a given environment.
 *
 * <p>
 * Due to the large variety of operating systems and Java platforms typically
 * used with LmdbJava, this class provides a convenient verification of correct
 * operating behavior through a potentially long duration set of tests that
 * carefully verify correct storage and retrieval of successively larger database entries.
 *
 * <p>
 * The verifier currently operates by incrementing a <code>long</code>
 * identifier that deterministically maps to a given Dbi and value size.
 * The key is simply the <code>long</code> identifier. The value commences with
 * a CRC that includes the identifier and the random bytes of the value. Each
 * entry is written out, and then the prior entry is retrieved using its key.
 * The prior entry's value is evaluated for accuracy and then deleted.
 * Transactions are committed in batches to ensure successive transactions
 * correctly retrieve the results of earlier transactions.
 *
 * <p>
 * Please note the verification approach may be modified in the future.
 *
 * <p>
 * If an exception is raised by this class, please:
 *
 * <ol>
 * <li>Ensure the Env passed at construction time complies with the
 * requirements specified at #Verifier(org.lmdbjava.Env)</li>
 * <li>Attempt to use a different file system to store the database (be
 * especially careful to not use network file systems, remote file systems,
 * read-only file systems etc)</li>
 * <li>Record the full exception message and stack trace, then run the verifier
 * again to see if it fails at the same or a different point</li>
 * <li>Raise a ticket on the LmdbJava Issue Tracker that confirms the above
 * details along with the failing operating system and Java version</li>
 * </ol>
 *
 */
object Verifier {
  /**
   * Number of DBIs the created environment should allow.
   */
  val DBI_COUNT = 5
  private val BATCH_SIZE = 64
  private val BUFFER_LEN = 1024 * BATCH_SIZE
  private val CRC_LENGTH = java.lang.Long.BYTES
  private val KEY_LENGTH = java.lang.Long.BYTES
}

/**
 * Create an instance of the verifier.
 *
 * <p>
 * The caller must provide an Env configured with a suitable local
 * storage location, maximum DBIs equal to #DBI_COUNT, and a
 * map size large enough to accommodate the intended verification duration.
 *
 * <p>
 * ALL EXISTING DATA IN THE DATABASE WILL BE DELETED. The caller must not
 * interact with the <code>Env</code> in any way (eg querying, transactions
 * etc) while the verifier is executing.
 *
 * @param env target that complies with the above requirements (required)
 */
final class Verifier(private val env: Env[ByteBuffer,JnrPointer]) extends Callable[Long] {
  require(env != null, "env is null")

  final private val ba = new Array[Byte](Verifier.BUFFER_LEN)
  final private val crc = new CRC32
  final private val dbis = new ArrayList[Dbi[ByteBuffer,JnrPointer]](Verifier.DBI_COUNT)
  private var id = 0L
  final private val key = ByteBuffer.allocateDirect(Verifier.KEY_LENGTH)
  final private val proceed = new AtomicBoolean(true)
  final private val rnd = new Random
  private var txn: Txn[ByteBuffer,JnrPointer] = _
  final private val `val` = ByteBuffer.allocateDirect(Verifier.BUFFER_LEN)

  key.order(BIG_ENDIAN)
  deleteDbis()
  createDbis()

  /**
   * Run the verifier until #stop() is called or an exception occurs.
   *
   * <p>
   * Successful return of this method indicates no faults were detected. If any
   * fault was detected the exception message will detail the exact point that
   * the fault was encountered.
   *
   * @return number of database rows successfully verified
   */
  override def call(): Long = {
    try {
      while (proceed.get()) {
        transactionControl()

        write(id)

        if (id > 0)
          fetchAndDelete(id - 1)

        id += 1
      }
    }
    finally if (txn != null) txn.close()

    id
  }

  /**
   * Execute the verifier for the given duration.
   *
   * <p>
   * This provides a simple way to execute the verifier for those applications
   * which do not wish to manage threads directly.
   *
   * @param duration amount of time to execute
   * @param unit     units used to express the duration
   * @return number of database rows successfully verified
   */
  def runFor(duration: Long, unit: TimeUnit): Long = {
    val deadline = System.currentTimeMillis + unit.toMillis(duration)
    val es = Executors.newSingleThreadExecutor
    val future = es.submit(this)

    try {
      while ( {System.currentTimeMillis < deadline && !future.isDone}) Thread.sleep(unit.toMillis(1))
    } catch {
      case ignored: InterruptedException =>
    } finally stop()

    val result = try future.get catch {
      case ex@(_: InterruptedException | _: ExecutionException) => throw new IllegalStateException(ex)
    } finally es.shutdown()

    result
  }

  private def createDbis(): Unit = {
    var i = 0
    while (i < Verifier.DBI_COUNT) {
      dbis.add(env.openDbi(classOf[Verifier].getSimpleName + i, MDB_CREATE))

      i += 1
    }
  }

  private def deleteDbis(): Unit = {
    //import scala.collection.JavaConversions._
    for (existingDbiName <- env.getDbiNames()) {
      val existingDbi = env.openDbi(existingDbiName)
      var txn: Txn[ByteBuffer,JnrPointer] = null
      try {
        txn = env.txnWrite()
        existingDbi.drop(txn, true)
        txn.commit()
      } finally if (txn != null) txn.close()
    }
  }

  private def fetchAndDelete(forId: Long): Unit = {
    val dbi = getDbi(forId)
    updateKey(forId)
    val fetchedValue = try dbi.get(txn, key) catch {
      case ex: LmdbException => throw new IllegalStateException("DB get id=" + forId, ex)
    }

    if (fetchedValue == null) throw new IllegalStateException("DB not found id=" + forId)

    verifyValue(forId, fetchedValue)

    try dbi.delete(txn, key) catch {
      case ex: LmdbException => throw new IllegalStateException("DB del id=" + forId, ex)
    }
  }

  private def getDbi(forId: Long) = dbis.get((forId % dbis.size).toInt)

  /**
   * Request the verifier to stop execution.
   */
  private def stop(): Unit = {
    proceed.set(false)
  }

  private def transactionControl(): Unit = {
    if (id % Verifier.BATCH_SIZE == 0) {
      if (txn != null) {
        txn.commit()
        txn.close()
      }
      rnd.nextBytes(ba)
      txn = env.txnWrite
    }
  }

  private def updateKey(forId: Long): Unit = {
    key.clear
    key.putLong(forId)
    key.flip
  }

  private def updateValue(forId: Long): Unit = {
    val rndSize = valueSize(forId)
    crc.reset()
    crc.update(forId.toInt)
    crc.update(ba, Verifier.CRC_LENGTH, rndSize)
    val crcVal = crc.getValue
    `val`.clear
    `val`.putLong(crcVal)
    `val`.put(ba, Verifier.CRC_LENGTH, rndSize)
    `val`.flip
  }

  private def valueSize(forId: Long): Int = {
    val mod = (forId % Verifier.BATCH_SIZE).toInt
    val base = 1024 * mod
    val value = if (base == 0) 512
    else base
    value - Verifier.CRC_LENGTH - Verifier.KEY_LENGTH // aim to minimise partial pages

  }

  private def verifyValue(forId: Long, bb: ByteBuffer): Unit = {
    val rndSize = valueSize(forId)
    val expected = rndSize + Verifier.CRC_LENGTH
    if (bb.limit != expected) throw new IllegalStateException("Limit error id=" + forId + " exp=" + expected + " limit=" + bb.limit)
    val crcRead = bb.getLong
    crc.reset()
    crc.update(forId.toInt)
    crc.update(bb)
    val crcVal = crc.getValue
    if (crcRead != crcVal) throw new IllegalStateException("CRC error id=" + forId)
  }

  private def write(forId: Long): Unit = {
    val dbi = getDbi(forId)
    updateKey(forId)
    updateValue(forId)
    try dbi.put(txn, key, `val`)
    catch {
      case ex: LmdbException =>
        throw new IllegalStateException("DB put id=" + forId, ex)
    }
  }
}

