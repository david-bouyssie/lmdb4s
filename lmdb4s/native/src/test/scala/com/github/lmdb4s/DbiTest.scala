package com.github.lmdb4s

import scala.scalanative.unsafe._

import EnvFlags.MDB_NOSUBDIR
import Lmdb.createEnv
import TestUtils._

import utest._

/**
 * Test Dbi.
 */
object DbiTest extends TestSuite with AbstractDbiTest[Array[Byte],Ptr[Byte]] {

  protected lazy val dbName: String = DB_1
  protected lazy val thisClassName: String = className(this.getClass)
  protected lazy val kvProxyImpl: ByteArrayProxy = ByteArrayProxy.PROXY_BA
  protected var env: Env[Array[Byte], Ptr[Byte]] = _

  protected def i2kv(value: Int): Array[Byte] = ba(value)
  protected def kv2i(kv: Array[Byte]): Int = getKVAsInt(kv)

  override def utestAfterEach(path: Seq[String]): Unit = {
    env.close()
  }

  override def utestBeforeEach(path: Seq[String]): Unit = {
    val path = createTempLmdbFile(thisClassName, deleteLockFileOnExit = true)
    env = createEnv().setMapSize(MEBIBYTES.toBytes(64)).setMaxReaders(2).setMaxDbs(2).open(path, MDB_NOSUBDIR).asInstanceOf[Env[Array[Byte], Ptr[Byte]]]
  }

  val tests = Tests {
    'alreadyClosed - alreadyClosed()
    //'customComparator - customComparator()
    'dbOpenMaxDatabases - dbOpenMaxDatabases()
    //'dbiWithComparatorThreadSafety - dbiWithComparatorThreadSafety()
    'drop - drop()
    //'dropAndDelete - dropAndDelete()
    //'dropAndDeleteAnonymousDb - dropAndDeleteAnonymousDb()
    'getName - getName()
    'getNamesWhenDbisPresent - getNamesWhenDbisPresent()
    'getNamesWhenEmpty - getNamesWhenEmpty()
    'putAbortGet - putAbortGet()
    'putAndGetAndDeleteWithInternalTx - putAndGetAndDeleteWithInternalTx()
    'putCommitGet - putCommitGet()
    //'putCommitGetByteArray - putCommitGetByteArray()
    'putDelete - putDelete()
    'putDuplicateDelete - putDuplicateDelete()
    //'putReserve - putReserve()
    //'putZeroByteValueForNonMdbDupSortDatabase - putZeroByteValueForNonMdbDupSortDatabase()
    'returnValueForNoDupData - returnValueForNoDupData()
    'returnValueForNoOverwrite - returnValueForNoOverwrite()
    'stats - stats()
    //'testMapFullException - testMapFullException()
    //'testParallelWritesStress - testParallelWritesStress()
  }

}