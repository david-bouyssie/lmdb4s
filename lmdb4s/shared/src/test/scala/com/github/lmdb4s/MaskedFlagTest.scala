package com.github.lmdb4s

import EnvFlags.MDB_FIXEDMAP
import EnvFlags.MDB_NOSYNC
import EnvFlags.MDB_RDONLY_ENV
import MaskedFlag.isSet
import MaskedFlag.mask

import utest._

/**
 * Test MaskedFlag.
 */
object MaskedFlagTest extends TestSuite {

  val tests = Tests {
    'isSetOperates - isSetOperates()
    'masking - masking()
  }

  def isSetOperates(): Unit = {
    assert(!isSet(0, MDB_NOSYNC))
    assert(!isSet(0, MDB_FIXEDMAP))
    assert(!isSet(0, MDB_RDONLY_ENV))
    assert(!isSet(MDB_FIXEDMAP.getMask(), MDB_NOSYNC))
    assert(isSet(MDB_FIXEDMAP.getMask(), MDB_FIXEDMAP))
    assert(!isSet(MDB_FIXEDMAP.getMask(), MDB_RDONLY_ENV))
    assert(isSet(MDB_NOSYNC.getMask(), MDB_NOSYNC))
    assert(!isSet(MDB_NOSYNC.getMask(), MDB_FIXEDMAP))
    assert(!isSet(MDB_NOSYNC.getMask(), MDB_RDONLY_ENV))

    val syncFixed = mask(MDB_NOSYNC, MDB_FIXEDMAP)
    assert(isSet(syncFixed, MDB_NOSYNC))
    assert(isSet(syncFixed, MDB_FIXEDMAP))
    assert(!isSet(syncFixed, MDB_RDONLY_ENV))
  }

  def masking(): Unit = {
    val nullFlags = null
    assert(mask(nullFlags) == 0)

    val emptyFlags = Array[EnvFlags.Flag]()
    assert(mask(emptyFlags: _*) == 0)

    val nullElementZero = Array[EnvFlags.Flag](null)
    assert(nullElementZero.length == 1)
    assert(mask(nullElementZero: _*) == 0)
    assert(mask(MDB_NOSYNC) == MDB_NOSYNC.getMask())

    val expected = MDB_NOSYNC.getMask + MDB_FIXEDMAP.getMask
    assert(mask(MDB_NOSYNC, MDB_FIXEDMAP) == expected)
  }
}