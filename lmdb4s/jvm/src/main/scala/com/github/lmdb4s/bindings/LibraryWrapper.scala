package com.github.lmdb4s.bindings

import java.util.Comparator

import jnr.constants.ConstantSet.getConstantSet
import jnr.ffi.byref._
import jnr.ffi.Memory.allocateDirect
import jnr.ffi.NativeType.ADDRESS
import jnr.ffi.{ Pointer => JnrPointer }

import com.github.lmdb4s._
import Meta.Version
import ResultCodeMapper.checkRc

import Library.LIB
import Library.RUNTIME

object LibraryWrapper {
  private lazy val singleton = new LibraryWrapper()
  def getWrapper(): ILibraryWrapper[JnrPointer] = singleton
}

class LibraryWrapper private() extends ILibraryWrapper[JnrPointer] {

  type Pointer = AnyPointer // TODO: remove this alias

  private val POSIX_ERR_NO = "Errno"
  private val CONSTANTS = getConstantSet(POSIX_ERR_NO)
  def throwSystemException(rc: Int): Unit = {
    val constant = CONSTANTS.getConstant(rc)
    require(constant != null, s"Unknown result code $rc")

    val msg = s"${constant.name} $constant"

    throw new LmdbNativeException.SystemException(rc, msg)

    ()
  }

  // FIXME: replace me by the sealed trait pattern
  //@inline implicit private def castPointer(ptr: Pointer): jnr.ffi.Pointer = ptr.asInstanceOf[jnr.ffi.Pointer]

  def mdb_cursor_close(cursor: Pointer): Unit = LIB.mdb_cursor_close(cursor)
  def mdb_cursor_count(cursor: Pointer): Long = {
    val countp = new NativeLongByReference()
    checkRc(LIB.mdb_cursor_count(cursor, countp))
    countp.longValue
  }
  def mdb_cursor_del(cursorPtr: Pointer, flags: Int): Unit = checkRc(LIB.mdb_cursor_del(cursorPtr, flags))
  def mdb_cursor_get(cursorPtr: Pointer, k: Pointer, v: Pointer, cursorOp: Int): Boolean = {
    val rc = LIB.mdb_cursor_get(cursorPtr, k, v, cursorOp)
    if (rc == ResultCode.MDB_NOTFOUND) return false
    checkRc(rc)
    true
  }
  def mdb_cursor_open(txnPtr: Pointer, dbiPtr: Pointer): Pointer = {
    val cursorPtr = new PointerByReference()
    checkRc(LIB.mdb_cursor_open(txnPtr, dbiPtr, cursorPtr))
    cursorPtr.getValue
  }
  def mdb_cursor_put(cursorPtr: Pointer, key: Pointer, data: Pointer, flags: Int): Boolean = {
    val rc = LIB.mdb_cursor_put(cursorPtr, key, data, flags)

    isPutOK(rc, flags)
  }
  def mdb_cursor_renew(txnPtr: Pointer, cursorPtr: Pointer): Unit = checkRc(LIB.mdb_cursor_renew(txnPtr, cursorPtr))

  def mdb_dbi_close(envPtr: Pointer, dbiPtr: Pointer): Unit = LIB.mdb_dbi_close(envPtr, dbiPtr)
  def mdb_dbi_open(txnPtr: Pointer, name: Array[Byte], flags: Seq[DbiFlags.Flag]): Pointer = {

    val flagsMask = MaskedFlag.mask(flags: _*)

    val dbiPtr = allocateDirect(RUNTIME, ADDRESS)
    checkRc(LIB.mdb_dbi_open(txnPtr, name, flagsMask, dbiPtr))

    val ptr = dbiPtr.getPointer(0)

    ptr
  }
  // TODO: implement me (never used in lmdb4java)
  //def mdb_dbi_flags(@In txn: Pointer, @In dbi: Pointer, flags: Int): Int

  def mdb_del(txnPtr: Pointer, dbiPtr: Pointer, key: Pointer, data: Pointer): Boolean = {
    val rc = LIB.mdb_del(txnPtr, dbiPtr, key, data)
    if (rc == ResultCode.MDB_NOTFOUND) return false

    checkRc(rc)
    true
  }

  def mdb_drop(txnPtr: Pointer, dbiPtr: Pointer, del: Int): Unit = checkRc(LIB.mdb_drop(txnPtr, dbiPtr, del))

  def mdb_env_close(envPtr: Pointer): Unit = LIB.mdb_env_close(envPtr)
  def mdb_env_copy2(envPtr: Pointer, path: java.io.File, flags: Int): Unit = {
    checkRc(LIB.mdb_env_copy2(envPtr, path.getAbsolutePath, flags))
  }
  def mdb_env_create(): Pointer = {
    val envPtr = new PointerByReference()
    checkRc(LIB.mdb_env_create(envPtr))

    envPtr.getValue
  }
  //def mdb_env_get_fd(@In env: Pointer, @In fd: Pointer): Int
  //def mdb_env_get_flags(@In env: Pointer, flags: Int): Int
  def mdb_env_get_maxkeysize(envPtr: Pointer): Int = LIB.mdb_env_get_maxkeysize(envPtr)
  //def mdb_env_get_maxreaders(@In env: Pointer, readers: Int): Int
  //def mdb_env_get_path(@In env: Pointer, path: String): Int
  def mdb_env_info(envPtr: Pointer): EnvInfo = {
    val info = new Library.MDB_envinfo(RUNTIME)
    checkRc(LIB.mdb_env_info(envPtr, info))

    val mapAddress = if (info.f0_me_mapaddr.get == null) 0
    else info.f0_me_mapaddr.get.address

    EnvInfo(
      mapAddress,
      info.f1_me_mapsize.longValue,
      info.f2_me_last_pgno.longValue,
      info.f3_me_last_txnid.longValue,
      info.f4_me_maxreaders.intValue,
      info.f5_me_numreaders.intValue
    )
  }
  def mdb_env_open(envPtr: Pointer, path: java.io.File, flags: Int, mode: Int): Unit = {
    checkRc(LIB.mdb_env_open(envPtr, path.getAbsolutePath, flags, mode))
  }
  //def mdb_env_set_flags(@In env: Pointer, flags: Int, onoff: Int): Int
  def mdb_env_set_mapsize(envPtr: Pointer, size: Long): Unit = checkRc(LIB.mdb_env_set_mapsize(envPtr, size))
  def mdb_env_set_maxdbs(envPtr: Pointer, dbs: Int): Unit = checkRc(LIB.mdb_env_set_maxdbs(envPtr, dbs))
  def mdb_env_set_maxreaders(envPtr: Pointer, readers: Int): Unit = checkRc(LIB.mdb_env_set_maxreaders(envPtr, readers))
  def mdb_env_stat(envPtr: Pointer): Stat = {
    val stat = new Library.MDB_stat(RUNTIME)
    checkRc(LIB.mdb_env_stat(envPtr, stat))
    Stat(
      stat.f0_ms_psize.intValue,
      stat.f1_ms_depth.intValue,
      stat.f2_ms_branch_pages.longValue,
      stat.f3_ms_leaf_pages.longValue,
      stat.f4_ms_overflow_pages.longValue,
      stat.f5_ms_entries.longValue
    )
  }
  def mdb_env_sync(envPtr: Pointer, f: Int): Unit = checkRc(LIB.mdb_env_sync(envPtr, f))

  def mdb_get(txnPtr: Pointer, dbiPtr: Pointer, key: Pointer, data: Pointer): Boolean = {
    val rc = LIB.mdb_get(txnPtr, dbiPtr, key, data)

    if (rc == ResultCode.MDB_NOTFOUND) false
    else {
      checkRc(rc)
      true
    }
  }
  def mdb_put(txnPtr: Pointer, dbiPtr: Pointer, key: Pointer, data: Pointer, flags: Int): Boolean = {
    val rc = LIB.mdb_put(txnPtr, dbiPtr, key, data, flags)

    isPutOK(rc, flags)
  }

  def mdb_set_compare[T >: Null](txn: Txn[T,Pointer], dbiPtr: Pointer, comparator: Comparator[T]): AnyRef = {
    require(comparator != null, "comparator is null")

    val kvFactory = txn.keyValFactory

    val ccb: Library.ComparatorCallback = new Library.ComparatorCallback {
      def compare(keyA: jnr.ffi.Pointer, keyB: jnr.ffi.Pointer): Int = {
        // TODO: call a direct implementation instead when Proxy details have been moved to JVM???
        kvFactory.compareKeys(keyA, keyB, comparator)
      }
    }

    checkRc(LIB.mdb_set_compare(txn.pointer, dbiPtr, ccb))

    ccb
  }

  def mdb_stat(txnPtr: Pointer, dbiPtr: Pointer): Stat = {
    val stat = new Library.MDB_stat(RUNTIME)
    checkRc(LIB.mdb_stat(txnPtr, dbiPtr, stat))
    Stat(
      stat.f0_ms_psize.intValue,
      stat.f1_ms_depth.intValue,
      stat.f2_ms_branch_pages.longValue,
      stat.f3_ms_leaf_pages.longValue,
      stat.f4_ms_overflow_pages.longValue,
      stat.f5_ms_entries.longValue
    )
  }

  def mdb_strerror(rc: Int): String = LIB.mdb_strerror(rc)

  /* --- Wrappers for Txn functions --- */
  def mdb_txn_abort(txn: Pointer): Unit = {
    //println("Txn is being aborted")
    LIB.mdb_txn_abort(txn)
  }
  def mdb_txn_begin(env: Pointer, txnParentPtr: Pointer, flags: Int): Pointer = {
    val txnPtr: jnr.ffi.Pointer = allocateDirect(RUNTIME, ADDRESS)
    //println(s"Txn is beginning (is parent null = ${txnParentPtr == null}, flags = $flags)")
    val rc = LIB.mdb_txn_begin(env, txnParentPtr, flags, txnPtr)
    checkRc(rc)
    //println("Txn has begun")
    txnPtr.getPointer(0)
  }
  def mdb_txn_commit(txn: Pointer): Unit = {
    //println("Txn is being committed")
    checkRc(LIB.mdb_txn_commit(txn))
  }
  //def mdb_txn_env(txn: Pointer): Pointer = LIB.mdb_txn_env(txn)
  def mdb_txn_id(txn: Pointer): Long = LIB.mdb_txn_id(txn)
  def mdb_txn_renew(txn: Pointer): Unit = checkRc(LIB.mdb_txn_renew(txn))
  def mdb_txn_reset(txn: Pointer): Unit = LIB.mdb_txn_reset(txn)

  def mdb_version(): Version = {
    val major = new IntByReference()
    val minor = new IntByReference()
    val patch = new IntByReference()
    LIB.mdb_version(major, minor, patch)
    Version(major.intValue, minor.intValue, patch.intValue)
  }

}