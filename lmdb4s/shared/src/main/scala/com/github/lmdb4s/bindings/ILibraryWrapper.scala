package com.github.lmdb4s.bindings

import java.util.Comparator

import com.github.lmdb4s._
import com.github.lmdb4s.Meta.Version

trait ILibraryWrapper[P >: Null] {

  //type AnyPointer >: Null // TODO: rename me PointerType

  type AnyPointer = P

  def throwSystemException(rc: Int): Unit

  /* --- Wrappers for Cursor functions --- */
  def mdb_cursor_close(cursorPtr: AnyPointer): Unit
  def mdb_cursor_count(cursorPtr: AnyPointer): Long
  def mdb_cursor_del(cursorPtr: AnyPointer, flags: Int): Unit
  def mdb_cursor_get(cursorPtr: AnyPointer, k: AnyPointer, v: AnyPointer, cursorOp: Int): Boolean
  def mdb_cursor_open(txnPtr: AnyPointer, dbiPtr: AnyPointer): AnyPointer
  def mdb_cursor_put(cursorPtr: AnyPointer, key: AnyPointer, data: AnyPointer, flags: Int): Boolean
  def mdb_cursor_renew(txnPtr: AnyPointer, cursorPtr: AnyPointer): Unit

  /* --- Wrappers for Dbi functions --- */
  def mdb_dbi_close(envPtr: AnyPointer, dbiPtr: AnyPointer): Unit
  def mdb_dbi_open(txnPtr: AnyPointer, name: Array[Byte], flags: Seq[DbiFlags.Flag]): AnyPointer

  /* --- Wrappers for misc. functions --- */
  def mdb_del(txnPtr: AnyPointer, dbiPtr: AnyPointer, key: AnyPointer, data: AnyPointer): Boolean
  def mdb_drop(txnPtr: AnyPointer, dbiPtr: AnyPointer, del: Int): Unit

  /* --- Wrappers for Env functions --- */
  def mdb_env_close(envPtr: AnyPointer): Unit
  def mdb_env_copy2(envPtr: AnyPointer, path: java.io.File, flags: Int): Unit
  def mdb_env_create(): AnyPointer
  def mdb_env_get_maxkeysize(envPtr: AnyPointer): Int
  def mdb_env_info(envPtr: AnyPointer): EnvInfo
  def mdb_env_open(envPtr: AnyPointer, path: java.io.File, flags: Int, mode: Int): Unit
  def mdb_env_set_mapsize(envPtr: AnyPointer, size: Long): Unit
  def mdb_env_set_maxdbs(envPtr: AnyPointer, dbs: Int): Unit
  def mdb_env_set_maxreaders(envPtr: AnyPointer, readers: Int): Unit
  def mdb_env_stat(envPtr: AnyPointer): Stat
  def mdb_env_sync(envPtr: AnyPointer, f: Int): Unit

  /* --- Wrappers for GET/PUT functions --- */
  def mdb_get(txnPtr: AnyPointer, dbiPtr: AnyPointer, key: AnyPointer, data: AnyPointer): Boolean
  def mdb_put(txnPtr: AnyPointer, dbiPtr: AnyPointer, key: AnyPointer, data: AnyPointer, flags: Int): Boolean

  /* --- Wrappers for misc. functions --- */
  def mdb_set_compare[T >: Null](txn: Txn[T,AnyPointer], dbiPtr: AnyPointer, comparator: Comparator[T]): AnyRef // returns a comparator callback
  def mdb_stat(txnPtr: AnyPointer, dbiPtr: AnyPointer): Stat
  def mdb_strerror(rc: Int): String

  /* --- Wrappers for Txn functions --- */
  def mdb_txn_abort(txn: AnyPointer): Unit
  def mdb_txn_begin(env: AnyPointer, parent: AnyPointer, flags: Int): AnyPointer
  def mdb_txn_commit(txn: AnyPointer): Unit
  //def mdb_txn_env(txn: AnyPointer): Pointer
  def mdb_txn_id(txn: AnyPointer): Long
  def mdb_txn_renew(txn: AnyPointer): Unit
  def mdb_txn_reset(txn: AnyPointer): Unit

  /* --- Wrapper for version function --- */
  def mdb_version(): Version


  protected def isPutOK(rc: Int, flags: Int): Boolean = {
    if (rc != ResultCode.MDB_KEYEXIST) {
      ResultCodeMapper.checkRc(rc)
      true
    } else {
      if (!MaskedFlag.isSet(flags, PutFlags.MDB_NOOVERWRITE) && !MaskedFlag.isSet(flags, PutFlags.MDB_NODUPDATA))
        ResultCodeMapper.checkRc(rc)

      false
    }
  }

  /*
  def mdb_cursor_close(@In cursor: Pointer): Unit
  def mdb_cursor_count(@In cursor: Pointer, countp: NativeLongByReference): Int
  def mdb_cursor_del(@In cursor: Pointer, flags: Int): Int
  def mdb_cursor_get(@In cursor: Pointer, k: Pointer, @Out v: Pointer, cursorOp: Int): Int
  def mdb_cursor_open(@In txn: Pointer, @In dbi: Pointer, cursorPtr: PointerByReference): Int
  def mdb_cursor_put(@In cursor: Pointer, @In key: Pointer, @In data: Pointer, flags: Int): Int
  def mdb_cursor_renew(@In txn: Pointer, @In cursor: Pointer): Int
  def mdb_dbi_close(@In env: Pointer, @In dbi: Pointer): Unit
  //def mdb_dbi_flags(@In txn: Pointer, @In dbi: Pointer, flags: Int): Int
  def mdb_dbi_open(@In txn: Pointer, @In name: Array[Byte], flags: Int, @In dbiPtr: Pointer): Int
  def mdb_del(@In txn: Pointer, @In dbi: Pointer, @In key: Pointer, @In data: Pointer): Int
  def mdb_drop(@In txn: Pointer, @In dbi: Pointer, del: Int): Int
  def mdb_env_close(@In env: Pointer): Unit
  def mdb_env_copy2(@In env: Pointer, @In path: String, flags: Int): Int
  def mdb_env_create(envPtr: PointerByReference): Int
  //def mdb_env_get_fd(@In env: Pointer, @In fd: Pointer): Int
  //def mdb_env_get_flags(@In env: Pointer, flags: Int): Int
  def mdb_env_get_maxkeysize(@In env: Pointer): Int
  //def mdb_env_get_maxreaders(@In env: Pointer, readers: Int): Int
  //def mdb_env_get_path(@In env: Pointer, path: String): Int
  def mdb_env_info(@In env: Pointer, @Out info: Library.MDB_envinfo): Int
  def mdb_env_open(@In env: Pointer, @In path: String, flags: Int, mode: Int): Int
  //def mdb_env_set_flags(@In env: Pointer, flags: Int, onoff: Int): Int
  def mdb_env_set_mapsize(@In env: Pointer, @size_t size: Long): Int
  def mdb_env_set_maxdbs(@In env: Pointer, dbs: Int): Int
  def mdb_env_set_maxreaders(@In env: Pointer, readers: Int): Int
  def mdb_env_stat(@In env: Pointer, @Out stat: MDB_stat): Int
  def mdb_env_sync(@In env: Pointer, f: Int): Int
  def mdb_get(@In txn: Pointer, @In dbi: Pointer, @In key: Pointer, @Out data: Pointer): Int
  def mdb_put(@In txn: Pointer, @In dbi: Pointer, @In key: Pointer, @In data: Pointer, flags: Int): Int
  //def mdb_reader_check(@In env: Pointer, dead: Int): Int
  def mdb_set_compare(@In txn: Pointer, @In dbi: Pointer, cb: ComparatorCallback): Int
  def mdb_stat(@In txn: Pointer, @In dbi: Pointer, @Out stat: MDB_stat): Int
  def mdb_strerror(rc: Int): String
  def mdb_txn_abort(@In txn: Pointer): Unit
  def mdb_txn_begin(@In env: Pointer, @In parentTx: Pointer, flags: Int, txPtr: Pointer): Int
  def mdb_txn_commit(@In txn: Pointer): Int
  //def mdb_txn_env(@In txn: Pointer): Pointer
  def mdb_txn_id(@In txn: Pointer): Long
  def mdb_txn_renew(@In txn: Pointer): Int
  def mdb_txn_reset(@In txn: Pointer): Unit
  def mdb_version(major: IntByReference, minor: IntByReference, patch: IntByReference): Pointer*/
}
