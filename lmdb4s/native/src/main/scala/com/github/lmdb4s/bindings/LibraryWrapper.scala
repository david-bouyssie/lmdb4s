package com.github.lmdb4s.bindings

import java.util.Comparator

import scala.language.implicitConversions
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

import com.github.lmdb4s._
import Meta.Version
import ResultCodeMapper.checkRc

object LibraryWrapper {

  private lazy val singleton = new LibraryWrapper()
  def getWrapper(): ILibraryWrapper[Ptr[Byte]] = singleton

  implicit class struct_lmdb_env_info_ops(val p: Ptr[lmdb.struct_lmdb_env_info]) extends AnyVal {
    def mapaddr: Ptr[Byte] = p._1
    def mapaddr_=(value: Ptr[Byte]): Unit = { p._1 = value }
    def mapsize: CSize = p._2
    def mapsize_=(value: CSize): Unit = { p._2 = value }
    def last_pgno: CSize = p._3
    def last_pgno_=(value: CSize): Unit = { p._3 = value }
    def last_txnid: CSize = p._4
    def last_txnid_=(value: CSize): Unit = { p._4 = value }
    def maxreaders: UInt = p._5
    def maxreaders_=(value: UInt): Unit = { p._5 = value }
    def numreaders: UInt = p._6
    def numreaders_=(value: UInt): Unit = { p._6 = value }
  }

  implicit class struct_lmdb_stat_ops(val p: Ptr[lmdb.struct_lmdb_stat]) extends AnyVal {
    def psize: UInt = p._1
    def psize_=(value: UInt): Unit = { p._1 = value }
    def depth: UInt = p._2
    def depth_=(value: UInt): Unit = { p._2 = value }
    def branch_pages: CSize = p._3
    def branch_pages_=(value: CSize): Unit = { p._3 = value }
    def leaf_pages: CSize = p._4
    def leaf_pages_=(value: CSize): Unit = { p._4 = value }
    def overflow_pages: CSize = p._5
    def overflow_pages_=(value: CSize): Unit = { p._5 = value }
    def entries: CSize = p._6
    def entries_=(value: CSize): Unit = { p._6 = value }
  }
}

class LibraryWrapper private() extends ILibraryWrapper[Ptr[Byte]] {

  private[lmdb4s] val LIB = lmdb

  type Pointer = AnyPointer // TODO: remove this alias

  def throwSystemException(rc: Int): Unit = {
    require(rc < 134, s"Unknown system error code $rc") // FIXME: try to retrieve the MAX_ERRNO value

    val cstr = scala.scalanative.libc.string.strerror(rc)
    assert(cstr != null)

    val msg = fromCString(cstr)

    throw new LmdbNativeException.SystemException(rc, msg)

    ()
  }

  // TODO: do we keep this casting or do we replace KVPtr by Ptr[Byte]???
  @inline implicit private def castPointer(ptr: Pointer): LIB.KVPtr = ptr.asInstanceOf[LIB.KVPtr]
  @inline implicit private def pointer2uint(ptr: Pointer): UInt = _unwrapUInt(ptr)

  def mdb_cursor_close(cursor: Pointer): Unit = LIB.mdb_cursor_close(cursor)
  def mdb_cursor_count(cursor: Pointer): Long = {
    val countp = stackalloc[Long]
    checkRc(LIB.mdb_cursor_count(cursor, countp))
    !countp
  }
  def mdb_cursor_del(cursorPtr: Pointer, flags: Int): Unit = checkRc(LIB.mdb_cursor_del(cursorPtr, flags))
  def mdb_cursor_get(cursorPtr: Pointer, k: Pointer, v: Pointer, cursorOp: Int): Boolean = {
    val rc = LIB.mdb_cursor_get(cursorPtr, k, v, cursorOp)
    if (rc == ResultCode.MDB_NOTFOUND) return false
    checkRc(rc)
    true
  }
  def mdb_cursor_open(txnPtr: Pointer, dbiPtr: Pointer): Pointer = {
    val cursorPtr = stackalloc[Pointer]
    checkRc(LIB.mdb_cursor_open(txnPtr, dbiPtr, cursorPtr))
    !cursorPtr
  }
  def mdb_cursor_put(cursorPtr: Pointer, key: Pointer, data: Pointer, flags: Int): Boolean = {
    val rc = LIB.mdb_cursor_put(cursorPtr, key, data, flags)

    isPutOK(rc, flags)
  }
  def mdb_cursor_renew(txnPtr: Pointer, cursorPtr: Pointer): Unit = checkRc(LIB.mdb_cursor_renew(txnPtr, cursorPtr))

  /* --- Wrappers for Dbi functions --- */
  def mdb_dbi_close(envPtr: Pointer, dbiPtr: Pointer): Unit = LIB.mdb_dbi_close(envPtr, dbiPtr)
  def mdb_dbi_open(txnPtr: Pointer, name: Array[Byte], flags: Seq[DbiFlags.Flag]): Pointer = {

    val flagsMask = MaskedFlag.mask(flags: _*)

    val dbiPtr = stackalloc[UInt]

    Zone { implicit z =>
      val nameAsCStr = if (name == null) null else _bytesToCString(name)
      checkRc(LIB.mdb_dbi_open(txnPtr, nameAsCStr, flagsMask, dbiPtr))
    }

    _wrapUInt(!dbiPtr)
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
    val rc = Zone { implicit z =>
      LIB.mdb_env_copy2(envPtr, toCString(path.getAbsolutePath), flags)
    }
    checkRc(rc)
  }
  def mdb_env_create(): Pointer = {
    val envPtr = stackalloc[Pointer]
    checkRc(LIB.mdb_env_create(envPtr))

    !envPtr
  }
  //def mdb_env_get_fd(@In env: Pointer, @In fd: Pointer): Int
  //def mdb_env_get_flags(@In env: Pointer, flags: Int): Int
  def mdb_env_get_maxkeysize(envPtr: Pointer): Int = LIB.mdb_env_get_maxkeysize(envPtr)
  //def mdb_env_get_maxreaders(@In env: Pointer, readers: Int): Int
  //def mdb_env_get_path(@In env: Pointer, path: String): Int
  def mdb_env_info(envPtr: Pointer): EnvInfo = {
    val infoPtr = stackalloc[LIB.struct_lmdb_env_info]
    checkRc(LIB.mdb_env_info(envPtr, infoPtr))

    val info: LibraryWrapper.struct_lmdb_env_info_ops = infoPtr

    // FIXME: find a way to get the memory address as a Long value
    val mapAddress = 0L //if (info.mapaddr == null) 0L else info.mapaddr.address

    EnvInfo(
      mapAddress,
      info.mapsize.longValue,
      info.last_pgno.longValue,
      info.last_txnid.longValue,
      info.maxreaders.toInt,
      info.numreaders.toInt
    )
  }
  def mdb_env_open(envPtr: Pointer, path: java.io.File, flags: Int, mode: Int): Unit = {
    val rc = Zone { implicit z =>
      LIB.mdb_env_open(envPtr, toCString(path.getAbsolutePath), flags, mode)
    }
    checkRc(rc)
  }
  //def mdb_env_set_flags(@In env: Pointer, flags: Int, onoff: Int): Int
  def mdb_env_set_mapsize(envPtr: Pointer, size: Long): Unit = checkRc(LIB.mdb_env_set_mapsize(envPtr, size))
  def mdb_env_set_maxdbs(envPtr: Pointer, dbs: Int): Unit = checkRc(LIB.mdb_env_set_maxdbs(envPtr, dbs))
  def mdb_env_set_maxreaders(envPtr: Pointer, readers: Int): Unit = checkRc(LIB.mdb_env_set_maxreaders(envPtr, readers))
  def mdb_env_stat(envPtr: Pointer): Stat = {
    val statPtr = stackalloc[LIB.struct_lmdb_stat]
    checkRc(LIB.mdb_env_stat(envPtr, statPtr))

    val stat: LibraryWrapper.struct_lmdb_stat_ops = statPtr

    Stat(
      stat.psize.toInt,
      stat.depth.toInt,
      stat.branch_pages.longValue,
      stat.leaf_pages.longValue,
      stat.overflow_pages.longValue,
      stat.entries.longValue
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

  def mdb_set_compare[T >: Null](txn: Txn[T, Pointer], dbiPtr: Pointer, comparator: Comparator[T]): AnyRef = {

    val kvFactory = txn.keyValFactory

    val ccb = new CFuncPtr2[Ptr[Byte],Ptr[Byte],Int] {
      def apply(keyA: Ptr[Byte], keyB: Ptr[Byte]): Int = {
        kvFactory.compareKeys(keyA, keyB, comparator)
      }
    }

    checkRc(LIB.mdb_set_compare(txn.pointer, dbiPtr, ccb))

    ccb
  }

  def mdb_stat(txnPtr: Pointer, dbiPtr: Pointer): Stat = {
    val statPtr = stackalloc[LIB.struct_lmdb_stat]
    checkRc(LIB.mdb_stat(txnPtr, dbiPtr, statPtr))

    val stat: LibraryWrapper.struct_lmdb_stat_ops = statPtr

    Stat(
      stat.psize.toInt,
      stat.depth.toInt,
      stat.branch_pages.longValue,
      stat.leaf_pages.longValue,
      stat.overflow_pages.longValue,
      stat.entries.longValue
    )
  }

  def mdb_strerror(rc: Int): String = fromCString(LIB.mdb_strerror(rc))

  /* --- Wrappers for TXN functions --- */
  def mdb_txn_abort(txn: Pointer): Unit = {
    //println("Txn is being aborted")
    LIB.mdb_txn_abort(txn)
  }
  def mdb_txn_begin(env: Pointer, txnParentPtr: Pointer, flags: Int): Pointer = {
    val txnPtr = stackalloc[Pointer]
    //println(s"Txn is beginning (is parent null = ${txnParentPtr == null}, flags = $flags)")
    val rc = LIB.mdb_txn_begin(env, txnParentPtr, flags, txnPtr)
    checkRc(rc)
    //println("Txn has begun")
    !txnPtr
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
    val major = stackalloc[CInt]
    val minor = stackalloc[CInt]
    val patch = stackalloc[CInt]

    val versionAsStr: CString = lmdb.mdb_version(major, minor, patch)

    Version(!major, !minor, !patch)
  }

  private val uIntSize = sizeof[UInt].toInt
  private def _wrapUInt(uint: UInt): Ptr[Byte] = {
    val byteArray = scala.scalanative.runtime.ByteArray.alloc(uIntSize)
    val byteArrayPtr = byteArray.at(0)
    val uintPtr = byteArrayPtr.asInstanceOf[Ptr[UInt]]
    uintPtr.update(0, uint)

    byteArrayPtr
  }

  private def _unwrapUInt(wrappedUInt: Ptr[Byte]): UInt = {
    val uintPtr = wrappedUInt.asInstanceOf[Ptr[UInt]]
    uintPtr(0)
  }

  // TODO: put this version in sqlite4s/CUtils
  private def _bytesToCString(bytes: Array[Byte])(implicit z: Zone): CString = {
    if (bytes == null) return null

    val nBytes = bytes.length
    val cstr: Ptr[CChar] = z.alloc(nBytes + 1)

    /*var c = 0
    while (c < nBytes) {
      cstr.update(c, bytes(c))
      c += 1
    }
    cstr.update(c, 0.toByte) // NULL CHAR*/

    // TODO: find a way to cast bytes to a bytesPtr and then use CUtils.strcpy?
    val bytesPtr = bytes.asInstanceOf[scalanative.runtime.ByteArray].at(0)
    scalanative.libc.string.memcpy(cstr, bytesPtr, nBytes)
    cstr(nBytes) = 0.toByte // NULL CHAR

    cstr
  }

}