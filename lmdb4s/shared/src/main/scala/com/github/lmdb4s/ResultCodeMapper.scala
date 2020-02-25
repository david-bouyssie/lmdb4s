package com.github.lmdb4s

import bindings._

object ResultCode {
  val	MDB_SUCCESS = 0
  val MDB_KEYEXIST = -30799
  val MDB_NOTFOUND = -30798
  val MDB_PAGE_NOTFOUND = -30797
  val MDB_CORRUPTED = -30796
  val MDB_PANIC = -30795
  val MDB_VERSION_MISMATCH = -30794
  val MDB_INVALID = -30793
  val MDB_MAP_FULL = -30792
  val MDB_DBS_FULL = -30791
  val MDB_READERS_FULL = -30790
  val MDB_TLS_FULL = -30789
  val MDB_TXN_FULL = -30788
  val MDB_CURSOR_FULL = -30787
  val MDB_PAGE_FULL = -30786
  val MDB_MAP_RESIZED = -30785
  val MDB_INCOMPATIBLE = -30784
  val MDB_BAD_RSLOT = -30783
  val MDB_BAD_TXN = -30782
  val MDB_BAD_VALSIZE = -30781
  val MDB_BAD_DBI = -30780
}

/**
 * Maps a LMDB C result code to the equivalent Java exception.
 *
 * <p>
 * The immutable nature of all LMDB exceptions means the mapper internally
 * maintains a table of them.
 */
object ResultCodeMapper {

  private[lmdb4s] val LIB: ILibraryWrapper[AnyRef] = LibraryWrapper.getWrapper().asInstanceOf[ILibraryWrapper[AnyRef]]

  import ResultCode._

  /**
   * Checks the result code and raises an exception is not #MDB_SUCCESS.
   *
   * @param rc the LMDB result code
   */
  private[lmdb4s] def checkRc(rc: Int): Unit = {

    rc match {
      case MDB_SUCCESS => ()
      case MDB_BAD_DBI => throw new Dbi.BadDbiException()
      case MDB_BAD_RSLOT => throw new Txn.BadReaderLockException()
      case MDB_BAD_TXN => throw new Txn.BadException()
      case MDB_BAD_VALSIZE => throw new Dbi.BadValueSizeException()
      case MDB_CORRUPTED => throw new LmdbNativeException.PageCorruptedException()
      case MDB_CURSOR_FULL => throw new Cursor.FullException()
      case MDB_DBS_FULL => throw new Dbi.DbFullException()
      case MDB_INCOMPATIBLE => throw new Dbi.IncompatibleException()
      case MDB_INVALID => throw new Env.FileInvalidException()
      case MDB_KEYEXIST => throw new Dbi.KeyExistsException()
      case MDB_MAP_FULL => throw new Env.MapFullException()
      case MDB_MAP_RESIZED => throw new Dbi.MapResizedException()
      case MDB_NOTFOUND => throw new Dbi.KeyNotFoundException()
      case MDB_PAGE_FULL => throw new LmdbNativeException.PageFullException()
      case MDB_PAGE_NOTFOUND => throw new LmdbNativeException.PageNotFoundException()
      case MDB_PANIC => throw new LmdbNativeException.PanicException()
      case MDB_READERS_FULL => throw new Env.ReadersFullException()
      case MDB_TLS_FULL => throw new LmdbNativeException.TlsFullException()
      case MDB_TXN_FULL => throw new Txn.TxFullException()
      case MDB_VERSION_MISMATCH => throw new Env.VersionMismatchException()
      case _ => LIB.throwSystemException(rc)
    }
  }

}