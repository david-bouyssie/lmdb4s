package com.github.lmdb4s

import bindings._
import Env.SHOULD_CHECK

object Cursor {

  /**
   * Cursor has already been closed.
   */
  @SerialVersionUID(1L)
  final class ClosedException() extends LmdbException("Cursor has already been closed")

  /**
   * Cursor stack too deep - internal error.
   */
  @SerialVersionUID(1L)
  final class FullException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_CURSOR_FULL, "Cursor stack too deep - internal error"
  )

}

/**
 * A cursor handle.
 *
 * @tparam T buffer type
 */
final class Cursor[T >: Null, P >: Null] private[lmdb4s](
  private val ptrCursor: P,
  private var txn: Txn[T,P]
)(private implicit val keyValFactory: IKeyValFactory[T, P]) extends AutoCloseable {
  require(ptrCursor != null, "ptrCursor is null")
  require(txn != null, "txn is null")
  require(keyValFactory != null, "keyValFactory is null")

  import SeekOp._

  private val LIB = keyValFactory.libraryWrapper
  private val kv = txn.newKeyVal()
  private var closed = false

  /**
   * Close a cursor handle.
   *
   * <p>
   * The cursor handle will be freed and must not be used again after this call.
   * Its transaction must still be live if it is a write-transaction.
   */
  override def close(): Unit = {
    if (closed) return
    if (SHOULD_CHECK && !txn.isReadOnly) txn.checkReady()
    LIB.mdb_cursor_close(ptrCursor.asInstanceOf[LIB.AnyPointer])
    kv.close()
    closed = true
  }

  /**
   * Return count of duplicates for current key.
   *
   * <p>
   * This call is only valid on databases that support sorted duplicate data
   * items DbiFlags#MDB_DUPSORT.
   *
   * @return count of duplicates for current key
   */
  def count(): Long = {
    if (SHOULD_CHECK) {
      checkNotClosed()
      txn.checkReady()
    }
    LIB.mdb_cursor_count(ptrCursor)
  }

  /**
   * Delete current key/data pair.
   *
   * <p>
   * This function deletes the key/data pair to which the cursor refers.
   *
   * @param f flags (either null or { @link PutFlags#MDB_NODUPDATA}
   */
  def delete(f: PutFlags.Flag*): Unit = {
    if (SHOULD_CHECK) {
      checkNotClosed()
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    val flags = MaskedFlag.mask(f: _*)
    LIB.mdb_cursor_del(ptrCursor, flags)
  }

  /**
   * Position at first key/data item.
   *
   * @return false if requested position not found
   */
  def first(): Boolean = seek(MDB_FIRST)

  /**
   * Reposition the key/value buffers based on the passed key and operation.
   *
   * @param key to search for
   * @param op  options for this operation
   * @return false if key not found
   */
  def get(key: T, op: GetOp.Op): Boolean = {
    if (SHOULD_CHECK) {
      require(key != null, "key is null")
      require(op != null, "op is null")
      checkNotClosed()
      txn.checkReady()
    }
    kv.keyIn(key)

    val found = LIB.mdb_cursor_get(ptrCursor, kv.pointerKey, kv.pointerVal, op.getCode())
    if (!found) return false

    kv.keyOut
    kv.valOut

    true
  }

  /**
   * Obtain the key.
   *
   * @return the key that the cursor is located at.
   */
  def key: T = kv.key

  def getKeyAsBytes(): Array[Byte] = kv.getKeyAsBytes()

  /**
   * Position at last key/data item.
   *
   * @return false if requested position not found
   */
  def last(): Boolean = seek(MDB_LAST)

  /**
   * Position at next data item.
   *
   * @return false if requested position not found
   */
  def next(): Boolean = seek(MDB_NEXT)

  /**
   * Position at previous data item.
   *
   * @return false if requested position not found
   */
  def prev(): Boolean = seek(MDB_PREV)

  /**
   * Store by cursor.
   *
   * <p>
   * This function stores key/data pairs into the database.
   *
   * @param key key to store
   * @param val data to store
   * @param op  options for this operation
   * @return true if the value was put, false if MDB_NOOVERWRITE or
   *         MDB_NODUPDATA were set and the key/value existed already.
   */
  def put(key: T, `val`: T, op: PutFlags.Flag*): Boolean = {
    if (SHOULD_CHECK) {
      require(key != null, "key is null")
      require(`val` != null, "val is null")
      checkNotClosed()
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    kv.keyIn(key)
    kv.valIn(`val`)

    val mask = MaskedFlag.mask(op: _*)

    var isPut = false
    try {
      isPut = LIB.mdb_cursor_put(ptrCursor, kv.pointerKey, kv.pointerVal, mask)
    } finally {
      if (!isPut && MaskedFlag.isSet(mask, PutFlags.MDB_NOOVERWRITE)) kv.valOut // marked as in,out in LMDB C docs
    }

    isPut
  }

  /**
   * Put multiple values into the database in one <code>MDB_MULTIPLE</code>
   * operation.
   *
   * <p>
   * The database must have been opened with {@link DbiFlags#MDB_DUPFIXED}. The
   * buffer must contain fixed-sized values to be inserted. The size of each
   * element is calculated from the buffer's size divided by the given element
   * count. For example, to populate 10 X 4 byte integers at once, present a
   * buffer of 40 bytes and specify the element as 10.
   *
   * @param key      key to store in the database (not null)
   * @param val      value to store in the database (not null)
   * @param elements number of elements contained in the passed value buffer
   * @param op       options for operation (must set <code>MDB_MULTIPLE</code>)
   */
  def putMultiple(key: T, `val`: T, elements: Int, op: PutFlags.Flag*): Unit = {
    if (SHOULD_CHECK) {
      require(txn != null, "txn is null")
      require(key != null, "key is null")
      require(`val` != null, "val is null")
      txn.checkReady()
      txn.checkWritesAllowed()
    }

    val mask = MaskedFlag.mask(op: _*)
    if (SHOULD_CHECK && !MaskedFlag.isSet(mask, PutFlags.MDB_MULTIPLE))
      throw new IllegalArgumentException(s"Must set ${PutFlags.MDB_MULTIPLE} flag")

    txn.kv.keyIn(key)

    val dataPtr = txn.kv.valInMulti(`val`, elements)
    LIB.mdb_cursor_put(ptrCursor, txn.kv.pointerKey, dataPtr, mask)
  }

  /**
   * Renew a cursor handle.
   *
   * <p>
   * A cursor is associated with a specific transaction and database. Cursors
   * that are only used in read-only transactions may be re-used, to avoid
   * unnecessary malloc/free overhead. The cursor may be associated with a new
   * read-only transaction, and referencing the same database handle as it was
   * created with. This may be done whether the previous transaction is live or
   * dead.
   *
   * @param newTxn transaction handle
   */
  def renew(newTxn: Txn[T,P]): Unit = {
    if (SHOULD_CHECK) {
      require(newTxn != null, "newTxn is null")
      checkNotClosed()
      this.txn.checkReadOnly() // existing

      newTxn.checkReadOnly()
      newTxn.checkReady()
    }
    LIB.mdb_cursor_renew(newTxn.pointer, ptrCursor)
    this.txn = newTxn
  }

  /**
   * Reserve space for data of the given size, but don't copy the given val.
   * Instead, return a pointer to the reserved space, which the caller can fill
   * in later - before the next update operation or the transaction ends. This
   * saves an extra memcpy if the data is being generated later. LMDB does
   * nothing else with this memory, the caller is expected to modify all of the
   * space requested.
   *
   * <p>
   * This flag must not be specified if the database was opened with MDB_DUPSORT
   *
   * @param key  key to store in the database (not null)
   * @param size size of the value to be stored in the database (not null)
   * @param op   options for this operation
   * @return a buffer that can be used to modify the value
   */
  def reserve(key: T, size: Int, op: PutFlags.Flag*): T = {
    if (SHOULD_CHECK) {
      require(key != null, "key is null")
      checkNotClosed()
      txn.checkReady()
      txn.checkWritesAllowed()
    }
    kv.keyIn(key)
    kv.valIn(size)

    val flags = MaskedFlag.mask(op: _*) | PutFlags.MDB_RESERVE.getMask

    LIB.mdb_cursor_put(ptrCursor, kv.pointerKey, kv.pointerVal, flags)

    kv.valOut

    `val`
  }

  /**
   * Reposition the key/value buffers based on the passed operation.
   *
   * @param op options for this operation
   * @return false if requested position not found
   */
  def seek(op: SeekOp.Op): Boolean = {
    if (SHOULD_CHECK) {
      require(op != null, "op is null")
      checkNotClosed()
      txn.checkReady()
    }

    val found = LIB.mdb_cursor_get(ptrCursor, kv.pointerKey, kv.pointerVal, op.getCode())
    if (!found) return false

    kv.keyOut
    kv.valOut

    true
  }

  /**
   * Obtain the value.
   *
   * @return the value that the cursor is located at.
   */
  def `val`: T = kv.`val`

  private def checkNotClosed(): Unit = {
    if (closed) throw new Cursor.ClosedException()
  }
}
