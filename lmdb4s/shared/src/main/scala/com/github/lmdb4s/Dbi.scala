package com.github.lmdb4s

import java.util.Arrays
import java.util.Comparator

import Env.SHOULD_CHECK

object Dbi {

  /**
   * The specified DBI was changed unexpectedly.
   */
  @SerialVersionUID(1L)
  final class BadDbiException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_BAD_DBI,
    "The specified DBI was changed unexpectedly"
  )

  /**
   * Unsupported size of key/DB name/data, or wrong DUPFIXED size.
   */
  @SerialVersionUID(1L)
  final class BadValueSizeException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_BAD_VALSIZE,
    "Unsupported size of key/DB name/data, or wrong DUPFIXED size"
  )

  /**
   * Environment maxdbs reached.
   */
  @SerialVersionUID(1L)
  final class DbFullException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_DBS_FULL,
    "Environment maxdbs reached"
  )

  /**
   * Operation and DB incompatible, or DB type changed.
   *
   * <p>
   * This can mean:
   * <ul>
   * <li>The operation expects an MDB_DUPSORT / MDB_DUPFIXED database.</li>
   * <li>Opening a named DB when the unnamed DB has MDB_DUPSORT /
   * MDB_INTEGERKEY.</li>
   * <li>Accessing a data record as a database, or vice versa.</li>
   * <li>The database was dropped and recreated with different flags.</li>
   * </ul>
   */
  @SerialVersionUID(1L)
  final class IncompatibleException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_INCOMPATIBLE,
    "Operation and DB incompatible, or DB type changed"
  )

  /**
   * Key/data pair already exists.
   */
  @SerialVersionUID(1L)
  final class KeyExistsException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_KEYEXIST,
    "key/data pair already exists"
  )

  /**
   * Key/data pair not found (EOF).
   */
  @SerialVersionUID(1L)
  final class KeyNotFoundException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_NOTFOUND,
    "key/data pair not found (EOF)"
  )

  /**
   * Database contents grew beyond environment mapsize.
   */
  @SerialVersionUID(1L)
  final class MapResizedException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_MAP_RESIZED,
    "Database contents grew beyond environment mapsize"
  )

}

/**
 * LMDB Database.
 *
 * @tparam T buffer type
 */
final class Dbi[T >: Null, P >: Null] private(
  private val env: Env[T,P],
  private val name: Array[Byte],
  private val dbiPtr: P
)(private implicit val keyValFactory: IKeyValFactory[T, P]) extends IDbi[T] {

  require(keyValFactory != null, "keyValFactory is null")

  private val LIB = keyValFactory.libraryWrapper
  private var compFunc: Option[Comparator[T]] = None // optional
  private var compFuncCB: AnyRef = _
  private var cleaned: Boolean = false

  private[lmdb4s] def this(
    env: Env[T,P],
    txn: Txn[T,P],
    name: Array[Byte],
    flags: Seq[DbiFlags.Flag]
  )(implicit keyValFactory: IKeyValFactory[T, P]) {
    this(
      env,
      // FIXME: DBO => why do we need to copy???
      if (name == null) null else java.util.Arrays.copyOf(name, name.length),
      keyValFactory.libraryWrapper.mdb_dbi_open(txn.pointer, name, flags)
    )
  }

  private[lmdb4s] def this(
    env: Env[T,P],
    txn: Txn[T,P],
    name: Array[Byte],
    comparator: Comparator[T],
    flags: Seq[DbiFlags.Flag]
  )(implicit keyValFactory: IKeyValFactory[T, P]) {
    this(env, txn, name, flags)

    require(comparator != null, "comparator is null")

    compFunc = Option(comparator)

    // Note: hold the CB in the Dbi instance to avoid NPE (See: https://github.com/lmdbjava/lmdbjava/issues/125)
    compFuncCB = keyValFactory.libraryWrapper.mdb_set_compare(txn, dbiPtr, comparator)
  }

  def pointer: P = dbiPtr

  /**
   * Close the database handle (normally unnecessary; use with caution).
   *
   * <p>
   * It is very rare that closing a database handle is useful. There are also
   * many warnings/restrictions if closing a database handle (refer to the LMDB
   * C documentation). As such this is non-routine usage and this class does not
   * track the open/closed state of the Dbi. Advanced users are expected
   * to have specific reasons for using this method and will manage their own state accordingly.
   */
  def close(): Unit = {
    clean()
    LIB.mdb_dbi_close(env.pointer, dbiPtr)
  }

  /**
   * Starts a new read-write transaction and deletes the key.
   *
   * @param key key to delete from the database (not null)
   * @return true if the key/data pair was found, false otherwise
   * @see #delete(org.lmdbjava.Txn, java.lang.Object, java.lang.Object)
   */
  def delete(key: T): Boolean = {
    var txn: ITxn[T] = null
    try {
      txn = env.txnWrite()
      val ret = delete(txn, key)
      txn.commit()
      ret
    } finally {
      if (txn != null) txn.close()
    }
  }

  /**
   * Deletes the key using the passed transaction.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to delete from the database (not null)
   * @return true if the key/data pair was found, false otherwise
   * @see #delete(org.lmdbjava.Txn, java.lang.Object, java.lang.Object)
   */
  def delete(txn: ITxn[T], key: T): Boolean = delete(txn, key, null)

  /**
   * Removes key/data pairs from the database.
   *
   * <p>
   * If the database does not support sorted duplicate data items
   * (DbiFlags#MDB_DUPSORT) the value parameter is ignored. If the
   * database supports sorted duplicates and the value parameter is null, all of
   * the duplicate data items for the key will be deleted. Otherwise, if the
   * data parameter is non-null only the matching data item will be deleted.
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   * @param key key to delete from the database (not null)
   * @param val value to delete from the database (null permitted)
   * @return true if the key/data pair was found, false otherwise
   */
  def delete(txn: ITxn[T], key: T, `val`: T): Boolean = {
    require(txn != null, "txn is null")
    require(key != null, "key is null")

    val typedTxn = txn.asInstanceOf[Txn[T,P]]

    if (SHOULD_CHECK) {
      typedTxn.checkReady()
      typedTxn.checkWritesAllowed()
    }
    typedTxn.kv.keyIn(key)

    val data = if (`val` == null) null
    else {
      typedTxn.kv.valIn(`val`)
      typedTxn.kv.pointerVal
    }

    LIB.mdb_del(typedTxn.pointer, dbiPtr, typedTxn.kv.pointerKey, data)
  }

  /**
   * Drops the data in this database, leaving the database open for further use.
   *
   * <p>
   * This method slightly differs from the LMDB C API in that it does not
   * provide support for also closing the DB handle. If closing the DB handle is
   * required, please see #close().
   *
   * @param txn transaction handle (not null; not committed; must be R-W)
   */
  def drop(txn: ITxn[T]): Unit = {
    drop(txn, delete = false)
  }

  /**
   * Drops the database. If delete is set to true, the database will be deleted
   * and handle will be closed. See #close() for implication of handle
   * close. Otherwise, only the data in this database will be dropped.
   *
   * @param txn    transaction handle (not null; not committed; must be R-W)
   * @param delete whether database should be deleted.
   */
  def drop(txn: ITxn[T], delete: Boolean): Unit = {
    require(txn != null, "txn is null")

    val typedTxn = txn.asInstanceOf[Txn[T,P]]

    if (SHOULD_CHECK) {
      typedTxn.checkReady()
      typedTxn.checkWritesAllowed()
    }

    if (delete) clean()
    val del = if (delete) 1 else 0

    LIB.mdb_drop(typedTxn.pointer, dbiPtr, del)
  }

  /**
   * Get items from a database, moving the Txn#val() to the value.
   *
   * <p>
   * This function retrieves key/data pairs from the database. The address and
   * length of the data associated with the specified \b key are returned in the
   * structure to which \b data refers. If the database supports duplicate keys
   * (org.lmdbjava.DbiFlags#MDB_DUPSORT) then the first data item for
   * the key will be returned. Retrieval of other items requires the use of
   * #mdb_cursor_get().
   *
   * @param txn transaction handle (not null; not committed)
   * @param key key to search for in the database (not null)
   * @return the data or null if not found
   */
  def get(txn: ITxn[T], key: T): T = {
    require(txn != null, "txn is null")
    require(key != null, "key is null")

    val typedTxn = txn.asInstanceOf[Txn[T,P]]
    if (SHOULD_CHECK) {
      typedTxn.checkReady()
    }

    typedTxn.kv.keyIn(key)

    val found = LIB.mdb_get(typedTxn.pointer, dbiPtr, typedTxn.kv.pointerKey, typedTxn.kv.pointerVal)

    if (!found) null else typedTxn.kv.valOut // marked as out in LMDB C docs
  }

  /**
   * Obtains the name of this database.
   *
   * @return the name (may be null)
   */
  def getName(): Array[Byte] = if (name == null) null else Arrays.copyOf(name, name.length)

  /**
   * Iterate the database from the first item and forwards.
   *
   * @param txn transaction handle (not null; not committed)
   * @return iterator
   */
  def iterate(txn: ITxn[T]): ICursorIterator[T] = iterate(txn, KeyRange.all)

  /*

  / **
   * Iterate the database from the first/last item and forwards/backwards.
   *
   * @param txn  transaction handle (not null; not committed)
   * @param type direction of iterator (not null)
   * @return iterator (never null)
   * @deprecated use iterate method with a { @link KeyRange} instead
   * /
  @deprecated def iterate(txn: Txn[T], `type`: CursorIterator.IteratorType): CursorIterator[T] = {
    if (SHOULD_CHECK) requireNonNull(`type`)
    val range = if (`type` eq FORWARD) all
    else allBackward
    iterate(txn, range)
  }

  / **
   * Iterate the database from the first/last item and forwards/backwards by
   * first seeking to the provided key.
   *
   * @param txn  transaction handle (not null; not committed)
   * @param key  the key to search from (may be null to denote first record)
   * @param type direction of iterator (not null)
   * @return iterator (never null)
   * @deprecated use iterate method with a { @link KeyRange} instead
   * /
  @deprecated def iterate(txn: Txn[T], key: T, `type`: CursorIterator.IteratorType): CursorIterator[T] = {
    if (SHOULD_CHECK) requireNonNull(`type`)
    var range = null
    if (`type` eq FORWARD) range = if (key == null) all
    else atLeast(key)
    else range = if (key == null) allBackward
    else atLeastBackward(key)
    iterate(txn, range)
  }
  */

  /**
   * Iterate the database in accordance with the provided KeyRange and default Comparator.
   *
   * @param txn   transaction handle (not null; not committed)
   * @param range range of acceptable keys (not null)
   * @return iterator (never null)
   */
  def iterate(txn: ITxn[T], range: KeyRange[T]): ICursorIterator[T] = iterate(txn, range, null)

  /**
   * Iterate the database in accordance with the provided KeyRange and Comparator.
   *
   * <p>
   * If a comparator is provided, it must reflect the same ordering as LMDB uses
   * for cursor operations (eg first, next, last, previous etc).
   *
   * <p>
   * If a null comparator is provided, any comparator provided when opening the
   * database is used. If no database comparator was specified, the buffer's
   * default comparator is used. Such buffer comparators reflect LMDB's default
   * lexicographical order.
   *
   * @param txn        transaction handle (not null; not committed)
   * @param range      range of acceptable keys (not null)
   * @param comparator custom comparator for keys (may be null)
   * @return iterator (never null)
   */
  def iterate(txn: ITxn[T], range: KeyRange[T], comparator: Comparator[T]): ICursorIterator[T] = {
    require(txn != null, "txn is null")
    require(range != null, "range is null")

    val typedTxn = txn.asInstanceOf[Txn[T,P]]
    if (SHOULD_CHECK) {
      typedTxn.checkReady()
    }

    val useComp = if (comparator != null) comparator else compFunc.getOrElse(typedTxn.comparator)

    new CursorIterator[T,P](typedTxn, this, range, useComp)
  }

  /**
   * Create a cursor handle.
   *
   * <p>
   * A cursor is associated with a specific transaction and database. A cursor
   * cannot be used when its database handle is closed. Nor when its transaction
   * has ended, except with Cursor#renew(org.lmdbjava.Txn). It can be
   * discarded with Cursor#close(). A cursor in a write-transaction can
   * be closed before its transaction ends, and will otherwise be closed when
   * its transaction ends. A cursor in a read-only transaction must be closed
   * explicitly, before or after its transaction ends. It can be reused with
   * Cursor#renew(org.lmdbjava.Txn) before finally closing it.
   *
   * @param txn transaction handle (not null; not committed)
   * @return cursor handle
   */
  def openCursor(txn: ITxn[T]): ICursor[T] = {
    require(txn != null, "txn is null")

    val typedTxn = txn.asInstanceOf[Txn[T,P]]

    if (SHOULD_CHECK) {
      typedTxn.checkReady()
    }

    val cursorPtr = LIB.mdb_cursor_open(typedTxn.pointer, dbiPtr)
    new Cursor[T,P](cursorPtr, typedTxn)
  }

  /**
   * Starts a new read-write transaction and puts the key/data pair.
   *
   * @param key key to store in the database (not null)
   * @param val value to store in the database (not null)
   * @see #put(org.lmdbjava.Txn, java.lang.Object, java.lang.Object,
   * org.lmdbjava.PutFlags...)
   */
  def put(key: T, `val`: T): Unit = {

    var txn: ITxn[T] = null
    try {
      txn = env.txnWrite()
      put(txn, key, `val`)
      txn.commit()
    } finally {
      if (txn != null) txn.close()
    }
  }

  /**
   * Store a key/value pair in the database.
   *
   * <p>
   * This function stores key/data pairs in the database. The default behavior
   * is to enter the new key/data pair, replacing any previously existing key if
   * duplicates are disallowed, or adding a duplicate data item if duplicates
   * are allowed (DbiFlags#MDB_DUPSORT).
   *
   * @param txn   transaction handle (not null; not committed; must be R-W)
   * @param key   key to store in the database (not null)
   * @param val   value to store in the database (not null)
   * @param flags Special options for this operation
   * @return true if the value was put, false if MDB_NOOVERWRITE or
   *         MDB_NODUPDATA were set and the key/value existed already.
   */
  def put(txn: ITxn[T], key: T, `val`: T, flags: PutFlags.Flag*): Boolean = {
    require(txn != null, "txn is null")
    require(key != null, "key is null")
    require(`val` != null, "val is null")

    val typedTxn = txn.asInstanceOf[Txn[T,P]]

    if (SHOULD_CHECK) {
      typedTxn.checkReady()
      typedTxn.checkWritesAllowed()
    }
    typedTxn.kv.keyIn(key)
    typedTxn.kv.valIn(`val`)

    val mask = MaskedFlag.mask(flags: _*)

    var isPut = false
    try {
      isPut = LIB.mdb_put(typedTxn.pointer, dbiPtr, typedTxn.kv.pointerKey, typedTxn.kv.pointerVal, mask)
    } finally {
      if (!isPut && MaskedFlag.isSet(mask, PutFlags.MDB_NOOVERWRITE)) typedTxn.kv.valOut // marked as in,out in LMDB C docs
    }

    isPut
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
   * @param txn  transaction handle (not null; not committed; must be R-W)
   * @param key  key to store in the database (not null)
   * @param size size of the value to be stored in the database
   * @param op   options for this operation
   * @return a buffer that can be used to modify the value
   */
  def reserve(txn: ITxn[T], key: T, size: Int, op: PutFlags.Flag*): T = {
    require(txn != null, "txn is null")
    require(key != null, "key is null")

    val typedTxn = txn.asInstanceOf[Txn[T,P]]

    if (SHOULD_CHECK) {
      typedTxn.checkReady()
      typedTxn.checkWritesAllowed()
    }

    typedTxn.kv.keyIn(key)
    typedTxn.kv.valIn(size)

    val flags = MaskedFlag.mask(op: _*) | PutFlags.MDB_RESERVE.getMask()

    LIB.mdb_put(typedTxn.pointer, dbiPtr, typedTxn.kv.pointerKey, typedTxn.kv.pointerVal, flags)

    typedTxn.kv.valOut

    typedTxn.value
  }

  /**
   * Return statistics about this database.
   *
   * @param txn transaction handle (not null; not committed)
   * @return an immutable statistics object.
   */
  def stat(txn: ITxn[T]): Stat = {
    require(txn != null, "txn is null")

    val typedTxn = txn.asInstanceOf[Txn[T,P]]

    if (SHOULD_CHECK) {
      typedTxn.checkReady()
    }

    LIB.mdb_stat(typedTxn.pointer, dbiPtr)
  }

  private def clean(): Unit = {
    if (cleaned) return
    cleaned = true
  }
}

