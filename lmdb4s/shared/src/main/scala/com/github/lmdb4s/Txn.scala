package com.github.lmdb4s

import java.util.Comparator

import Txn.State

object Txn {

  /**
   * Transaction must abort, has a child, or is invalid.
   */
  @SerialVersionUID(1L)
  final class BadException private[lmdb4s]() extends LmdbNativeException(ResultCode.MDB_BAD_TXN, "Transaction must abort, has a child, or is invalid")

  /**
   * Invalid reuse of reader locktable slot.
   */
  @SerialVersionUID(1L)
  final class BadReaderLockException private[lmdb4s]() extends LmdbNativeException(ResultCode.MDB_BAD_RSLOT, "Invalid reuse of reader locktable slot")

  /**
   * The proposed R-W transaction is incompatible with a R-O Env.
   */
  @SerialVersionUID(1L)
  class EnvIsReadOnly() extends LmdbException("Read-write Txn incompatible with read-only Env")

  /**
   * The proposed transaction is incompatible with its parent transaction.
   */
  @SerialVersionUID(1L)
  class IncompatibleParent() extends LmdbException("Transaction incompatible with its parent transaction")

  /**
   * Transaction is not in a READY state.
   */
  @SerialVersionUID(1L)
  final class NotReadyException() extends LmdbException("Transaction is not in ready state")

  /**
   * The current transaction has not been reset.
   */
  @SerialVersionUID(1L)
  class NotResetException() extends LmdbException("Transaction has not been reset")

  /**
   * The current transaction is not a read-only transaction.
   */
  @SerialVersionUID(1L)
  class ReadOnlyRequiredException() extends LmdbException("Not a read-only transaction")

  /**
   * The current transaction is not a read-write transaction.
   */
  @SerialVersionUID(1L)
  class ReadWriteRequiredException() extends LmdbException("Not a read-write transaction")

  /**
   * The current transaction has already been reset.
   */
  @SerialVersionUID(1L)
  class ResetException() extends LmdbException("Transaction has already been reset")

  /**
   * Transaction has too many dirty pages.
   */
  @SerialVersionUID(1L)
  final class TxFullException private[lmdb4s]() extends LmdbNativeException(ResultCode.MDB_TXN_FULL, "Transaction has too many dirty pages")

  /**
   * Transaction states.
   */
  private[lmdb4s] object State extends Enumeration {
    type State = Value
    val READY, DONE, RESET, RELEASED = Value
  }

}

/**
 * LMDB transaction.
 *
 * @tparam T buffer type
 */
final class Txn[T >: Null, P >: Null] private[lmdb4s](
  private val env: Env[T,P],
  private val parent: Txn[T,P],
  //private val proxy: BufferProxy[T, Env.LIB.AnyPointer],
  private val flags: TxnFlags.Flag*
)(private[lmdb4s] implicit val keyValFactory: IKeyValFactory[T, P]) extends ITxn[T] with AutoCloseable {

  require(keyValFactory != null, "keyValFactory is null")

  import State._

  private val LIB = keyValFactory.libraryWrapper
  private val keyVal = keyValFactory.createKeyVal()

  private val flagsMask: Int = MaskedFlag.mask(flags: _*)
  private val readOnly = {
    val isRO = MaskedFlag.isSet(flagsMask, TxnFlags.MDB_RDONLY_TXN)
    if (env.isReadOnly && !isRO) throw new Txn.EnvIsReadOnly()
    if (parent != null && parent.isReadOnly != isRO) throw new Txn.IncompatibleParent()
    isRO
  }

  private var state: State.Value = _
  private val txnPtr: P = {
    val txnParentPtr = if (parent == null) null else parent.txnPtr
    val ptr = LIB.mdb_txn_begin(env.pointer, txnParentPtr, flagsMask)
    state = READY
    ptr
  }

  /**
   * Aborts this transaction.
   */
  def abort(): Unit = {
    checkReady()
    state = DONE
    LIB.mdb_txn_abort(txnPtr)
  }

  /**
   * Closes this transaction by aborting if not already committed.
   *
   * <p>
   * Closing the transaction will invoke
   * BufferProxy#deallocate(java.lang.Object) for each read-only buffer
   * (ie the key and value).
   */
  override def close(): Unit = {
    if (state == RELEASED) return
    if (state == READY) LIB.mdb_txn_abort(txnPtr)
    keyVal.close()
    state = RELEASED
  }

  /**
   * Commits this transaction.
   */
  def commit(): Unit = {
    checkReady()
    state = DONE
    LIB.mdb_txn_commit(txnPtr)
  }

  /**
   * Return the transaction's ID.
   *
   * @return A transaction ID, valid if input is an active transaction
   */
  def getId: Long = LIB.mdb_txn_id(txnPtr)

  /**
   * Obtains this transaction's parent.
   *
   * @return the parent transaction (may be null)
   */
  def getParent: ITxn[T] = parent

  /**
   * Whether this transaction is read-only.
   *
   * @return if read-only
   */
  def isReadOnly: Boolean = readOnly

  /**
   * Fetch the buffer which holds a read-only view of the LMDB allocated memory.
   * Any use of this buffer must comply with the standard LMDB C "mdb_get"
   * contract (ie do not modify, do not attempt to release the memory, do not
   * use once the transaction or cursor closes, do not use after a write etc).
   *
   * @return the key buffer (never null)
   */
  def key: T = keyVal.key

  /**
   * Renews a read-only transaction previously released by #reset().
   */
  def renew(): Unit = {
    if (state != RESET) throw new Txn.NotResetException()
    state = DONE
    LIB.mdb_txn_renew(txnPtr)
    state = READY
  }

  /**
   * Aborts this read-only transaction and resets the transaction handle so it
   * can be reused upon calling #renew().
   */
  def reset(): Unit = {
    checkReadOnly()
    if ((state != READY) && (state != DONE)) throw new Txn.ResetException()
    state = RESET
    LIB.mdb_txn_reset(txnPtr)
  }

  /**
   * Fetch the buffer which holds a read-only view of the LMDB allocated memory.
   * Any use of this buffer must comply with the standard LMDB C "mdb_get"
   * contract (ie do not modify, do not attempt to release the memory, do not
   * use once the transaction or cursor closes, do not use after a write etc).
   *
   * @return the value buffer (never null)
   */
  def value: T = keyVal.`val`

  private[lmdb4s] def checkReadOnly(): Unit = {
    if (!readOnly) throw new Txn.ReadOnlyRequiredException()
  }

  private[lmdb4s] def checkReady(): Unit = {
    if (state != READY) throw new Txn.NotReadyException()
  }

  private[lmdb4s] def checkWritesAllowed(): Unit = {
    if (readOnly) throw new Txn.ReadWriteRequiredException()
  }

  //private[lmdb4s] def comparator: Comparator[T] = proxy
  private[lmdb4s] def comparator: Comparator[T] = keyValFactory.comparator()

  /**
   * Obtain the buffer proxy.
   *
   * @return proxy (never null)
   */
  //private[lmdb4s] def getProxy(): BufferProxy[T, Env.LIB.AnyPointer] = proxy

  /**
   * Return the state of the transaction.
   *
   * @return the state
   */
  private[lmdb4s] def getState(): State.Value = state

  private[lmdb4s] def kv = keyVal

  //private[lmdb4s] def newKeyVal() = proxy.createKeyVal()
  private[lmdb4s] def newKeyVal() = keyValFactory.createKeyVal()

  private[lmdb4s] def pointer = txnPtr
}

