package com.github.lmdb4s

import java.util.Comparator

object CursorIterator {

  /**
   * Holder for a key and value pair.
   *
   * <p>
   * The same holder instance will always be returned for a given iterator.
   * The returned keys and values may change or point to different memory
   * locations following changes in the iterator, cursor or transaction.
   *
   * @tparam T buffer type
   */
  final class KeyVal[T] {
    private var k: T = _
    private var v: T = _

    /**
     * The key.
     *
     * @return key
     */
    def key: T = k

    /**
     * The value.
     *
     * @return value
     */
    def `val`: T = v

    private[lmdb4s] def setK(key: T): Unit = {
      this.k = key
    }

    private[lmdb4s] def setV(`val`: T): Unit = {
      this.v = `val`
    }
  }

  /**
   * Direction in terms of key ordering for CursorIterator.
   *
   * @deprecated use { @link KeyRange} instead
   */
  /*@deprecated
  object IteratorType extends Enumeration {
    type IteratorType = Value
    val

    /**
     * Move forward.
     */
    FORWARD,

    /**
     * Move backward.
     */
    BACKWARD = Value
  }*/

  /**
   * Represents the internal CursorIterator state.
   */
  private[lmdb4s] object State extends Enumeration {
    val REQUIRES_INITIAL_OP, REQUIRES_NEXT_OP, REQUIRES_ITERATOR_OP, RELEASED, TERMINATED = Value
  }

}

/**
 * Iterator that iterates over a Cursor as specified by a KeyRange.
 *
 * <p>
 * An instance will create and close its own cursor.
 *
 * <p>
 * If iterating over keys stored with DbiFlags#MDB_INTEGERKEY you must
 * provide a Java comparator when constructing the Dbi or this class. It
 * is more efficient to use a comparator only with this class, as this avoids
 * LMDB calling back into Java code to perform the integer key comparison.
 *
 * @tparam T buffer type
 */
final class CursorIterator[T >: Null,P >: Null] private[lmdb4s](
  private val txn: Txn[T,P],
  private val dbi: Dbi[T,P],
  private val range: KeyRange[T],
  private val comparator: Comparator[T]
) extends ICursorIterator[T] with java.util.Iterator[CursorIterator.KeyVal[T]] with AutoCloseable {

  import CursorIterator.State._

  private val cursor = dbi.openCursor(txn)
  private val entry = new CursorIterator.KeyVal[T]
  private var state = REQUIRES_INITIAL_OP

  override def close(): Unit = {
    cursor.close()
  }

  //@SuppressWarnings(Array("checkstyle:returncount"))
  override def hasNext: Boolean = {
    while (state != RELEASED && state != TERMINATED) update()
    state == RELEASED
  }

  /**
   * Obtain an iterator.
   *
   * @return an iterator
   */
  def iterable(): java.lang.Iterable[CursorIterator.KeyVal[T]] = {
    //() => CursorIterator.this //thisCursorIterator
    val thisCursorIterator = this
    new java.lang.Iterable[CursorIterator.KeyVal[T]] {
      override def iterator(): java.util.Iterator[CursorIterator.KeyVal[T]] = {
        thisCursorIterator
      }
    }
  }

  override def next: CursorIterator.KeyVal[T] = {
    if (!hasNext) throw new NoSuchElementException()
    state = REQUIRES_NEXT_OP
    entry
  }

  override def remove(): Unit = {
    cursor.delete()
  }

  //@SuppressWarnings(Array(Array("PMD.CyclomaticComplexity", "PMD.NullAssignment")))
  private def executeCursorOp(op: KeyRangeType.CursorOp.Value): Unit = {
    import KeyRangeType.CursorOp._

    val found = op match {
      case FIRST => cursor.first()
      case LAST => cursor.last()
      case NEXT => cursor.next()
      case PREV => cursor.prev()
      case GET_START_KEY => cursor.get(range.start, GetOp.MDB_SET_RANGE)
      case GET_START_KEY_BACKWARD => cursor.get(range.start, GetOp.MDB_SET_RANGE) || cursor.last
      case _ => throw new IllegalStateException("Unknown cursor operation")
    }

    entry.setK(if (found) cursor.key else null)
    entry.setV(if (found) cursor.`val` else null)
  }

  private def executeIteratorOp(): Unit = {
    import KeyRangeType.IteratorOp._

    val op = range.`type`.iteratorOp(range.start, range.stop, entry.key, comparator)
    state = op match {
      case CALL_NEXT_OP =>
        executeCursorOp(range.`type`.nextOp())
        REQUIRES_ITERATOR_OP
      case TERMINATE => TERMINATED
      case RELEASE => RELEASED
      case _ => throw new IllegalStateException("Unknown operation")
    }
  }

  private def update(): Unit = {
    import CursorIterator.State._

    state match {
      case REQUIRES_INITIAL_OP =>
        executeCursorOp(range.`type`.initialOp())
        state = REQUIRES_ITERATOR_OP
      case REQUIRES_NEXT_OP =>
        executeCursorOp(range.`type`.nextOp())
        state = REQUIRES_ITERATOR_OP
      case REQUIRES_ITERATOR_OP =>
        executeIteratorOp()
      case TERMINATED =>
      case _ => throw new IllegalStateException("Unknown state")
    }
  }
}

