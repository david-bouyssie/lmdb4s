package com.github.lmdb4s

trait IKeyVal[V,P] extends AutoCloseable {

  private[lmdb4s] def key: V

  private[lmdb4s] def getKeyAsBytes(): Array[Byte]

  private[lmdb4s] def keyIn(key: V): Unit

  private[lmdb4s] def keyOut: V

  private[lmdb4s] def pointerKey: P

  private[lmdb4s] def pointerVal: P

  private[lmdb4s] def `val`: V

  private[lmdb4s] def valIn(`val`: V): Unit

  private[lmdb4s] def valIn(size: Int): Unit

  /**
   * Prepares an array suitable for presentation as the data argument to a
   * <code>MDB_MULTIPLE</code> put.
   *
   * <p>
   * The returned array is equivalent of two <code>MDB_val</code>s as follows:
   *
   * <ul>
   * <li>ptrVal1.data = pointer to the data address of passed buffer</li>
   * <li>ptrVal1.size = size of each individual data element</li>
   * <li>ptrVal2.data = unused</li>
   * <li>ptrVal2.size = number of data elements (as passed to this method)</li>
   * </ul>
   *
   * @param val      a user-provided buffer with data elements (required)
   * @param elements number of data elements the user has provided
   * @return a properly-prepared pointer to an array for the operation
   */
  private[lmdb4s] def valInMulti(`val`: V, elements: Int): P

  private[lmdb4s] def valOut: V
}