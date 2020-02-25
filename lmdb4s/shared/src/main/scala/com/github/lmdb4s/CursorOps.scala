package com.github.lmdb4s

sealed abstract class CursorOpCode extends Enumeration {

  protected final def Op(value: Int): Op = new Op(value)
  class Op(value: Int) extends Val(value, null) {

    /**
     * Obtain the integer code for use by LMDB C API.
     *
     * @return the code
     */
    def getCode(): Int = this.value
  }

  final def withCode(mask: Int): Op = this.apply(mask).asInstanceOf[Op]
}

/**
 * Flags for use when performing a
 * Cursor#get(java.lang.Object, org.lmdbjava.GetOp).
 *
 * <p>
 * Unlike most other LMDB enums, this enum is not bit masked.
 */
object GetOp extends CursorOpCode {
  type Op = CursorOpCode#Op

  /**
   * Position at specified key.
   */
  val MDB_SET: Op = Op(15)

  /**
   * Position at specified key, return key + data.
   */
  val MDB_SET_KEY: Op = Op(16)

  /**
   * Position at first key greater than or equal to specified key.
   */
  val MDB_SET_RANGE: Op = Op(17)
}

/**
 * Flags for use when performing a Cursor#seek(org.lmdbjava.SeekOp).
 *
 * <p>
 * Unlike most other LMDB enums, this enum is not bit masked.
 */
object SeekOp extends CursorOpCode {
  type Op = CursorOpCode#Op

  /**
   * Position at first key/data item.
   */
  val MDB_FIRST: Op = Op(0)

  /**
   * Position at first data item of current key. Only for DbiFlags#MDB_DUPSORT.
   */
  val MDB_FIRST_DUP: Op = Op(1)

  /**
   * Position at key/data pair. Only for DbiFlags#MDB_DUPSORT.
   */
  val MDB_GET_BOTH: Op = Op(2)

  /**
   * position at key, nearest data. Only for DbiFlags#MDB_DUPSORT.
   */
  val MDB_GET_BOTH_RANGE: Op = Op(3)

  /**
   * Return key/data at current cursor position.
   */
  val MDB_GET_CURRENT: Op = Op(4)

  /**
   * Return key and up to a page of duplicate data items from current cursor
   * position. Move cursor to prepare for #MDB_NEXT_MULTIPLE. Only for DbiFlags#MDB_DUPSORT.
   */
  val MDB_GET_MULTIPLE: Op = Op(5)

  /**
   * Position at last key/data item.
   */
  val MDB_LAST: Op = Op(6)

  /**
   * Position at last data item of current key. Only for @link DbiFlags#MDB_DUPSORT.
   */
  val MDB_LAST_DUP: Op = Op(7)

  /**
   * Position at next data item.
   */
  val MDB_NEXT: Op = Op(8)

  /**
   * Position at next data item of current key. Only for DbiFlags#MDB_DUPSORT.
   */
  val MDB_NEXT_DUP: Op = Op(9)

  /**
   * Return key and up to a page of duplicate data items from next cursor
   * position. Move cursor to prepare for #MDB_NEXT_MULTIPLE. Only for DbiFlags#MDB_DUPSORT.
   */
  val MDB_NEXT_MULTIPLE: Op = Op(10)

  /**
   * Position at first data item of next key.
   */
  val MDB_NEXT_NODUP: Op = Op(11)

  /**
   * Position at previous data item.
   */
  val MDB_PREV: Op = Op(12)

  /**
   * Position at previous data item of current key. DbiFlags#MDB_DUPSORT.
   */
  val MDB_PREV_DUP: Op = Op(13)

  /**
   * Position at last data item of previous key.
   */
  val MDB_PREV_NODUP: Op = Op(14)
}
