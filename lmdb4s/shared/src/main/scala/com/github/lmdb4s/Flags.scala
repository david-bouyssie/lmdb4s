package com.github.lmdb4s

object MaskedFlag {

  /**
   * Fetch the integer mask for all presented flags.
   *
   * @param flags to mask (null or empty returns zero)
   * @return the integer mask for use in C
   */
  def mask(flags: MaskedFlag#Flag*): Int = {
    if (flags == null || flags.isEmpty) return 0

    var result = 0
    for (flag <- flags; if flag != null) {
      result |= flag.getMask
    }

    result
  }

  /**
   * Indicates whether the passed flag has the relevant masked flag high.
   *
   * @param flags to evaluate (usually produced by
   *              { @link #mask(org.lmdbjava.MaskedFlag...)}
   * @param test the flag being sought (required)
   * @return true if set.
   */
  def isSet(flags: Int, test: MaskedFlag#Flag): Boolean = {
    require(test != null, "test is null")
    (flags & test.getMask) == test.getMask
  }
}

/**
 * Indicates an enum that can provide integers for each of its values.
 */
sealed abstract class MaskedFlag extends Enumeration {

  protected final def Flag(value: Int): Flag = new Flag(value)
  class Flag(value: Int) extends Val(value, null) {

    /**
     * Obtains the integer value for this enum which can be included in a mask.
     *
     * @return the integer value for combination into a mask
     */
    def getMask(): Int = this.value
  }

  final def withMask(mask: Int): Flag = this.apply(mask).asInstanceOf[Flag]
}

/**
 * Flags for use when performing a @see [[Env#copy(java.io.File, org.lmdbjava.CopyFlags]].
 */
object CopyFlags extends MaskedFlag {
  type Flag = MaskedFlag#Flag

  /**
   * Compacting copy: Omit free space from copy, and renumber all pages
   * sequentially.
   */
  val MDB_CP_COMPACT: Flag = Flag(0x01)
}

/**
 * Flags for use when opening a @see [[Dbi]].
 */
object DbiFlags extends MaskedFlag {
  type Flag = MaskedFlag#Flag

  /**
   * Use reverse string keys.
   *
   * <p>
   * Keys are strings to be compared in reverse order, from the end of the
   * strings to the beginning. By default, keys are treated as strings and
   * compared from beginning to end.
   */
  val MDB_REVERSEKEY: Flag = Flag(0x02)
  /**
   * Use sorted duplicates.
   *
   * <p>
   * Duplicate keys may be used in the database. Or, from another perspective,
   * keys may have multiple data items, stored in sorted order. By default keys
   * must be unique and may have only a single data item.
   */
  val MDB_DUPSORT: Flag = Flag(0x04)
  /**
   * Numeric keys in native byte order: either unsigned int or size_t. The keys
   * must all be of the same size.
   */
  val MDB_INTEGERKEY: Flag = Flag(0x08)
  /**
   * With #MDB_DUPSORT, sorted dup items have fixed size.
   *
   * <p>
   * This flag may only be used in combination with #MDB_DUPSORT. This
   * option tells the library that the data items for this database are all the
   * same size, which allows further optimizations in storage and retrieval.
   * When all data items are the same size, the SeekOp#MDB_GET_MULTIPLE
   * and SeekOp#MDB_NEXT_MULTIPLE cursor operations may be used to
   * retrieve multiple items at once.
   */
  val MDB_DUPFIXED: Flag = Flag(0x10)
  /**
   * With #MDB_DUPSORT} dups are #MDB_INTEGERKEY-style integers.
   *
   * <p>
   * This option specifies that duplicate data items are binary integers,
   * similar to #MDB_INTEGERKEY keys.
   */
  val MDB_INTEGERDUP: Flag = Flag(0x20)
  /**
   * With #MDB_DUPSORT, use reverse string dups.
   *
   * <p>
   * This option specifies that duplicate data items should be compared as
   * strings in reverse order.
   */
  val MDB_REVERSEDUP: Flag = Flag(0x40)
  /**
   * Create the named database if it doesn't exist.
   *
   * <p>
   * This option is not allowed in a read-only transaction or a read-only
   * environment.
   */
  val MDB_CREATE: Flag = Flag(0x40000)

}

/**
 * Flags for use when opening the Env.
 */
object EnvFlags extends MaskedFlag {
  type Flag = MaskedFlag#Flag

  /**
   * Mmap at a fixed address (experimental).
   */
  val MDB_FIXEDMAP: Flag = Flag(0x01)
  /**
   * No environment directory.
   */
  val MDB_NOSUBDIR: Flag = Flag(0x4000)
  /**
   * Don't fsync after commit.
   */
  val MDB_NOSYNC: Flag = Flag(0x10000)
  /**
   * Read only.
   */
  val MDB_RDONLY_ENV: Flag = Flag(0x20000)
  /**
   * Don't fsync metapage after commit.
   */
  val MDB_NOMETASYNC: Flag = Flag(0x40000)
  /**
   * Use writable mmap.
   */
  val MDB_WRITEMAP: Flag = Flag(0x80000)
  /**
   * Use asynchronous msync when #MDB_WRITEMAP is used.
   */
  val MDB_MAPASYNC: Flag = Flag(0x100000)
  /**
   * Tie reader locktable slots to Txn objects instead of to threads.
   */
  val MDB_NOTLS: Flag = Flag(0x200000)
  /**
   * Don't do any locking, caller must manage their own locks.
   */
  val MDB_NOLOCK: Flag = Flag(0x400000)
  /**
   * Don't do readahead (no effect on Windows).
   */
  val MDB_NORDAHEAD: Flag = Flag(0x800000)
  /**
   * Don't initialize malloc'd memory before writing to datafile.
   */
  val MDB_NOMEMINIT: Flag = Flag(0x1000000)
}


/**
 * Flags for use when performing a "put".
 */
object PutFlags extends MaskedFlag {
  type Flag = MaskedFlag#Flag

  /**
   * For put: Don't write if the key already exists.
   */
  val MDB_NOOVERWRITE: Flag = Flag(0x10)
  /**
   * Only for #MDB_DUPSORT<br>
   * For put: don't write if the key and data pair already exist.<br>
   * For mdb_cursor_del: remove all duplicate data items.
   */
  val MDB_NODUPDATA: Flag = Flag(0x20)
  /**
   * For mdb_cursor_put: overwrite the current key/data pair.
   */
  val MDB_CURRENT: Flag = Flag(0x40)
  /**
   * For put: Just reserve space for data, don't copy it. Return a pointer to
   * the reserved space.
   */
  val MDB_RESERVE: Flag = Flag(0x10000)
  /**
   * Data is being appended, don't split full pages.
   */
  val MDB_APPEND: Flag = Flag(0x20000)
  /**
   * Duplicate data is being appended, don't split full pages.
   */
  val MDB_APPENDDUP: Flag = Flag(0x40000)
  /**
   * Store multiple data items in one call. Only for #MDB_DUPFIXED.
   */
  val MDB_MULTIPLE: Flag = Flag(0x80000)

}


/**
 * Flags for use when creating a Txn.
 */
object TxnFlags extends MaskedFlag {
  type Flag = MaskedFlag#Flag
  /**
   * Read only.
   */
  val MDB_RDONLY_TXN: Flag = Flag(0x20000)
}