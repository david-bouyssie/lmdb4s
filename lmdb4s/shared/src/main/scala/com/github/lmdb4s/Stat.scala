package com.github.lmdb4s

/**
 * Statistics, as returned by Env#stat() and Dbi#stat(org.lmdbjava.Txn).
 */
final case class Stat private[lmdb4s](
  /**
   * Size of a database page. This is currently the same for all databases.
   */
  pageSize: Int,

  /**
   * Depth (height) of the B-tree.
   */
  depth: Int,

  /**
   * Number of internal (non-leaf) pages.
   */
  branchPages: Long,

  /**
   * Number of leaf pages.
   */
  leafPages: Long,

  /**
   * Number of overflow pages.
   */
  overflowPages: Long,

  /**
   * Number of data items.
   */
  entries: Long
) {
  override def toString(): String = {
    s"Stat(branchPages=$branchPages, depth=$depth, entries=$entries leafPages=$leafPages, overflowPages=$overflowPages, pageSize=$pageSize)"
  }
}