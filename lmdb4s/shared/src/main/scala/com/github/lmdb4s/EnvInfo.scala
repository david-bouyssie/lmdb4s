package com.github.lmdb4s

/**
 * Environment information, as returned by Env#info().
 */
case class EnvInfo private[lmdb4s](
  /**
   * Address of map, if fixed.
   */
  mapAddress: Long,

  /**
   * Size of the data memory map.
   */
  mapSize: Long,

  /**
   * ID of the last used page.
   */
  lastPageNumber: Long,

  /**
   * ID of the last committed transaction.
   */
  lastTransactionId: Long,

  /**
   * Max reader slots in the environment.
   */
  maxReaders: Int,

  /**
   * Max reader slots used in the environment.
   */
  numReaders: Int
) {
  override def toString(): String = {
    s"EnvInfo(lastPageNumber=$lastPageNumber, lastTransactionId=$lastTransactionId, mapAddress=$mapAddress, mapSize=$mapSize, maxReaders=$maxReaders, numReaders=$numReaders)"
  }

  /*
  	pageSize := os.Getpagesize()

	println("Environment Info")
	println("  Map address: unkown",)
	println("  Map size:", mapSize)
	println("  Page size:", pageSize)
	println("  Max pages:", mapSize/pageSize)
	println("  Number of pages used:", lastPageNumber +1)
	println("  Last transaction ID:", lastTransactionId)
	println("  Max readers:", maxReaders)
	println("  Number of readers used:", numReaders)

   */
}

