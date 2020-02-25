package com.github.lmdb4s

import java.io.File
import java.lang.Boolean.getBoolean
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Comparator

import scala.collection.mutable.ArrayBuffer

object Env {

  //private[lmdb4s] val LIB: ILibraryWrapper[P] = LibraryWrapper.getWrapper()

  /**
   * Java system property name that can be set to disable optional checks.
   */
  val DISABLE_CHECKS_PROP = "lmdb4s.disable_checks"

  /**
   * Indicates whether optional checks should be applied in LmdbJava. Optional
   * checks are only disabled in critical paths (see package-level JavaDocs).
   * Non-critical paths have optional checks performed at all times, regardless
   * of this property.
   */
  val SHOULD_CHECK: Boolean = ! getBoolean(DISABLE_CHECKS_PROP)

  /**
   * Object has already been closed and the operation is therefore prohibited.
   */
  @SerialVersionUID(1L)
  final class AlreadyClosedException() extends LmdbException("Environment has already been closed")

  /**
   * Object has already been opened and the operation is therefore prohibited.
   */
  @SerialVersionUID(1L)
  final class AlreadyOpenException() extends LmdbException("Environment has already been opened")

  object Builder {
    private[lmdb4s] val MAX_READERS_DEFAULT = 126
  }

  /**
   * Builder for configuring and opening Env.
   *
   * @tparam T buffer type
   */
  final class Builder[T >: Null, P >: Null] private[lmdb4s](
    //val proxy: BufferProxy[T, Env.LIB.AnyPointer]
  )(private[lmdb4s] implicit val keyValFactory: IKeyValFactory[T, P]) {
    //require(proxy != null, "proxy is null")
    require(keyValFactory != null, "keyValFactory is null")

    private val LIB = keyValFactory.libraryWrapper
    private var mapSize: Long = 1024 * 1024
    private var maxDbs = 1
    private var maxReaders = Builder.MAX_READERS_DEFAULT
    private var opened = false

    /**
     * Opens the environment.
     *
     * @param path  file system destination
     * @param mode  Unix permissions to set on created files and semaphores (provided as a decimal representation of an octal value)
     * @param flags the flags for this new environment
     * @return an environment ready for use
     */
    //@SuppressWarnings(Array("PMD.AccessorClassGeneration"))
    def open(path: File, mode: Int, flags: EnvFlags.Flag*): Env[T,P] = {
      require(path != null, "path is null")

      if (opened) throw new Env.AlreadyOpenException()

      opened = true

      val ptr = LIB.mdb_env_create()

      try {
        LIB.mdb_env_set_mapsize(ptr, mapSize)
        LIB.mdb_env_set_maxdbs(ptr, maxDbs)
        LIB.mdb_env_set_maxreaders(ptr, maxReaders)

        val flagsMask = MaskedFlag.mask(flags: _*)
        val readOnly = MaskedFlag.isSet(flagsMask, EnvFlags.MDB_RDONLY_ENV)
        LIB.mdb_env_open(ptr, path, flagsMask, mode)

        new Env[T, P](ptr, readOnly)
      } catch {
        case e: LmdbNativeException =>
          LIB.mdb_env_close(ptr)
          throw e
      }
    }

    /**
     * Opens the environment.
     *
     * @param path  file system destination
     * @param mode  Unix permissions to set on created files and semaphores (provided as a String representation of an octal value)
     * @param flags the flags for this new environment
     * @return an environment ready for use
     */
    def open(path: File, mode: String, flags: EnvFlags.Flag*): Env[T,P] = open(path, Integer.parseInt(mode, 8), flags: _*)

    /**
     * Opens the environment with 0664 mode.
     *
     * @param path  file system destination
     * @param flags the flags for this new environment
     * @return an environment ready for use
     */
    //@SuppressWarnings(Array("PMD.AvoidUsingOctalValues"))
    def open(path: File, flags: EnvFlags.Flag*): Env[T,P] = open(path, "0664", flags: _*)

    /**
     * Sets the map size.
     *
     * @param mapSize new limit in bytes
     * @return the builder
     */
    def setMapSize(mapSize: Long): Env.Builder[T,P] = {
      if (opened) throw new Env.AlreadyOpenException()
      if (mapSize < 0) throw new IllegalArgumentException("Negative value; overflow?")
      this.mapSize = mapSize
      this
    }

    /**
     * Sets the maximum number of databases (ie Dbi) permitted.
     *
     * @param dbs new limit
     * @return the builder
     */
    def setMaxDbs(dbs: Int): Env.Builder[T,P] = {
      if (opened) throw new Env.AlreadyOpenException()
      this.maxDbs = dbs
      this
    }

    /**
     * Sets the maximum number of reader slots for the environment.
     *
     * @param readers new limit
     * @return the builder
     */
    def setMaxReaders(readers: Int): Env.Builder[T,P] = {
      if (opened) throw new Env.AlreadyOpenException()
      this.maxReaders = readers
      this
    }
  }

  /**
   * File is not a valid LMDB file.
   */
  @SerialVersionUID(1L)
  final class FileInvalidException private[lmdb4s]() extends LmdbNativeException(ResultCode.MDB_INVALID, "File is not a valid LMDB file")

  /**
   * The specified copy destination is invalid.
   */
  @SerialVersionUID(1L)
  final class InvalidCopyDestination(val message: String) extends LmdbException(message)

  /**
   * Environment mapsize reached.
   */
  @SerialVersionUID(1L)
  final class MapFullException private[lmdb4s]() extends LmdbNativeException(ResultCode.MDB_MAP_FULL, "Environment mapsize reached")

  /**
   * Environment maxreaders reached.
   */
  @SerialVersionUID(1L)
  final class ReadersFullException private[lmdb4s]() extends LmdbNativeException(ResultCode.MDB_READERS_FULL, "Environment maxreaders reached")

  /**
   * Environment version mismatch.
   */
  @SerialVersionUID(1L)
  final class VersionMismatchException private[lmdb4s]() extends LmdbNativeException(ResultCode.MDB_VERSION_MISMATCH, "Environment version mismatch")

}

/**
 * LMDB environment.
 *
 * @tparam T buffer type
 */
final class Env[T >: Null, P >: Null] private(
  //private val proxy: BufferProxy[T, Env.LIB.AnyPointer],
  private val envPtr: P,
  private val readOnly: Boolean
)(private implicit val keyValFactory: IKeyValFactory[T, P]) extends AutoCloseable { // cache max key size to avoid further JNI calls

  require(keyValFactory != null, "keyValFactory is null")

  private val LIB = keyValFactory.libraryWrapper

  private val maxKeySize = LIB.mdb_env_get_maxkeysize(envPtr)
  private var closed = false

  /**
   * Close the handle.
   *
   * <p>
   * Will silently return if already closed or never opened.
   */
  override def close(): Unit = {
    if (closed) return
    closed = true
    LIB.mdb_env_close(envPtr)
  }

  /**
   * Copies an LMDB environment to the specified destination path.
   *
   * <p>
   * This function may be used to make a backup of an existing environment. No
   * lockfile is created, since it gets recreated at need.
   *
   * <p>
   * Note: This call can trigger significant file size growth if run in parallel
   * with write transactions, because it employs a read-only transaction. See
   * long-lived transactions under "Caveats" in the LMDB native documentation.
   *
   * @param path  destination directory, which must exist, be writable and empty
   * @param flags special options for this copy
   */
  def copy(path: File, flags: CopyFlags.Flag*): Unit = {
    require(path != null, "path is null")
    if (!path.exists) throw new Env.InvalidCopyDestination("Path must exist")
    if (!path.isDirectory) throw new Env.InvalidCopyDestination("Path must be a directory")
    val files = path.list
    if (files != null && files.nonEmpty) throw new Env.InvalidCopyDestination("Path must contain no files")
    val flagsMask = MaskedFlag.mask(flags: _*)
    LIB.mdb_env_copy2(envPtr, path, flagsMask)
  }

  /**
   * Obtain the DBI names.
   *
   * <p>
   * This method is only compatible with Envs that use named databases.
   * If an unnamed Dbi is being used to store data, this method will
   * attempt to return all such keys from the unnamed database.
   *
   * @return a list of DBI names (never null)
   */
  def getDbiNames(): Seq[Array[Byte]] = {

    val anonymousDbi = openDbi(null: Array[Byte])

    var txn: Txn[T,P] = null
    var cursor: Cursor[T,P] = null
    try {
      txn = txnRead()
      cursor = anonymousDbi.openCursor(txn)

      if (!cursor.first) return Seq.empty[Array[Byte]]

      val result = new ArrayBuffer[Array[Byte]]

      do {
        val name = cursor.getKeyAsBytes()
        result += name
      } while (cursor.next())

      result

    } finally {
      if (txn != null) txn.close()
      if (cursor != null) cursor.close()
    }
  }

  def getDbiNames(charset: java.nio.charset.Charset): Seq[String] = { // use with StandardCharsets.UTF_8
    getDbiNames().map { bytes =>
      new String(bytes, charset)
    }
  }

  /**
   * Set the size of the data memory map.
   *
   * @param mapSize the new size, in bytes
   */
  def setMapSize(mapSize: Long): Unit = {
    LIB.mdb_env_set_mapsize(envPtr, mapSize)
  }

  /**
   * Get the maximum size of keys and MDB_DUPSORT data we can write.
   *
   * @return the maximum size of keys.
   */
  def getMaxKeySize(): Int = maxKeySize

  /**
   * Return information about this environment.
   *
   * @return an immutable information object.
   */
  def info(): EnvInfo = {
    if (closed) throw new Env.AlreadyClosedException()

    LIB.mdb_env_info(envPtr)
  }

  /**
   * Indicates whether this environment has been closed.
   *
   * @return true if closed
   */
  def isClosed: Boolean = closed

  /**
   * Indicates if this environment was opened with
   * EnvFlags#MDB_RDONLY_ENV.
   *
   * @return true if read-only
   */
  def isReadOnly: Boolean = readOnly

  /**
   * Convenience method that opens a Dbi with a UTF-8 database name.
   *
   * @param name  name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   */
  def openDbi(name: String, flags: DbiFlags.Flag*): Dbi[T,P] = {
    val nameBytes = if (name == null) null else name.getBytes(UTF_8)
    openDbi(nameBytes, flags: _*)
  }

  /**
   * Convenience method that opens a Dbi with a UTF-8 database name
   * and custom comparator.
   *
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use LMDB default)
   * @param flags      to open the database with
   * @return a database that is ready to use
   */
  def openDbi(name: String, comparator: Comparator[T], flags: DbiFlags.Flag*): Dbi[T,P] = {
    val nameBytes = if (name == null) null else name.getBytes(UTF_8)
    openDbi(nameBytes, comparator, flags: _*)
  }

  /**
   * Open the Dbi.
   *
   * @param name  name of the database (or null if no name is required)
   * @param flags to open the database with
   * @return a database that is ready to use
   */
  def openDbi(name: Array[Byte], flags: DbiFlags.Flag*): Dbi[T,P] = {
    var txn: Txn[T,P] = null

    try {
      txn = if (readOnly) txnRead() else txnWrite()
      val dbi = new Dbi[T,P](this, txn, name, flags)
      txn.commit() // even RO Txns require a commit to retain Dbi in Env
      dbi
    } finally {
      if (txn != null) txn.close()
    }
  }

  /**
   * Open the Dbi.
   *
   * <p>
   * If a custom comparator is specified, this comparator is called from LMDB
   * any time it needs to compare two keys. The comparator must be used any time
   * any time this database is opened, otherwise database corruption may occur.
   * The custom comparator will also be used whenever a CursorIterator
   * is created from the returned Dbi. If a custom comparator is not
   * specified, LMDB's native default lexicographical order is used. The default
   * comparator is typically more efficient (as there is no need for the native
   * library to call back into Java for the comparator result).
   *
   * @param name       name of the database (or null if no name is required)
   * @param comparator custom comparator callback (or null to use LMDB default)
   * @param flags      to open the database with
   * @return a database that is ready to use
   */
  def openDbi(name: Array[Byte], comparator: Comparator[T], flags: DbiFlags.Flag*): Dbi[T,P] = {
    var txn: Txn[T,P] = null

    try {
      txn = if (readOnly) txnRead() else txnWrite()
      val dbi = new Dbi[T,P](this: Env[T,P], txn, name, comparator, flags)
      txn.commit()
      dbi
    } finally {
      if (txn != null) txn.close()
    }
  }

  /**
   * Return statistics about this environment.
   *
   * @return an immutable statistics object.
   */
  def stat(): Stat = {
    if (closed) throw new Env.AlreadyClosedException()

    LIB.mdb_env_stat(envPtr)
  }

  /**
   * Flushes the data buffers to disk.
   *
   * @param force force a synchronous flush (otherwise if the environment has
   *              the MDB_NOSYNC flag set the flushes will be omitted, and with
   *              MDB_MAPASYNC they will be asynchronous)
   */
  def sync(force: Boolean): Unit = {
    if (closed) throw new Env.AlreadyClosedException()
    val f = if (force) 1 else 0
    LIB.mdb_env_sync(envPtr, f)
  }

  /**
   * Obtain a transaction with the requested parent and flags.
   *
   * @param parent parent transaction (may be null if no parent)
   * @param flags  applicable flags (eg for a reusable, read-only transaction)
   * @return a transaction (never null)
   */
  def txn(parent: Txn[T,P], flags: TxnFlags.Flag*): Txn[T,P] = {
    if (closed) throw new Env.AlreadyClosedException()

    new Txn[T,P](this, parent, flags: _*)
  }

  /**
   * Obtain a read-only transaction.
   *
   * @return a read-only transaction
   */
  def txnRead(): Txn[T,P] = txn(null, TxnFlags.MDB_RDONLY_TXN)

  /**
   * Obtain a read-write transaction.
   *
   * @return a read-write transaction
   */
  def txnWrite(): Txn[T,P] = txn(null)

  //private[lmdb4s]
  def pointer: P = envPtr
}

