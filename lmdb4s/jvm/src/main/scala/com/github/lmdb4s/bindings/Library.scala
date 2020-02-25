package com.github.lmdb4s.bindings

import java.io.File.createTempFile
import java.io.{File, IOException, InputStream, OutputStream}
import java.lang.Boolean.getBoolean
import java.lang.System.getProperty
import java.lang.Thread.currentThread
import java.nio.file.Files
import java.util.Locale.ENGLISH
import java.util.Objects.{nonNull, requireNonNull}

import com.github.lmdb4s.LmdbException
import jnr.ffi.LibraryLoader.create
import jnr.ffi.Pointer
import jnr.ffi.Runtime
import jnr.ffi.Runtime.getRuntime
import jnr.ffi.Struct
import jnr.ffi.annotations.Delegate
import jnr.ffi.annotations.In
import jnr.ffi.annotations.Out
import jnr.ffi.byref.IntByReference
import jnr.ffi.byref.NativeLongByReference
import jnr.ffi.byref.PointerByReference
import jnr.ffi.types.size_t


object Library {

  /**
   * Java system property name that can be set to disable automatic extraction
   * of the LMDB system library from the LmdbJava JAR. This may be desirable if
   * an operating system-provided LMDB system library is preferred (eg operating
   * system package management, vendor support, special compiler flags, security
   * auditing, profile guided optimization builds, faster startup time by
   * avoiding the library copy etc).
   */
  val DISABLE_EXTRACT_PROP: String = "lmdb4s.disable_jar_extract"
  /**
   * Java system property name that can be set to provide a custom path to a
   * external LMDB system library. If set, the system property
   * DISABLE_EXTRACT_PROP will be overridden.
   */
  val LMDB_NATIVE_LIB_PROP: String = "lmdb4s.jvm_native_lib"
  /**
   * Indicates whether automatic extraction of the LMDB system library is
   * permitted.
   */
  val SHOULD_EXTRACT: Boolean = !getBoolean(DISABLE_EXTRACT_PROP)

  /**
   * Indicates whether external LMDB system library is provided.
   */
  private[lmdb4s] val SHOULD_USE_LIB: Boolean = nonNull(getProperty(LMDB_NATIVE_LIB_PROP))

  private[lmdb4s] val LIB: Lmdb = {
    val arch: String = getProperty("os.arch")
    val arch64: Boolean = "x64" == arch || "amd64" == arch || "x86_64" == arch

    val os: String = getProperty("os.name")
    val linux: Boolean = os.toLowerCase(ENGLISH).startsWith("linux")
    val osx: Boolean = os.startsWith("Mac OS X")
    val windows: Boolean = os.startsWith("Windows")

    val libToLoad = if (SHOULD_USE_LIB) getProperty(LMDB_NATIVE_LIB_PROP)
    else if (SHOULD_EXTRACT && arch64 && linux) extract("org/lmdbjava/lmdbjava-native-linux-x86_64.so")
    else if (SHOULD_EXTRACT && arch64 && osx) extract("org/lmdbjava/lmdbjava-native-osx-x86_64.dylib")
    else if (SHOULD_EXTRACT && arch64 && windows) extract("org/lmdbjava/lmdbjava-native-windows-x86_64.dll")
    else "lmdb"

    create(classOf[Lmdb]).load(libToLoad)
  }

  private[lmdb4s] val RUNTIME: Runtime = getRuntime(LIB)

  //@SuppressWarnings(Array("PMD.AssignmentInOperand"))
  //@SuppressFBWarnings(Array("OBL_UNSATISFIED_OBLIGATION")) // Spotbugs issue #432
  private def extract(name: String): String = {
    val suffix: String = name.substring(name.lastIndexOf('.'))
    var file: File = null

    val path = try {
      file = createTempFile("lmdbjava-native-library-", suffix)
      file.deleteOnExit()
      val cl = currentThread.getContextClassLoader

      var in: InputStream = null
      var out: OutputStream = null
      try {
        in = cl.getResourceAsStream(name)
        out = Files.newOutputStream(file.toPath)
        requireNonNull(in, "Classpath resource not found")

        var bytes = 0
        val buffer = new Array[Byte](4096)
        while ( {bytes = in.read(buffer); bytes != -1} ) {
          out.write(buffer, 0, bytes)
        }
      } finally {
        if (in != null) in.close()
        if (out != null) out.close()
      }

      file.getAbsolutePath
    } catch {
      case e: IOException =>
        throw new LmdbException("Failed to extract " + name, e)
    }

    path
  }

  /**
   * Structure to wrap a native <code>MDB_envinfo</code>. Not for external use.
   */
  //@SuppressWarnings(Array(Array("checkstyle:typename", "checkstyle:visibilitymodifier", "checkstyle:membername")))
  final class MDB_envinfo private[lmdb4s](private val runtime: Runtime) extends Struct(runtime) {
    val f0_me_mapaddr = new Pointer()
    val f1_me_mapsize = new size_t()
    val f2_me_last_pgno = new size_t()
    val f3_me_last_txnid = new size_t()
    val f4_me_maxreaders = new u_int32_t()
    val f5_me_numreaders = new u_int32_t()
  }

  /**
   * Structure to wrap a native <code>MDB_stat</code>. Not for external use.
   */
  //@SuppressWarnings(Array(Array("checkstyle:typename", "checkstyle:visibilitymodifier", "checkstyle:membername")))
  final class MDB_stat private[lmdb4s](private val runtime: Runtime) extends Struct(runtime) {
    val f0_ms_psize = new u_int32_t()
    val f1_ms_depth = new u_int32_t()
    val f2_ms_branch_pages = new size_t()
    val f3_ms_leaf_pages = new size_t()
    val f4_ms_overflow_pages = new size_t()
    val f5_ms_entries = new size_t()
  }

  /**
   * Custom comparator callback used by <code>mdb_set_compare</code>.
   */
  trait ComparatorCallback {
    @Delegate def compare(@In keyA: Pointer, @In keyB: Pointer): Int
  }

  /**
   * JNR API for MDB-defined C functions. Not for external use.
   */
  //@SuppressWarnings(Array("checkstyle:methodname"))
  trait Lmdb {
    def mdb_cursor_close(@In cursor: Pointer): Unit

    def mdb_cursor_count(@In cursor: Pointer, countp: NativeLongByReference): Int

    def mdb_cursor_del(@In cursor: Pointer, flags: Int): Int

    def mdb_cursor_get(@In cursor: Pointer, k: Pointer, @Out v: Pointer, cursorOp: Int): Int

    def mdb_cursor_open(@In txn: Pointer, @In dbi: Pointer, cursorPtr: PointerByReference): Int

    def mdb_cursor_put(@In cursor: Pointer, @In key: Pointer, @In data: Pointer, flags: Int): Int

    def mdb_cursor_renew(@In txn: Pointer, @In cursor: Pointer): Int

    def mdb_dbi_close(@In env: Pointer, @In dbi: Pointer): Unit

    def mdb_dbi_flags(@In txn: Pointer, @In dbi: Pointer, flags: Int): Int

    def mdb_dbi_open(@In txn: Pointer, @In name: Array[Byte], flags: Int, @In dbiPtr: Pointer): Int

    def mdb_del(@In txn: Pointer, @In dbi: Pointer, @In key: Pointer, @In data: Pointer): Int

    def mdb_drop(@In txn: Pointer, @In dbi: Pointer, del: Int): Int

    def mdb_env_close(@In env: Pointer): Unit

    def mdb_env_copy2(@In env: Pointer, @In path: String, flags: Int): Int

    def mdb_env_create(envPtr: PointerByReference): Int

    def mdb_env_get_fd(@In env: Pointer, @In fd: Pointer): Int

    def mdb_env_get_flags(@In env: Pointer, flags: Int): Int

    def mdb_env_get_maxkeysize(@In env: Pointer): Int

    def mdb_env_get_maxreaders(@In env: Pointer, readers: Int): Int

    def mdb_env_get_path(@In env: Pointer, path: String): Int

    def mdb_env_info(@In env: Pointer, @Out info: MDB_envinfo): Int

    def mdb_env_open(@In env: Pointer, @In path: String, flags: Int, mode: Int): Int

    def mdb_env_set_flags(@In env: Pointer, flags: Int, onoff: Int): Int

    def mdb_env_set_mapsize(@In env: Pointer, @size_t size: Long): Int

    def mdb_env_set_maxdbs(@In env: Pointer, dbs: Int): Int

    def mdb_env_set_maxreaders(@In env: Pointer, readers: Int): Int

    def mdb_env_stat(@In env: Pointer, @Out stat: MDB_stat): Int

    def mdb_env_sync(@In env: Pointer, f: Int): Int

    def mdb_get(@In txn: Pointer, @In dbi: Pointer, @In key: Pointer, @Out data: Pointer): Int

    def mdb_put(@In txn: Pointer, @In dbi: Pointer, @In key: Pointer, @In data: Pointer, flags: Int): Int

    def mdb_reader_check(@In env: Pointer, dead: Int): Int

    def mdb_set_compare(@In txn: Pointer, @In dbi: Pointer, cb: ComparatorCallback): Int

    def mdb_stat(@In txn: Pointer, @In dbi: Pointer, @Out stat: MDB_stat): Int

    def mdb_strerror(rc: Int): String

    def mdb_txn_abort(@In txn: Pointer): Unit

    def mdb_txn_begin(@In env: Pointer, @In parentTx: Pointer, flags: Int, txPtr: Pointer): Int

    def mdb_txn_commit(@In txn: Pointer): Int

    def mdb_txn_env(@In txn: Pointer): Pointer

    def mdb_txn_id(@In txn: Pointer): Long

    def mdb_txn_renew(@In txn: Pointer): Int

    def mdb_txn_reset(@In txn: Pointer): Unit

    def mdb_version(major: IntByReference, minor: IntByReference, patch: IntByReference): Pointer
  }

}
