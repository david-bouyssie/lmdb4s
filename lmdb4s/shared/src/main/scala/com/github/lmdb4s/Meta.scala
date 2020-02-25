package com.github.lmdb4s

import bindings._

/**
 * LMDB metadata functions.
 */
object Meta {

  private[lmdb4s] val LIB: ILibraryWrapper[AnyRef] = LibraryWrapper.getWrapper().asInstanceOf[ILibraryWrapper[AnyRef]]

  /**
   * Fetches the LMDB error code description.
   *
   * <p>
   * End users should not need this method, as LmdbJava converts all LMDB
   * exceptions into a typed Java exception that incorporates the error code.
   * However it is provided here for verification and troubleshooting (eg if the
   * user wishes to see the original LMDB description of the error code, or
   * there is a newer library version etc).
   *
   * @param err the error code returned from LMDB
   * @return the description
   */
  def error(err: Int): String = LIB.mdb_strerror(err)

  /**
   * Obtains the LMDB C library version information.
   *
   * @return the version data
   */
  def version(): Meta.Version = LIB.mdb_version()

  /**
   * Immutable return value from #version().
   */
  final case class Version private[lmdb4s](
    /**
     * LMDC native library major version number.
     */
    major: Int,

    /**
     * LMDC native library patch version number.
     */
    minor: Int,

    patch: Int
  )

}