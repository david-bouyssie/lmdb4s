package com.github.lmdb4s

/**
 * Superclass for all LmdbJava custom exceptions.
 * Constructs an instance with the provided detailed message and cause.
 *
 * @param errorMessage the detail message
 * @param errorCause   original cause
 */
@SerialVersionUID(1L)
class LmdbException(private val errorMessage: String, private val errorCause: Throwable = null) extends Exception(errorMessage, errorCause) {
  override def getCause(): Throwable = errorCause
}

/*
@SerialVersionUID(1L)
class LmdbException2(private val errorMessage: String, private val errorCause: Throwable = null) extends Exception(errorMessage) {
  try {
    if (errorCause != null)
      super.initCause(errorCause)
  } catch {
    case t: Throwable => System.err.println("Can't set Exception cause")
  }
}
 */

/*
/**
 * Superclass for all LmdbJava custom exceptions.
 * Constructs an instance with the provided detailed message.
 *
 * @param message the detail message
 */
@SerialVersionUID(1L)
class LmdbException3(private val errorMessage: String) extends Exception(errorMessage) {

  /**
   * Constructs an instance with the provided detailed message and cause.
   *
   * @param message the detail message
   * @param cause   original cause
   */
  def this(errorMessage: String, errorCause: Throwable) {
    this(errorMessage)

    require(errorCause != null, "errorCause is null")

    super.initCause(errorCause)
  }
}*/

/**
 * Constructs an instance with the provided detailed message.
 *
 * @param msg the detail message.
 * @param rc  the result code.
 */
@SerialVersionUID(1L)
class LmdbNativeException private[lmdb4s](
  /**
   * Result code returned by the LMDB C function.
   */
  private val rc: Int,
  private val msg: String
) extends LmdbException(s"$msg ($rc)") {

  /**
   * Obtain the LMDB C-side result code.
   *
   * @return the C-side result code
   */
  final def getResultCode(): Int = rc
}

/*
@SerialVersionUID(1L)
final class SystemException private[lmdb4s](private val rc: Int, private val message: String) extends LmdbNativeException(
  rc, "Platform error: " + message
)*/

/**
 * Superclass for all exceptions that originate from a native C call.
 */
@SerialVersionUID(1L)
object LmdbNativeException {

  /**
   * Exception raised from a system constant table lookup.
   */
  @SerialVersionUID(1L)
  final class SystemException private[lmdb4s](private val rc: Int, private val message: String) extends LmdbNativeException(
    rc, "System error: " + message
  )

  /**
   * Located page was wrong type.
   */
  @SerialVersionUID(1L)
  final class PageCorruptedException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_CORRUPTED, "located page was wrong type"
  )

  /**
   * Page has not enough space - internal error.
   */
  @SerialVersionUID(1L)
  final class PageFullException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_PAGE_FULL,
    "Page has not enough space - internal error"
  )

  /**
   * Requested page not found - this usually indicates corruption.
   */
  @SerialVersionUID(1L)
  final class PageNotFoundException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_PAGE_NOTFOUND,
    "Requested page not found - this usually indicates corruption"
  )

  /**
   * Update of meta page failed or environment had fatal error.
   */
  @SerialVersionUID(1L)
  final class PanicException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_PANIC,
    "Update of meta page failed or environment had fatal error"
  )

  /**
   * Too many TLS keys in use - Windows only.
   */
  @SerialVersionUID(1L)
  final class TlsFullException private[lmdb4s]() extends LmdbNativeException(
    ResultCode.MDB_TLS_FULL,
    "Too many TLS keys in use - Windows only"
  )

}