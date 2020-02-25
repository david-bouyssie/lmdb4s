package com.github.lmdb4s

import java.lang.Boolean.getBoolean
import java.lang.reflect.Field
import sun.misc.Unsafe

/**
 * Provides access to Unsafe.
 */
object UnsafeAccess {
  /**
   * Java system property name that can be set to disable unsafe.
   */
  val DISABLE_UNSAFE_PROP = "lmdbjava.disable.unsafe"

  /**
   * Indicates whether unsafe use is allowed.
   */
  val ALLOW_UNSAFE: Boolean = !getBoolean(DISABLE_UNSAFE_PROP)
  if (!ALLOW_UNSAFE) throw new LmdbException("Unsafe disabled by user")

  /**
   * Unsafe field name (used to reflectively obtain the unsafe instance).
   */
  private val FIELD_NAME_THE_UNSAFE = "theUnsafe"

  /**
   * The actual unsafe. Guaranteed to be non-null if this class can access
   * unsafe and #ALLOW_UNSAFE is true. In other words, this entire class
   * will fail to initialize if unsafe is unavailable. This avoids callers from
   * needing to deal with null checks.
   */
  private[lmdb4s] var UNSAFE: Unsafe = _

  try {
    val field: Field = classOf[Unsafe].getDeclaredField(FIELD_NAME_THE_UNSAFE)
    field.setAccessible(true)
    UNSAFE = field.get(null).asInstanceOf[Unsafe]
  } catch {
    case e@(_: NoSuchFieldException | _: SecurityException | _: IllegalArgumentException | _: IllegalAccessException) =>
      throw new LmdbException("Unsafe unavailable", e)
  }

}