package com.github.lmdb4s

import java.lang.Long.BYTES
import java.util.Comparator

/**
 * The strategy for mapping memory address to a given buffer type.
 *
 * <p>
 * The proxy is passed to the Env#create(org.lmdbjava.BufferProxy)
 * method and is subsequently used by every Txn, Dbi and
 * Cursor associated with the Env.
 *
 */
//@SuppressWarnings(Array("checkstyle:abstractclassname"))
private[lmdb4s] object BufferProxyLike {

  /**
   * Size of a <code>MDB_val</code> pointer in bytes.
   */
  val MDB_VAL_STRUCT_SIZE: Int = BYTES * 2

  /**
   * Offset from a pointer of the <code>MDB_val.mv_data</code> field.
   */
  val STRUCT_FIELD_OFFSET_DATA: Int = BYTES

  /**
   * Offset from a pointer of the <code>MDB_val.mv_size</code> field.
   */
  val STRUCT_FIELD_OFFSET_SIZE = 0
}

//@SuppressWarnings(Array("checkstyle:abstractclassname"))
trait BufferProxyLike[V,P] extends Comparator[V] {

  /**
   * Allocate a new buffer suitable for passing to
   * #out(java.lang.Object, jnr.ffi.Pointer, long).
   *
   * @return a buffer for passing to the <code>out</code> method
   */
  protected[lmdb4s] def allocate(): V

  /**
   * Compare the two buffers.
   *
   * <p>
   * Implemented as a protected method to discourage use of the buffer proxy
   * in collections etc (given by design it wraps a temporary value only).
   *
   * @param o1 left operand
   * @param o2 right operand
   * @return as per { @link Comparable}
   */
  protected[lmdb4s] def compare(o1: V, o2: V): Int

  /**
   * Deallocate a buffer that was previously provided by #allocate().
   *
   * @param buff the buffer to deallocate (required)
   */
  protected[lmdb4s] def deallocate(buff: V): Unit

  /**
   * Obtain a copy of the bytes contained within the passed buffer.
   *
   * @param buffer a non-null buffer created by this proxy instance
   * @return a copy of the bytes this buffer is currently representing
   */
  protected[lmdb4s] def getBytes(buffer: V): Array[Byte]

  /**
   * Called when the <code>MDB_val</code> should be set to reflect the passed buffer.
   * This buffer will have been created by end users, not #allocate().
   *
   * @param buffer  the buffer to write to <code>MDB_val</code>
   * @param ptr     the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   */
  protected[lmdb4s] def in(buffer: V, ptr: P, ptrAddr: Long): Unit

  /**
   * Called when the <code>MDB_val</code> should be set to reflect the passed buffer.
   *
   * @param buffer  the buffer to write to <code>MDB_val</code>
   * @param size    the buffer size to write to <code>MDB_val</code>
   * @param ptr     the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   */
  protected[lmdb4s] def in(buffer: V, size: Int, ptr: P, ptrAddr: Long): Unit

  /**
   * Called when the <code>MDB_val</code> may have changed and the passed buffer
   * should be modified to reflect the new <code>MDB_val</code>.
   *
   * @param buffer  the buffer to write to <code>MDB_val</code>
   * @param ptr     the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   * @return the buffer for <code>MDB_val</code>
   */
  protected[lmdb4s] def out(buffer: V, ptr: P, ptrAddr: Long): V

}

/*package com.github.lmdb4s

import java.lang.Long.BYTES
import java.util.Comparator

/**
 * The strategy for mapping memory address to a given buffer type.
 *
 * <p>
 * The proxy is passed to the Env#create(org.lmdbjava.BufferProxy)
 * method and is subsequently used by every Txn, Dbi and
 * Cursor associated with the Env.
 *
 */
//@SuppressWarnings(Array("checkstyle:abstractclassname"))
private[lmdb4s] object BufferProxy {

  /**
   * Size of a <code>MDB_val</code> pointer in bytes.
   */
  val MDB_VAL_STRUCT_SIZE: Int = BYTES * 2

  /**
   * Offset from a pointer of the <code>MDB_val.mv_data</code> field.
   */
  val STRUCT_FIELD_OFFSET_DATA: Int = BYTES

  /**
   * Offset from a pointer of the <code>MDB_val.mv_size</code> field.
   */
  val STRUCT_FIELD_OFFSET_SIZE = 0
}

//@SuppressWarnings(Array("checkstyle:abstractclassname"))
abstract class BufferProxy[V,P] extends Comparator[V] {

  /**
   * Allocate a new buffer suitable for passing to
   * #out(java.lang.Object, jnr.ffi.Pointer, long).
   *
   * @return a buffer for passing to the <code>out</code> method
   */
  protected[lmdb4s] def allocate(): V

  /**
   * Compare the two buffers.
   *
   * <p>
   * Implemented as a protected method to discourage use of the buffer proxy
   * in collections etc (given by design it wraps a temporary value only).
   *
   * @param o1 left operand
   * @param o2 right operand
   * @return as per { @link Comparable}
   */
  protected[lmdb4s] def compare(o1: V, o2: V): Int

  /**
   * Deallocate a buffer that was previously provided by #allocate().
   *
   * @param buff the buffer to deallocate (required)
   */
  protected[lmdb4s] def deallocate(buff: V): Unit

  /**
   * Obtain a copy of the bytes contained within the passed buffer.
   *
   * @param buffer a non-null buffer created by this proxy instance
   * @return a copy of the bytes this buffer is currently representing
   */
  protected[lmdb4s] def getBytes(buffer: V): Array[Byte]

  /**
   * Called when the <code>MDB_val</code> should be set to reflect the passed
   * buffer. This buffer will have been created by end users, not
   * #allocate().
   *
   * @param buffer  the buffer to write to <code>MDB_val</code>
   * @param ptr     the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   */
  protected[lmdb4s] def in(buffer: V, ptr: P, ptrAddr: Long): Unit

  /**
   * Called when the <code>MDB_val</code> should be set to reflect the passed
   * buffer.
   *
   * @param buffer  the buffer to write to <code>MDB_val</code>
   * @param size    the buffer size to write to <code>MDB_val</code>
   * @param ptr     the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   */
  protected[lmdb4s] def in(buffer: V, size: Int, ptr: P, ptrAddr: Long): Unit

  /**
   * Called when the <code>MDB_val</code> may have changed and the passed buffer
   * should be modified to reflect the new <code>MDB_val</code>.
   *
   * @param buffer  the buffer to write to <code>MDB_val</code>
   * @param ptr     the pointer to the <code>MDB_val</code>
   * @param ptrAddr the address of the <code>MDB_val</code> pointer
   * @return the buffer for <code>MDB_val</code>
   */
  protected[lmdb4s] def out(buffer: V, ptr: P, ptrAddr: Long): V

  /**
   * Create a new KeyVal to hold pointers for this buffer proxy.
   *
   * @return a non-null key value holder
   */
  final private[lmdb4s] def createKeyVal(): IKeyVal[V, P] = KeyVal[V,P](this)
}*/