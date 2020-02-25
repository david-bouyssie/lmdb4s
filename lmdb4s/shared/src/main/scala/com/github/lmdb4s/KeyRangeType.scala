package com.github.lmdb4s

import java.util.Comparator

sealed abstract class KeyRangeType(
  val directionForward: Boolean,
  val startKeyRequired: Boolean,
  val stopKeyRequired: Boolean
) {

  import KeyRangeType._
  import IteratorOp.{CALL_NEXT_OP, RELEASE, TERMINATE}

  /**
   * Determine the iterator action to take when iterator first begins.
   *
   * <p>
   * The iterator will perform this action and present the resulting key to
   * #iteratorOp(java.util.Comparator, java.lang.Object) for decision.
   *
   * @return appropriate action in response to this buffer
   */
  private[lmdb4s] def initialOp(): CursorOp.Value = {
    import CursorOp._

    this match {
      case FORWARD_ALL =>
        FIRST
      case FORWARD_AT_LEAST =>
        GET_START_KEY
      case FORWARD_AT_MOST =>
        FIRST
      case FORWARD_CLOSED =>
        GET_START_KEY
      case FORWARD_CLOSED_OPEN =>
        GET_START_KEY
      case FORWARD_GREATER_THAN =>
        GET_START_KEY
      case FORWARD_LESS_THAN =>
        FIRST
      case FORWARD_OPEN =>
        GET_START_KEY
      case FORWARD_OPEN_CLOSED =>
        GET_START_KEY
      case BACKWARD_ALL =>
        LAST
      case BACKWARD_AT_LEAST =>
        GET_START_KEY_BACKWARD
      case BACKWARD_AT_MOST =>
        LAST
      case BACKWARD_CLOSED =>
        GET_START_KEY_BACKWARD
      case BACKWARD_CLOSED_OPEN =>
        GET_START_KEY_BACKWARD
      case BACKWARD_GREATER_THAN =>
        GET_START_KEY_BACKWARD
      case BACKWARD_LESS_THAN =>
        LAST
      case BACKWARD_OPEN =>
        GET_START_KEY_BACKWARD
      case BACKWARD_OPEN_CLOSED =>
        GET_START_KEY_BACKWARD
      case _ =>
        throw new IllegalStateException("Invalid type")
    }
  }

  /**
   * Determine the iterator's response to the presented key.
   *
   * @tparam T buffer type
   * @tparam C comparator for the buffers
   * @param start  start buffer
   * @param stop   stop buffer
   * @param buffer current key returned by LMDB (may be null)
   * @param c      comparator (required)
   * @return response to this key
   */
  private[lmdb4s] def iteratorOp[T >: Null, C <: Comparator[T]](start: T, stop: T, buffer: T, c: C): IteratorOp.Value = {
    require(c != null, "Comparator required")

    if (buffer == null) return TERMINATE
    this match {
      case FORWARD_ALL =>
        RELEASE
      case FORWARD_AT_LEAST =>
        RELEASE
      case FORWARD_AT_MOST =>
        if (c.compare(buffer, stop) > 0) TERMINATE else RELEASE
      case FORWARD_CLOSED =>
        if (c.compare(buffer, stop) > 0) TERMINATE else RELEASE
      case FORWARD_CLOSED_OPEN =>
        if (c.compare(buffer, stop) >= 0) TERMINATE else RELEASE
      case FORWARD_GREATER_THAN =>
        if (c.compare(buffer, start) == 0) CALL_NEXT_OP else RELEASE
      case FORWARD_LESS_THAN =>
        if (c.compare(buffer, stop) >= 0) TERMINATE else RELEASE
      case FORWARD_OPEN =>
        if (c.compare(buffer, start) == 0) return CALL_NEXT_OP
        if (c.compare(buffer, stop) >= 0) TERMINATE else RELEASE
      case FORWARD_OPEN_CLOSED =>
        if (c.compare(buffer, start) == 0) return CALL_NEXT_OP
        if (c.compare(buffer, stop) > 0) TERMINATE else RELEASE
      case BACKWARD_ALL => RELEASE
      case BACKWARD_AT_LEAST =>
        if (c.compare(buffer, start) > 0) CALL_NEXT_OP else RELEASE // rewind
      case BACKWARD_AT_MOST =>
        if (c.compare(buffer, stop) >= 0) RELEASE else TERMINATE
      case BACKWARD_CLOSED =>
        if (c.compare(buffer, start) > 0) return CALL_NEXT_OP // rewind
        if (c.compare(buffer, stop) >= 0) RELEASE else TERMINATE
      case BACKWARD_CLOSED_OPEN =>
        if (c.compare(buffer, start) > 0) return CALL_NEXT_OP // rewind
        if (c.compare(buffer, stop) > 0) RELEASE else TERMINATE
      case BACKWARD_GREATER_THAN =>
        if (c.compare(buffer, start) >= 0) CALL_NEXT_OP else RELEASE
      case BACKWARD_LESS_THAN =>
        if (c.compare(buffer, stop) > 0) RELEASE else TERMINATE
      case BACKWARD_OPEN =>
        if (c.compare(buffer, start) >= 0) return CALL_NEXT_OP
        if (c.compare(buffer, stop) > 0) RELEASE else TERMINATE
      case BACKWARD_OPEN_CLOSED =>
        if (c.compare(buffer, start) >= 0) return CALL_NEXT_OP
        if (c.compare(buffer, stop) >= 0) RELEASE else TERMINATE
      case _ =>
        throw new IllegalStateException("Invalid type")
    }
  }

  /**
   * Determine the iterator action to take when "next" is called or upon request
   * of #iteratorOp(java.util.Comparator, java.lang.Object).
   *
   * <p>
   * The iterator will perform this action and present the resulting key to
   * #iteratorOp(java.util.Comparator, java.lang.Object) for decision.
   *
   * @return appropriate action for this key range type
   */
  private[lmdb4s] def nextOp(): CursorOp.Value = if (directionForward) CursorOp.NEXT else CursorOp.PREV
}

object KeyRangeType {

  /**
   * Starting on the first key and iterate forward until no keys remain.
   *
   * <p>
   * The "start" and "stop" values are ignored.
   *
   * <p>
   * In our example, the returned keys would be 2, 4, 6 and 8.
   */
  case object FORWARD_ALL extends KeyRangeType(true, false, false)
  /**
   * Start on the passed key (or the first key immediately after it) and
   * iterate forward until no keys remain.
   *
   * <p>
   * The "start" value is required. The "stop" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 5, the returned keys would
   * be 6 and 8. With a passed key of 6, the returned keys would be 6 and 8.
   */
  case object FORWARD_AT_LEAST extends KeyRangeType(true, true, false)

  /**
   * Start on the first key and iterate forward until a key equal to it (or the
   * first key immediately after it) is reached.
   *
   * <p>
   * The "stop" value is required. The "start" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 5, the returned keys would
   * be 2 and 4. With a passed key of 6, the returned keys would be 2, 4 and 6.
   */
  case object FORWARD_AT_MOST extends KeyRangeType(true, false, true)

  /**
   * Iterate forward between the passed keys, matching on the first keys
   * directly equal to the passed key (or immediately following it in the case
   * of the "start" key, or immediately preceding it in the case of the "stop"
   * key).
   *
   * <p>
   * The "start" and "stop" values are both required.
   *
   * <p>
   * In our example and with a passed search range of 3 - 7, the returned keys
   * would be 4 and 6. With a range of 2 - 6, the keys would be 2, 4 and 6.
   */
  case object FORWARD_CLOSED extends KeyRangeType(true, true, true)

  /**
   * Iterate forward between the passed keys, matching on the first keys
   * directly equal to the passed key (or immediately following it in the case
   * of the "start" key, or immediately preceding it in the case of the "stop"
   * key). Do not return the "stop" key.
   *
   * <p>
   * The "start" and "stop" values are both required.
   *
   * <p>
   * In our example and with a passed search range of 3 - 8, the returned keys
   * would be 4 and 6. With a range of 2 - 6, the keys would be 2 and 4.
   */
  case object FORWARD_CLOSED_OPEN extends KeyRangeType(true, true, true)

  /**
   * Start after the passed key (but not equal to it) and iterate forward until
   * no keys remain.
   *
   * <p>
   * The "start" value is required. The "stop" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 4, the returned keys would
   * be 6 and 8. With a passed key of 3, the returned keys would be 4, 6 and 8.
   */
  case object FORWARD_GREATER_THAN extends KeyRangeType(true, true, false)

  /**
   * Start on the first key and iterate forward until a key the passed key has
   * been reached (but do not return that key).
   *
   * <p>
   * The "stop" value is required. The "start" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 5, the returned keys would
   * be 2 and 4. With a passed key of 8, the returned keys would be 2, 4 and 6.
   */
  case object FORWARD_LESS_THAN extends KeyRangeType(true, false, true)

  /**
   * Iterate forward between the passed keys but not equal to either of them.
   *
   * <p>
   * The "start" and "stop" values are both required.
   *
   * <p>
   * In our example and with a passed search range of 3 - 7, the returned keys
   * would be 4 and 6. With a range of 2 - 8, the key would be 4 and 6.
   */
  case object FORWARD_OPEN extends KeyRangeType(true, true, true)

  /**
   * Iterate forward between the passed keys. Do not return the "start" key, but
   * do return the "stop" key.
   *
   * <p>
   * The "start" and "stop" values are both required.
   *
   * <p>
   * In our example and with a passed search range of 3 - 8, the returned keys
   * would be 4, 6 and 8. With a range of 2 - 6, the keys would be 4 and 6.
   */
  case object FORWARD_OPEN_CLOSED extends KeyRangeType(true, true, true)

  /**
   * Start on the last key and iterate backward until no keys remain.
   *
   * <p>
   * The "start" and "stop" values are ignored.
   *
   * <p>
   * In our example, the returned keys would be 8, 6, 4 and 2.
   */
  case object BACKWARD_ALL extends KeyRangeType(false, false, false)

  /**
   * Start on the passed key (or the first key immediately preceding it) and
   * iterate backward until no keys remain.
   *
   * <p>
   * The "start" value is required. The "stop" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 5, the returned keys would
   * be 4 and 2. With a passed key of 6, the returned keys would be 6, 4 and 2.
   * With a passed key of 9, the returned keys would be 8, 6, 4 and 2.
   */
  case object BACKWARD_AT_LEAST extends KeyRangeType(false, true, false)

  /**
   * Start on the last key and iterate backward until a key equal to it (or the
   * first key immediately preceding it it) is reached.
   *
   * <p>
   * The "stop" value is required. The "start" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 5, the returned keys would
   * be 8 and 6. With a passed key of 6, the returned keys would be 8 and 6.
   */
  case object BACKWARD_AT_MOST extends KeyRangeType(false, false, true)

  /**
   * Iterate backward between the passed keys, matching on the first keys
   * directly equal to the passed key (or immediately preceding it in the case
   * of the "start" key, or immediately following it in the case of the "stop"
   * key).
   *
   * <p>
   * The "start" and "stop" values are both required.
   *
   * <p>
   * In our example and with a passed search range of 7 - 3, the returned keys
   * would be 6 and 4. With a range of 6 - 2, the keys would be 6, 4 and 2.
   * With a range of 9 - 3, the returned keys would be 8, 6 and 4.
   */
  case object BACKWARD_CLOSED extends KeyRangeType(false, true, true)

  /**
   * Iterate backward between the passed keys, matching on the first keys
   * directly equal to the passed key (or immediately preceding it in the case
   * of the "start" key, or immediately following it in the case of the "stop"
   * key). Do not return the "stop" key.
   *
   * <p>
   * The "start" and "stop" values are both required.
   *
   * <p>
   * In our example and with a passed search range of 8 - 3, the returned keys
   * would be 8, 6 and 4. With a range of 7 - 2, the keys would be 6 and 4.
   * With a range of 9 - 3, the keys would be 8, 6 and 4.
   */
  case object BACKWARD_CLOSED_OPEN extends KeyRangeType(false, true, true)

  /**
   * Start immediate prior to the passed key (but not equal to it) and iterate
   * backward until no keys remain.
   *
   * <p>
   * The "start" value is required. The "stop" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 6, the returned keys would
   * be 4 and 2. With a passed key of 7, the returned keys would be 6, 4 and 2.
   * With a passed key of 9, the returned keys would be 8, 6, 4 and 2.
   */
  case object BACKWARD_GREATER_THAN extends KeyRangeType(false, true, false)

  /**
   * Start on the last key and iterate backward until the last key greater than
   * the passed "stop" key is reached. Do not return the "stop" key.
   *
   * <p>
   * The "stop" value is required. The "start" value is ignored.
   *
   * <p>
   * In our example and with a passed search key of 5, the returned keys would
   * be 8 and 6. With a passed key of 2, the returned keys would be 8, 6 and 4
   */
  case object BACKWARD_LESS_THAN extends KeyRangeType(false, false, true)

  /**
   * Iterate backward between the passed keys, but do not return the passed
   * keys.
   *
   * <p>
   * The "start" and "stop" values are both required.
   *
   * <p>
   * In our example and with a passed search range of 7 - 2, the returned keys
   * would be 6 and 4. With a range of 8 - 1, the keys would be 6, 4 and 2.
   * With a range of 9 - 4, the keys would be 8 and 6.
   */
  case object BACKWARD_OPEN extends KeyRangeType(false, true, true)

  /**
   * Iterate backward between the passed keys. Do not return the "start" key,
   * but do return the "stop" key.
   *
   * <p>
   * The "start" and "stop" values are both required.
   *
   * <p>
   * In our example and with a passed search range of 7 - 2, the returned keys
   * would be 6, 4 and 2. With a range of 8 - 4, the keys would be 6 and 4.
   * With a range of 9 - 4, the keys would be 8, 6 and 4.
   */
  case object BACKWARD_OPEN_CLOSED extends KeyRangeType(false, true, true)


  /**
   * Action now required with the iterator.
   */
  private[lmdb4s] object IteratorOp extends Enumeration {

    /**
     * Consider iterator completed.
     */
    val TERMINATE: Value = Value

    /**
     * Call KeyRange#nextOp() again and try again.
     */
    val CALL_NEXT_OP: Value = Value

    /**
     * Return the key to the user.
     */
    val RELEASE: Value = Value
  }

  /**
   * Action now required with the cursor.
   */
  private[lmdb4s] object CursorOp extends Enumeration {

    /**
     * Move to first.
     */
    val FIRST: Value = Value

    /**
     * Move to last.
     */
    val LAST: Value = Value

    /**
     * Get "start" key with GetOp#MDB_SET_RANGE.
     */
    val GET_START_KEY: Value = Value

    /**
     * Get "start" key with GetOp#MDB_SET_RANGE, fall back to LAST.
     */
    val GET_START_KEY_BACKWARD: Value = Value

    /**
     * Move forward.
     */
    val NEXT: Value = Value

    /**
     * Move backward.
     */
    val PREV: Value = Value
  }
}