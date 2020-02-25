package com.github.lmdb4s

/**
 * Limits the range and direction of keys to iterate.
 *
 * <p>
 * Immutable once created (although the buffers themselves may not be).
 *
 */
object KeyRange {

  private val BK = new KeyRange[Any](KeyRangeType.BACKWARD_ALL, null, null)
  private val FW = new KeyRange[Any](KeyRangeType.FORWARD_ALL, null, null)

  /**
   * Create a KeyRangeType#FORWARD_ALL range.
   *
   * @tparam T buffer type
   * @return a key range (never null)
   */
  //@SuppressWarnings(Array(Array("checkstyle:SuppressWarnings", "unchecked")))
  def all[T >: Null]: KeyRange[T] = FW.asInstanceOf[KeyRange[T]]

  /**
   * Create a KeyRangeType#BACKWARD_ALL range.
   *
   * @tparam T buffer type
   * @return a key range (never null)
   */
  //@SuppressWarnings(Array(Array("checkstyle:SuppressWarnings", "unchecked")))
  def allBackward[T >: Null]: KeyRange[T] = BK.asInstanceOf[KeyRange[T]]

  /**
   * Create a KeyRangeType#FORWARD_AT_LEAST range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  def atLeast[T >: Null](start: T) = new KeyRange[T](KeyRangeType.FORWARD_AT_LEAST, start, null)

  /**
   * Create a KeyRangeType#BACKWARD_AT_LEAST range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  def atLeastBackward[T >: Null](start: T) = new KeyRange[T](KeyRangeType.BACKWARD_AT_LEAST, start, null)

  /**
   * Create a KeyRangeType#FORWARD_AT_MOST range.
   *
   * @tparam T  buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  def atMost[T >: Null](stop: T) = new KeyRange[T](KeyRangeType.FORWARD_AT_MOST, null, stop)

  /**
   * Create a KeyRangeType#BACKWARD_AT_MOST range.
   *
   * @tparam T buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  def atMostBackward[T >: Null](stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_AT_MOST, null, stop)

  /**
   * Create a KeyRangeType#FORWARD_CLOSED range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def closed[T >: Null](start: T, stop: T) = new KeyRange[T](KeyRangeType.FORWARD_CLOSED, start, stop)

  /**
   * Create a KeyRangeType#BACKWARD_CLOSED range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def closedBackward[T >: Null](start: T, stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_CLOSED, start, stop)

  /**
   * Create a KeyRangeType#FORWARD_CLOSED_OPEN range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def closedOpen[T >: Null](start: T, stop: T) = new KeyRange[T](KeyRangeType.FORWARD_CLOSED_OPEN, start, stop)

  /**
   * Create a KeyRangeType#BACKWARD_CLOSED_OPEN range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def closedOpenBackward[T >: Null](start: T, stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_CLOSED_OPEN, start, stop)

  /**
   * Create a KeyRangeType#FORWARD_GREATER_THAN range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  def greaterThan[T >: Null](start: T) = new KeyRange[T](KeyRangeType.FORWARD_GREATER_THAN, start, null)

  /**
   * Create a KeyRangeType#BACKWARD_GREATER_THAN range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @return a key range (never null)
   */
  def greaterThanBackward[T >: Null](start: T) = new KeyRange[T](KeyRangeType.BACKWARD_GREATER_THAN, start, null)

  /**
   * Create a KeyRangeType#FORWARD_LESS_THAN range.
   *
   * @tparam T buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  def lessThan[T >: Null](stop: T) = new KeyRange[T](KeyRangeType.FORWARD_LESS_THAN, null, stop)

  /**
   * Create a KeyRangeType#BACKWARD_LESS_THAN range.
   *
   * @tparam T buffer type
   * @param stop stop key (required)
   * @return a key range (never null)
   */
  def lessThanBackward[T >: Null](stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_LESS_THAN, null, stop)

  /**
   * Create a KeyRangeType#FORWARD_OPEN range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def open[T >: Null](start: T, stop: T) = new KeyRange[T](KeyRangeType.FORWARD_OPEN, start, stop)

  /**
   * Create a KeyRangeType#BACKWARD_OPEN range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def openBackward[T >: Null](start: T, stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_OPEN, start, stop)

  /**
   * Create a KeyRangeType#FORWARD_OPEN_CLOSED range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def openClosed[T >: Null](start: T, stop: T) = new KeyRange[T](KeyRangeType.FORWARD_OPEN_CLOSED, start, stop)

  /**
   * Create a KeyRangeType#BACKWARD_OPEN_CLOSED range.
   *
   * @tparam T buffer type
   * @param start start key (required)
   * @param stop  stop key (required)
   * @return a key range (never null)
   */
  def openClosedBackward[T >: Null](start: T, stop: T) = new KeyRange[T](KeyRangeType.BACKWARD_OPEN_CLOSED, start, stop)
}

/**
 * Limits the range and direction of keys to iterate.
 *
 * Construct a key range.
 *
 * <p>
 * End user code may find it more expressive to use one of the static methods provided on this class.
 *
 * @tparam T buffer type
 * @param type  key type
 * @param start start key (required if applicable for the passed range type)
 * @param stop  stop key (required if applicable for the passed range type)
 *
 */
final case class KeyRange[T](

  /**
   * Key range type.
   *
   * @return type (never null)
   */
  `type`: KeyRangeType,
  /**
   * Start key.
   *
   * @return start key (may be null)
   */
  start: T,
  /**
   * Stop key.
   *
   * @return stop key (may be null)
   */
  stop: T
) {
  require(`type` != null, "Key range type is required")

  if (`type`.startKeyRequired)
    require(start != null, "Start key is required for this key range type")

  if (`type`.stopKeyRequired)
    require(stop != null, "Stop key is required for this key range type")
}