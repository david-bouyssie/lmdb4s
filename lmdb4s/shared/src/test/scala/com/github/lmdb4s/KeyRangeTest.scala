package com.github.lmdb4s

import java.util.ArrayList
import java.util.Comparator

import KeyRange._

import utest._

/**
 * Test KeyRange.
 *
 * <p>
 * This test case focuses on the contractual correctness detailed in
 * KeyRangeType. It does this using integers as per the JavaDoc
 * examples.
 */
object KeyRangeTest extends TestSuite {

  private lazy val cursor = new KeyRangeTest.FakeCursor()

  override def utestBeforeEach(path: Seq[String]): Unit = {
    cursor.reset()
  }

  val tests = Tests {
    'allBackwardTest - allBackwardTest()
    'allTest - allTest()
    'atLeastBackwardTest - atLeastBackwardTest()
    'atLeastTest - atLeastTest()
    'atMostBackwardTest - atMostBackwardTest()
    'atMostTest - atMostTest()
    'closedBackwardTest - closedBackwardTest()
    'closedOpenBackwardTest - closedOpenBackwardTest()
    'closedOpenTest - closedOpenTest()
    'closedTest - closedTest()
    'fakeCursor - fakeCursor()
    'greaterThanBackwardTest - greaterThanBackwardTest()
    'greaterThanTest - greaterThanTest()
    'lessThanBackwardTest - lessThanBackwardTest()
    'lessThanTest - lessThanTest()
    'openBackwardTest - openBackwardTest()
    'openClosedBackwardTest - openClosedBackwardTest()
    'openClosedTest - openClosedTest()
    'openTest - openTest()
  }

  /**
   * Cursor that behaves like an LMDB cursor would.
   *
   * <p>
   * We use <code>Integer</code> rather than the primitive to represent a
   * <code>null</code> buffer.
   */
  private object FakeCursor {
    private val KEYS = Array[Integer](2, 4, 6, 8)
  }

  private class FakeCursor() {
    private var position = 0

    import KeyRangeType.CursorOp._

    //@SuppressWarnings(Array("checkstyle:ReturnCount"))
    def apply(op: KeyRangeType.CursorOp.Value, startKey: Integer): Integer = {
      op match {
        case FIRST =>
          first()
        case LAST =>
          last()
        case NEXT =>
          next()
        case PREV =>
          prev()
        case GET_START_KEY =>
          getWithSetRange(startKey).get
        case GET_START_KEY_BACKWARD =>
          getWithSetRange(startKey).getOrElse(last())
        case _ =>
          throw new IllegalStateException("Unknown operation")
      }
    }

    def first(): Integer = {
      position = 0
      FakeCursor.KEYS(position)
    }

    def getWithSetRange(startKey: Integer): Option[Integer] = {
      var idx = 0
      while (idx < FakeCursor.KEYS.length) {
        val candidate = FakeCursor.KEYS(idx)
        if (candidate == startKey || candidate > startKey) {
          position = idx
          return Option(FakeCursor.KEYS(position))
        }

        idx += 1
      }

      None
    }

    def last(): Integer = {
      position = FakeCursor.KEYS.length - 1
      FakeCursor.KEYS(position)
    }

    def next(): Integer = {
      position += 1
      if (position == FakeCursor.KEYS.length) return null
      FakeCursor.KEYS(position)
    }

    def prev(): Integer = {
      position -= 1
      if (position == -1) return null
      FakeCursor.KEYS(position)
    }

    def reset(): Unit = {
      position = 0
    }
  }

  def allBackwardTest(): Unit = {
    verify(allBackward, 8, 6, 4, 2)
  }

  def allTest(): Unit = {
    verify(all, 2, 4, 6, 8)
  }

  def atLeastBackwardTest(): Unit = {
    verify(atLeastBackward[Integer](5), 4, 2)
    verify(atLeastBackward[Integer](6), 6, 4, 2)
    verify(atLeastBackward[Integer](9), 8, 6, 4, 2)
  }

  def atLeastTest(): Unit = {
    verify(atLeast[Integer](5), 6, 8)
    verify(atLeast[Integer](6), 6, 8)
  }

  def atMostBackwardTest(): Unit = {
    verify(atMostBackward[Integer](5), 8, 6)
    verify(atMostBackward[Integer](6), 8, 6)
  }

  def atMostTest(): Unit = {
    verify(atMost[Integer](5), 2, 4)
    verify(atMost[Integer](6), 2, 4, 6)
  }

  def closedBackwardTest(): Unit = {
    verify(closedBackward[Integer](7, 3), 6, 4)
    verify(closedBackward[Integer](6, 2), 6, 4, 2)
    verify(closedBackward[Integer](9, 3), 8, 6, 4)
  }

  def closedOpenBackwardTest(): Unit = {
    verify(closedOpenBackward(8, 3), 8, 6, 4)
    verify(closedOpenBackward(7, 2), 6, 4)
    verify(closedOpenBackward(9, 3), 8, 6, 4)
  }

  def closedOpenTest(): Unit = {
    verify(closedOpen(3, 8), 4, 6)
    verify(closedOpen(2, 6), 2, 4)
  }

  def closedTest(): Unit = {
    verify(closed(3, 7), 4, 6)
    verify(closed(2, 6), 2, 4, 6)
  }

  def fakeCursor(): Unit = {
    assert(cursor.first() == 2)
    assert(cursor.next() == 4)
    assert(cursor.next() == 6)
    assert(cursor.next() == 8)
    assert(cursor.next() == null)
    assert(cursor.first() == 2)
    assert(cursor.prev() == null)
    assert(cursor.getWithSetRange(3).get == 4)
    assert(cursor.next() == 6)
    assert(cursor.getWithSetRange(1).get == 2)
    assert(cursor.last() == 8)
    assert(cursor.getWithSetRange(100).isEmpty)
  }

  def greaterThanBackwardTest(): Unit = {
    verify(greaterThanBackward[Integer](6), 4, 2)
    verify(greaterThanBackward[Integer](7), 6, 4, 2)
    verify(greaterThanBackward[Integer](9), 8, 6, 4, 2)
  }

  def greaterThanTest(): Unit = {
    verify(greaterThan[Integer](4), 6, 8)
    verify(greaterThan[Integer](3), 4, 6, 8)
  }

  def lessThanBackwardTest(): Unit = {
    verify(lessThanBackward[Integer](5), 8, 6)
    verify(lessThanBackward[Integer](2), 8, 6, 4)
  }

  def lessThanTest(): Unit = {
    verify(lessThan[Integer](5), 2, 4)
    verify(lessThan[Integer](8), 2, 4, 6)
  }

  def openBackwardTest(): Unit = {
    verify(openBackward(7, 2), 6, 4)
    verify(openBackward(8, 1), 6, 4, 2)
    verify(openBackward(9, 4), 8, 6)
  }

  def openClosedBackwardTest(): Unit = {
    verify(openClosedBackward(7, 2), 6, 4, 2)
    verify(openClosedBackward(8, 4), 6, 4)
    verify(openClosedBackward(9, 4), 8, 6, 4)
  }

  def openClosedTest(): Unit = {
    verify(openClosed(3, 8), 4, 6, 8)
    verify(openClosed(2, 6), 4, 6)
  }

  def openTest(): Unit = {
    verify(open(3, 7), 4, 6)
    verify(open(2, 8), 4, 6)
  }

  private val intComparator = new Comparator[Integer] {
    def compare(valA: Integer, valB: Integer): Int = {
      Integer.compare(valA, valB)
    }
  }

  private def verify(range: KeyRange[Integer], expected: Integer*): Unit = {
    import KeyRangeType.IteratorOp._

    val rangeType = range.`type`
    var buff = cursor.apply(rangeType.initialOp(), range.start)
    val results = new ArrayList[Integer]()

    var op: KeyRangeType.IteratorOp.Value = null
    do {
      op = rangeType.iteratorOp(range.start, range.stop, buff, intComparator) // [Integer, Comparator[Integer]]

      op match {
        case CALL_NEXT_OP =>
          buff = cursor.apply(rangeType.nextOp(), range.start)
        case RELEASE =>
          results.add(buff)
          buff = cursor.apply(rangeType.nextOp(), range.start)
        case TERMINATE => ()
        case _ =>
          throw new IllegalStateException("Unknown operation")
      }

    } while (op != TERMINATE)

    assert(results.size == expected.length)

    var idx = 0
    while (idx < results.size) {
      assert(results.get(idx) == expected(idx))
      idx += 1
    }

    assert(results.size == expected.length)
  }
}