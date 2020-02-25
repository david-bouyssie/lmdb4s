package com.github.lmdb4s

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.Comparator

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator.DEFAULT
import org.agrona.DirectBuffer
import org.agrona.concurrent.UnsafeBuffer

import ByteArrayProxy.PROXY_BA
import ByteBufferProxy.PROXY_OPTIMAL

import utest._

/**
 * Test Env.
 */
object ComparatorTest extends TestSuite {

  val comparators: Seq[ComparatorTest.ComparatorRunner] = Seq(
    new ComparatorTest.StringRunner(),
    //new DirectBufferRunner()
    new ByteArrayRunner(),
    new ByteBufferRunner()
    //new NettyRunner()
    //new GuavaUnsignedBytes()
    //new GuavaSignedBytes()
  )

  val tests = Tests {
    // FIXME: there is a bug in utest https://github.com/lihaoyi/utest/issues/208
    // this has been fixed in further versions but we can't upgrade for now
    /*
    comparators.foreach { comparator =>
      val tester = new ComparatorTest(comparator)
      'atLeastOneBufferHasEightBytes_ + comparator.name - tester.atLeastOneBufferHasEightBytes()
      'buffersOfTwoBytes_ + comparator.name - tester.buffersOfTwoBytes()
      'equalBuffers_ + comparator.name - tester.equalBuffers()
    }*/
    var comparator = comparators(0)
    var tester = new ComparatorTest(comparator)

    'atLeastOneBufferHasEightBytes_StringComparator - tester.atLeastOneBufferHasEightBytes()
    'buffersOfTwoBytes_StringComparator - tester.buffersOfTwoBytes()
    'equalBuffers_StringComparator - tester.equalBuffers()

    comparator = comparators(1)
    tester = new ComparatorTest(comparator)

    'atLeastOneBufferHasEightBytes_ByteArrayComparator - tester.atLeastOneBufferHasEightBytes()
    'buffersOfTwoBytes_ByteArrayComparator - tester.buffersOfTwoBytes()
    'equalBuffers_ByteArrayComparator - tester.equalBuffers()

    comparator = comparators(2)
    tester = new ComparatorTest(comparator)

    'atLeastOneBufferHasEightBytes_ByteBufferComparator - tester.atLeastOneBufferHasEightBytes()
    'buffersOfTwoBytes_ByteBufferComparator - tester.buffersOfTwoBytes()
    'equalBuffers_ByteBufferComparator - tester.equalBuffers()
  }

  // H = 1 (high), L = 0 (low), X = byte not set in buffer
  private val HL = buffer(1, 0)
  private val HLLLLLLL = buffer(1, 0, 0, 0, 0, 0, 0, 0)
  private val HLLLLLLX = buffer(1, 0, 0, 0, 0, 0, 0)
  private val HX = buffer(1)
  private val LH = buffer(0, 1)
  private val LHLLLLLL = buffer(0, 1, 0, 0, 0, 0, 0, 0)
  private val LL = buffer(0, 0)
  private val LLLLLLLL = buffer(0, 0, 0, 0, 0, 0, 0, 0)
  private val LLLLLLLX = buffer(0, 0, 0, 0, 0, 0, 0)
  private val LX = buffer(0)
  private val XX = buffer()

  private def buffer(bytes: Int*): Array[Byte] = {
    val array = new Array[Byte](bytes.length)
    var i = 0
    while (i < bytes.length) {
      array(i) = bytes(i).toByte

      i += 1
    }
    array
  }


  class ComparatorTest(comparator: ComparatorRunner) {

    import ComparatorResult._

    def atLeastOneBufferHasEightBytes(): Unit = {
      assert(get(comparator.compare(HLLLLLLL, LLLLLLLL)) == GREATER_THAN)
      assert(get(comparator.compare(LLLLLLLL, HLLLLLLL)) == LESS_THAN)
      assert(get(comparator.compare(LHLLLLLL, LLLLLLLL)) == GREATER_THAN)
      assert(get(comparator.compare(LLLLLLLL, LHLLLLLL)) == LESS_THAN)
      assert(get(comparator.compare(LLLLLLLL, LLLLLLLX)) == GREATER_THAN)
      assert(get(comparator.compare(LLLLLLLX, LLLLLLLL)) == LESS_THAN)
      assert(get(comparator.compare(HLLLLLLL, HLLLLLLX)) == GREATER_THAN)
      assert(get(comparator.compare(HLLLLLLX, HLLLLLLL)) == LESS_THAN)
      assert(get(comparator.compare(HLLLLLLX, LHLLLLLL)) == GREATER_THAN)
      assert(get(comparator.compare(LHLLLLLL, HLLLLLLX)) == LESS_THAN)
    }

    def buffersOfTwoBytes(): Unit = {
      assert(get(comparator.compare(LL, XX)) == GREATER_THAN)
      assert(get(comparator.compare(XX, LL)) == LESS_THAN)
      assert(get(comparator.compare(LL, LX)) == GREATER_THAN)
      assert(get(comparator.compare(LX, LL)) == LESS_THAN)
      assert(get(comparator.compare(LH, LX)) == GREATER_THAN)
      assert(get(comparator.compare(LX, HL)) == LESS_THAN)
      assert(get(comparator.compare(HX, LL)) == GREATER_THAN)
      assert(get(comparator.compare(LH, HX)) == LESS_THAN)
    }

    def equalBuffers(): Unit = {
      assert(get(comparator.compare(LL, LL)) == EQUAL_TO)
      assert(get(comparator.compare(HX, HX)) == EQUAL_TO)
      assert(get(comparator.compare(LH, LH)) == EQUAL_TO)
      assert(get(comparator.compare(LL, LL)) == EQUAL_TO)
      assert(get(comparator.compare(LX, LX)) == EQUAL_TO)
      assert(get(comparator.compare(HLLLLLLL, HLLLLLLL)) == EQUAL_TO)
      assert(get(comparator.compare(HLLLLLLX, HLLLLLLX)) == EQUAL_TO)
      assert(get(comparator.compare(LHLLLLLL, LHLLLLLL)) == EQUAL_TO)
      assert(get(comparator.compare(LLLLLLLL, LLLLLLLL)) == EQUAL_TO)
      assert(get(comparator.compare(LLLLLLLX, LLLLLLLX)) == EQUAL_TO)
    }

  }

  /**
   * Converts an integer result code into its contractual meaning.
   */
  private object ComparatorResult extends Enumeration {
    val LESS_THAN, EQUAL_TO, GREATER_THAN = Value

    def get(comparatorResult: Int): ComparatorResult.Value = {
      if (comparatorResult == 0) return EQUAL_TO
      if (comparatorResult < 0) LESS_THAN
      else GREATER_THAN
    }
  }

  /**
   * Interface that can test a BufferProxy <code>compare</code> method.
   */
  private[lmdb4s] trait ComparatorRunner extends Comparator[Array[Byte]] {

    def name: String

    /**
     * Convert the passed byte arrays into the proxy's relevant buffer type and
     * then invoke the comparator.
     *
     * @param o1 lhs buffer content
     * @param o2 rhs buffer content
     * @return as per { @link Comparable}
     */
    def compare(o1: Array[Byte], o2: Array[Byte]): Int
  }

  /**
   * Tests ByteArrayProxy.
   */
  private class ByteArrayRunner extends ComparatorRunner {
    def name = "ByteArrayComparator"

    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = PROXY_BA.compare(o1, o2)
  }

  /**
   * Tests ByteBufferProxy.
   */
  private class ByteBufferRunner extends ComparatorRunner {
    def name = "ByteBufferComparator"

    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      val o1b = ByteBuffer.wrap(o1)
      val o2b = ByteBuffer.wrap(o2)
      PROXY_OPTIMAL.compare(o1b, o2b)
    }
  }

  /*
/**
 * Tests DirectBufferProxy.
 */
private class DirectBufferRunner extends ComparatorRunner {
  override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
    val o1b = new UnsafeBuffer(o1)
    val o2b = new UnsafeBuffer(o2)
    PROXY_DB.compare(o1b, o2b)
  }
}

/**
 * Tests using Guava's SignedBytes comparator.
 */
private class GuavaSignedBytes extends ComparatorRunner {
  override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
    val c = SignedBytes.lexicographicalComparator
    c.compare(o1, o2)
  }
}

/**
 * Tests using Guava's UnsignedBytes comparator.
 */
private class GuavaUnsignedBytes extends ComparatorRunner {
  override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
    val c = UnsignedBytes.lexicographicalComparator
    c.compare(o1, o2)
  }
}

/**
 * Tests ByteBufProxy.
 */
private class NettyRunner extends ComparatorRunner {
  override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
    val o1b = DEFAULT.directBuffer(o1.length)
    val o2b = DEFAULT.directBuffer(o2.length)
    o1b.writeBytes(o1)
    o2b.writeBytes(o2)
    new ByteBufProxy().compare(o1b, o2b)
  }
}*/

  /**
   * Tests String by providing a reference implementation of what a
   * comparator involving ASCII-encoded bytes should return.
   */
  private class StringRunner extends ComparatorRunner {
    def name = "StringComparator"

    override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
      val o1s = new String(o1, US_ASCII)
      val o2s = new String(o2, US_ASCII)
      o1s.compareTo(o2s)
    }
  }

}