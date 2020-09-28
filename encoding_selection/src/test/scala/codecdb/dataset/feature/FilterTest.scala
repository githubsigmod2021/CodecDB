package codecdb.dataset.feature

import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 4/27/17.
  */
class FilterTest {

  @Test
  def testFirstNFilter: Unit = {
    val input = (0 to 1000).map(_.toString).toIterator
    val filtered = Filter.firstNFilter(50)(input).toArray
    assertEquals(50, filtered.size)
    for (i <- 0 until 50) {
      assertEquals(filtered(i), i.toString)
    }
  }

  @Test
  def testIidSamplingFilter: Unit = {
    val input = (0 to 5000).map(_.toString).toIterator
    val filtered = Filter.iidSamplingFilter(0.1)(input).toArray
    assertTrue(400 <= filtered.size)
    assertTrue(filtered.size <= 600)
    filtered.foreach(i => {
      assertTrue(i.toInt >= 0)
      assertTrue(i.toInt <= 5000)
    })
    val set = filtered.toSet
    assertEquals(set.size, filtered.size)
  }

  @Test
  def testSizeFilter: Unit = {
    val input1 = (0 until 20).map(i => "abcde").toIterator
    val input2 = (0 until 1000).map(i => "abcde").toIterator

    val filtered1 = Filter.sizeFilter(500)(input1).toArray
    val filtered2 = Filter.sizeFilter(500)(input2).toArray

    assertEquals(20, filtered1.size)
    assertEquals(100, filtered2.size)
  }

  @Test
  def testMinSizeFilter: Unit = {
    val input1 = (0 until 20).map(i => "abcde").toIterator
    val filtered1 = Filter.minSizeFilter(100, 0.1)(input1).toArray

    assertEquals(20, filtered1.size)

    val input2 = (0 until 1000).map(i => "a").toIterator
    val filtered2 = Filter.minSizeFilter(100, 0.1)(input2).toArray

    assertTrue(170 <= filtered2.size)
    assertTrue(filtered2.size <= 210)
  }
}
