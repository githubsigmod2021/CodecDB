package codecdb.ptnmining.compose

import org.junit.Assert._
import org.junit.Test

class RegexParserTest {

  @Test
  def testParse: Unit = {
    val regex = "^MIR-([0-9a-fA-F]+)-([0-9a-fA-F]+)-(\\d+)(-)?(\\d*)$"

    val parsed = RegexParser.parse(regex)

    assertEquals(13, parsed.length)
    assertEquals(0, parsed(5).rep)
    assertEquals(0, parsed(6).rep)
    assertEquals(1, parsed(10).rep)
    assertEquals(2, parsed(11).asInstanceOf[GroupToken].children.last.rep)
  }

  @Test
  def testParse2: Unit = {
    val regex = "^(\\d+)-(\\d+)-(\\d+)\\s+(\\d+):(\\d+):(\\d+\\.?\\d*)$"
    val parsed = RegexParser.parse(regex)

    assertEquals(13, parsed.length)
    assertEquals(3, parsed(6).rep)
    assertEquals(true, parsed(6).asInstanceOf[SimpleToken].escape)
  }

  @Test
  def testParseRange: Unit = {
    val regex = "^([a-zA-Z]+)(_)?([a-zA-Z]*)(_)?([a-zA-Z]*)(_)?([a-zA-Z]*)$"
    val parsed = RegexParser.parse(regex)

    assertEquals(9, parsed.length)
    val g = parsed(7).asInstanceOf[GroupToken]
    assertEquals(1, g.children.size)
    val r = g.children(0).asInstanceOf[RangeToken]
    assertEquals(2, r.rep)
    assertEquals(2, r.children.length)
    assertEquals('a', r.children(0)._1.content)
    assertEquals('z', r.children(0)._2.content)
    assertEquals('A', r.children(1)._1.content)
    assertEquals('Z', r.children(1)._2.content)
  }

  @Test
  def testParseSpecialRange: Unit = {
    val regex = "[a*b+c^xd-]+[^t-qdm-py]*"
    val parsed = RegexParser.parse(regex)

    assertEquals(2, parsed.size)
    val r = parsed(0).asInstanceOf[RangeToken]

    assertEquals(9, r.children.size)
    assertEquals(3, r.rep)
    assertTrue(r.inclusive)
    for (i <- 0 to 8) {
      assertEquals("Error at %d".format(i), regex(i + 1), r.children(i)._1.content)
      assertEquals(0, r.children(i)._1.rep)
      assertEquals(false, r.children(i)._1.escape)
      assertEquals(null, r.children(i)._2)
    }

    val r2 = parsed(1).asInstanceOf[RangeToken]
    assertFalse(r2.inclusive)
    assertEquals(4, r2.children.size)
    assertEquals(2, r2.rep)

    for (i <- 0 to 3) {
      assertEquals(0, r2.children(i)._1.rep)
      assertEquals(false, r2.children(i)._1.escape)
      if (r2.children(i)._2 != null) {
        assertEquals(0, r2.children(i)._2.rep)
        assertEquals(false, r2.children(i)._2.escape)
      }
    }

    assertEquals('t', r2.children(0)._1.content)
    assertEquals('q', r2.children(0)._2.content)

    assertEquals('d', r2.children(1)._1.content)
    assertEquals(null, r2.children(1)._2)

    assertEquals('m', r2.children(2)._1.content)
    assertEquals('p', r2.children(2)._2.content)

    assertEquals('y', r2.children(3)._1.content)
    assertEquals(null, r2.children(3)._2)
  }

  @Test
  def testWrongRange: Unit = {
    val wrongexp = "[\\s-wdt]"
    try {
      val parsed = RegexParser.parse(wrongexp)
      fail("Should not reach here")
    } catch {
      case e: Exception => {

      }
    }
  }
}
