package codecdb.dataset.parser.tsv

import org.junit.Assert._
import org.junit.Test
import java.io.{ByteArrayInputStream, File}

import codecdb.dataset.schema.Schema

class SimpleTSVParserTest {

  @Test
  def testParseLine: Unit = {
    val parser = new SimpleTSVParser
    parser.schema = new Schema()
    val result = parser.parseLine("a\t\tb\tttt\t\tdae_ma\tafsew\t\t")

    assertEquals(9, result.length())
  }

  @Test
  def testGuessHeader: Unit = {
    val parser = new SimpleTSVParser
    val records = parser.parse(new File("src/test/resource/filefmt/test_tsv_parser.tsv").toURI, null).toArray
    assertArrayEquals(Array[Object]("M", "W", "N", "O"), parser.guessHeaderName.toArray[Object])
    assertEquals(5, records.length)
    assertEquals(4, records(0).length())
    assertEquals("3$$3.3$$Good Dog2$$7", records(2).toString())
    assertEquals("4,3.4,Good Dog3,8", records(3).iterator().mkString(","))
  }
}

class TSVParserTest {

  @Test
  def testParse: Unit = {
    val input = "A\tB\tC\r001\t3232\t\"53434343\t32223\r\nwe23\"\rabc\t001\t423"
    val parser = new TSVParser

    val records = parser.parse(new ByteArrayInputStream(input.getBytes("utf-8")), null).toArray

    assertEquals(2, records.size)
    assertEquals(3, records(0).length())
    assertEquals(3, records(1).length)
    val headers = parser.guessHeaderName
    assertEquals(3, headers.length)
    assertEquals("A", headers(0))
    assertEquals("B", headers(1))
    assertEquals("C", headers(2))

    assertEquals("001", records(0)(0))
    assertEquals("3232", records(0)(1))
    assertEquals("53434343\t32223\r\nwe23", records(0)(2))
    assertEquals("abc", records(1)(0))
    assertEquals("001", records(1)(1))
    assertEquals("423", records(1)(2))
  }

  def testCRLF: Unit = {
    val input = "A\tB\tC\r001\tx\ty\r\n002\tw\tq\n003\to\tu"
    val parser = new TSVParser

    val records = parser.parse(new ByteArrayInputStream(input.getBytes("utf-8")), null).toArray

    assertEquals(2, records.size)
    assertEquals(3, records(0).length())
    assertEquals(3, records(1).length)
    val headers = parser.guessHeaderName
    assertEquals(3, headers.length)
    assertEquals("A", headers(0))
    assertEquals("B", headers(1))
    assertEquals("C", headers(2))

    assertEquals("001", records(0)(0))
    assertEquals("x", records(0)(1))
    assertEquals("y", records(0)(2))
    assertEquals("002", records(1)(0))
    assertEquals("w", records(1)(1))
    assertEquals("q", records(1)(2))
    assertEquals("003", records(2)(0))
    assertEquals("o", records(2)(1))
    assertEquals("u", records(2)(2))
  }

  @Test
  def testEmptyRecord: Unit = {
    val input = "A\tB\tC\tD\r001\t\t\t\r\n002\tw\tq\t003"
    val parser = new TSVParser

    val records = parser.parse(new ByteArrayInputStream(input.getBytes("utf-8")), null).toArray

    assertEquals(2, records.size)
    assertEquals(4, records(0).length())
    assertEquals(4, records(1).length)
    val headers = parser.guessHeaderName
    assertEquals(4, headers.length)
    assertEquals("A", headers(0))
    assertEquals("B", headers(1))
    assertEquals("C", headers(2))
    assertEquals("D", headers(3))

    assertEquals("001", records(0)(0))
    assertEquals("", records(0)(1))
    assertEquals("", records(0)(2))
    assertEquals("", records(0)(3))

    assertEquals("002", records(1)(0))
    assertEquals("w", records(1)(1))
    assertEquals("q", records(1)(2))
    assertEquals("003", records(1)(3))
  }

  @Test
  def testEscape:Unit = {
    val input = "A\tB\tC\tD\r001\t\"324\"\"jj\"\t\t\r\n002\tw\tq\t003"
    val parser = new TSVParser

    val records = parser.parse(new ByteArrayInputStream(input.getBytes("utf-8")), null).toArray

    assertEquals(2, records.size)
    assertEquals(4, records(0).length())
    assertEquals(4, records(1).length)
    val headers = parser.guessHeaderName
    assertEquals(4, headers.length)
    assertEquals("A", headers(0))
    assertEquals("B", headers(1))
    assertEquals("C", headers(2))
    assertEquals("D", headers(3))

    assertEquals("001", records(0)(0))
    assertEquals("324\"jj", records(0)(1))
    assertEquals("", records(0)(2))
    assertEquals("", records(0)(3))

    assertEquals("002", records(1)(0))
    assertEquals("w", records(1)(1))
    assertEquals("q", records(1)(2))
    assertEquals("003", records(1)(3))
  }

  @Test
  def testGuessHeader: Unit = {
    val parser = new TSVParser
    val records = parser.parse(new File("src/test/resource/filefmt/test_tsv_parser.tsv").toURI, null).toArray
    assertArrayEquals(Array[Object]("M", "W", "N", "O"), parser.guessHeaderName.toArray[Object])
    assertEquals(5, records.length)
    assertEquals(4, records(0).length())
    assertEquals("3$$3.3$$Good Dog2$$7", records(2).toString())
    assertEquals("4,3.4,Good Dog3,8", records(3).iterator().mkString(","))
  }
}