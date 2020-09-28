package codecdb.dataset.parser.csv

import java.io.File

import org.junit.Test
import org.junit.Assert._
import java.util.Arrays

import codecdb.dataset.schema.Schema

class CommonsCSVParserTest {

  @Test
  def testParse(): Unit = {
    val records = new CommonsCSVParser().parse(new File("src/test/resource/filefmt/test_csv_parser.csv").toURI,
      Schema.fromParquetFile(new File("src/test/resource/filefmt/test_csv_parser.schema").toURI)).toArray
    assertEquals(7, records.length)
    assertEquals("""What a said "Not Good"""", records(0)(1))
  }

  @Test
  def testGuessHeader: Unit = {
    val parser = new CommonsCSVParser()
    val records = parser.parse(new File("src/test/resource/filefmt/test_csv_parser.csv").toURI,
      null).toArray
    val guessedHeader = parser.guessHeaderName
    assertArrayEquals(Array[Object]("A", "B", "C", "D", "E"), guessedHeader.toArray[Object])
    assertEquals("A", guessedHeader(0))
    assertEquals(7, records.length)
    assertEquals(5, records(0).length())
    assertEquals("""What a said "Not Good"""", records(0)(1))
    assertEquals("CSVRecord [comment=null, mapping=null, recordNumber=4, values=[23, asdf4, 32.42, 3, 0]]", records(2).toString())
    assertArrayEquals(Array[Object]("23", "asdf4", "32.42", "3", "0"), records(2).iterator().toArray[Object])
  }
}