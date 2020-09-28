package codecdb.dataset.parser

import org.junit.Test
import org.junit.Assert._
import java.io.File

import codecdb.dataset.parser.csv.CommonsCSVParser
import codecdb.dataset.parser.excel.XLSXParser
import codecdb.dataset.parser.json.LineJsonParser
import codecdb.dataset.parser.tsv.TSVParser

class ParserFactoryTest {

  @Test
  def testGetParser(): Unit = {
    val csvParser = ParserFactory.getParser(new File("src/test/resource/test_guess_schema.csv").toURI)
    assertTrue(csvParser.isInstanceOf[CommonsCSVParser])
    
    val jsonParser = ParserFactory.getParser(new File("src/test/resource/test_guess_schema.json").toURI)
    assertTrue(jsonParser.isInstanceOf[LineJsonParser])
    
    val xlsxParser = ParserFactory.getParser(new File("src/test/resource/test_guess_schema.xlsx").toURI)
    assertTrue(xlsxParser.isInstanceOf[XLSXParser])
    
    val tsvParser = ParserFactory.getParser(new File("src/test/resource/test_guess_schema.tsv").toURI)
    assertTrue(tsvParser.isInstanceOf[TSVParser])
  }
}