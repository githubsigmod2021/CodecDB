package codecdb.dataset.parser.csv

import org.junit.Assert._

import org.junit.Test

class CSVParserForTest extends CSVParser {
  override def parseLine(input: String) = super.parseLine(input)
}

class CSVParserTest {

  @Test
  def testParseLine(): Unit = {

    val parser = new CSVParserForTest()

    var input = "a,b,c,d,e"
    var output = parser.parseLine(input)

    assertEquals(5, output.length())

    input = "a,3,7,\"323,m4,2,34\""
    output = parser.parseLine(input)

    assertEquals(4, output.length())

    input = """a,b,"5,23.24",53.4149,132"""
    output = parser.parseLine(input)

    assertEquals(5, output.length())

    input = """,,,,,"""
    output = parser.parseLine(input)

    assertEquals(6, output.length())
  }

  @Test
  def testSpecialLine(): Unit = {
    val parser = new CSVParserForTest()
    val line1 = """19823,HT223608,03/29/2011 08:11:00 AM,009XX W FULLERTON AVE,0110,HOMICIDE,FIRST DEGREE MURDER,"CTA ""L"" PLATFORM",true,false,1933,019,43,7,01A,1169577,1916141,2011,08/17/2015 03:03:40 PM,41.925398449,-87.652311296,"(41.925398449, -87.652311296)""""

    val output = parser.parseLine(line1)
    assertEquals(22, output.length())
  }
}