package codecdb.dataset.parser.json

import java.io.File

import codecdb.dataset.schema.Schema
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test

class JsonParserTest {

  @Test
  def testParse: Unit = {
    val parser = new LineJsonParser
    val schema = Schema.fromParquetFile(new File("src/test/resource/filefmt/test_json_parser.schema").toURI)

    parser.schema = schema
    val data = parser.parseLine("""{"id":32,"name":"WangDaChui","gender":"male","rpg":"Good"}""")
    assertEquals(3, data.length())
  }

  @Test
  def testGuessHeader: Unit = {
    val parser = new LineJsonParser
    val records = parser.parse(new File("src/test/resource/filefmt/test_json_parser.json").toURI, null).toArray
    assertArrayEquals(Array[Object]("add", "gender", "id", "name"), parser.guessHeaderName.toArray[Object])
    assertEquals(5, records.length)
    assertEquals(4, records(0).length())
    assertEquals("""$$male$$34$${"a":"x","b":"c"}""", records(2).toString())
    assertEquals(",female,35,Lily3", records(3).iterator().mkString(","))
  }
}