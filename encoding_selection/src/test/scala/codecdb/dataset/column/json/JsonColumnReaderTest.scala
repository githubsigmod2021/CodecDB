package codecdb.dataset.column.json

import java.io.File

import codecdb.dataset.schema.Schema
import codecdb.model.DataType
import org.junit.Assert.assertEquals
import org.junit.Test

class JsonColumnReaderTest {

  @Test
  def testReadColumn(): Unit = {


    val sourceFile = new File("src/test/resource/filefmt/test_json_parser.json").toURI
    val ccr = new JsonColumnReader()
    val schema = Schema.fromParquetFile(new File("src/test/resource/filefmt/test_json_parser.schema").toURI)
    val cols = ccr.readColumn(sourceFile, schema)

    assertEquals(3, cols.size)
    val arrays = cols.toArray
    
    assertEquals(0, arrays(0).colIndex)
    assertEquals("id", arrays(0).colName)
    assertEquals(DataType.INTEGER, arrays(0).dataType)
    assertEquals(sourceFile, arrays(0).origin)
    
    assertEquals(1, arrays(1).colIndex)
    assertEquals("name", arrays(1).colName)
    assertEquals(DataType.STRING, arrays(1).dataType)
    assertEquals(sourceFile, arrays(1).origin)
    
    assertEquals(2, arrays(2).colIndex)
    assertEquals("gender", arrays(2).colName)
    assertEquals(DataType.STRING, arrays(2).dataType)
    assertEquals(sourceFile, arrays(2).origin)
  }
}