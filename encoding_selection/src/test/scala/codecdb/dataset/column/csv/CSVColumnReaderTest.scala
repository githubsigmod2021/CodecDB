package codecdb.dataset.column.csv

import java.io.File

import codecdb.dataset.schema.Schema
import codecdb.model.DataType

import scala.io.Source
import org.junit.Assert.assertEquals
import org.junit.Test

class CSVColumnReaderTest {

  @Test
  def testReadColumn(): Unit = {
    val sourceFile = new File("src/test/resource/filefmt/test_col_reader.csv").toURI
    val ccr = new CSVColumnReader()
    val schema = Schema.fromParquetFile(new File("src/test/resource/filefmt/test_col_reader.schema").toURI)
    val cols = ccr.readColumn(sourceFile, schema)

    assertEquals(5, cols.size)
    val arrays = cols.toArray

    assertEquals(0, arrays(0).colIndex)
    assertEquals("id", arrays(0).colName)
    assertEquals(DataType.INTEGER, arrays(0).dataType)
    assertEquals(sourceFile, arrays(0).origin)

    assertEquals(1, arrays(1).colIndex)
    assertEquals("c1", arrays(1).colName)
    assertEquals(DataType.BOOLEAN, arrays(1).dataType)
    assertEquals(sourceFile, arrays(1).origin)

    assertEquals(2, arrays(2).colIndex)
    assertEquals("c2", arrays(2).colName)
    assertEquals(DataType.FLOAT, arrays(2).dataType)
    assertEquals(sourceFile, arrays(2).origin)

    assertEquals(3, arrays(3).colIndex)
    assertEquals("c3", arrays(3).colName)
    assertEquals(DataType.STRING, arrays(3).dataType)
    assertEquals(sourceFile, arrays(3).origin)

    assertEquals(4, arrays(4).colIndex)
    assertEquals("c4", arrays(4).colName)
    assertEquals(DataType.INTEGER, arrays(4).dataType)
    assertEquals(sourceFile, arrays(4).origin)

    arrays.foreach { col =>
      {
        assertEquals(1, Source.fromFile(col.colFile).getLines().size)
      }
    }
  }
}