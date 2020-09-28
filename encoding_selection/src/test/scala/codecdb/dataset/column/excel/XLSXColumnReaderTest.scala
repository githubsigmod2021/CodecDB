package codecdb.dataset.column.excel

import org.junit.Test
import org.junit.Assert._
import java.io.File

import codecdb.dataset.schema.Schema
import edu.uchicago.cs.encsel.dataset.schema.Schema

import scala.io.Source

class XLSXColumnReaderTest {

  @Test
  def testRead(): Unit = {
    val cr = new XLSXColumnReader()
    val schema = Schema.fromParquetFile(new File("src/test/resource/filefmt/test_col_reader_xlsx.schema").toURI)

    val columns = cr.readColumn(new File("src/test/resource/filefmt/test_col_reader_xlsx.xlsx").toURI, schema)

    assertEquals(12, columns.size)

    columns.foreach(col => {
      assertEquals(11, Source.fromFile(col.colFile).getLines().size)
    })
  }
}