package codecdb.dataset.column

import java.io.File

import codecdb.dataset.column.csv.CSVColumnReader2
import codecdb.dataset.column.json.JsonColumnReader
import codecdb.dataset.column.tsv.TSVColumnReader
import org.junit.Assert.assertTrue
import org.junit.Test

class ColumnReaderFactoryTest {
  @Test
  def testGetColumnReader(): Unit = {
    var cr = ColumnReaderFactory.getColumnReader(new File("src/test/resource/test_columner.csv").toURI)
    assertTrue(cr.isInstanceOf[CSVColumnReader2])

    cr = ColumnReaderFactory.getColumnReader(new File("src/test/resource/test_columner.tsv").toURI)
    assertTrue(cr.isInstanceOf[TSVColumnReader])
    
    cr = ColumnReaderFactory.getColumnReader(new File("src/test/resource/test_json_parser.json").toURI)
    assertTrue(cr.isInstanceOf[JsonColumnReader])
  }
}