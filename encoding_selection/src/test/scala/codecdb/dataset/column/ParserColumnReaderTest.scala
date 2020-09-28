package codecdb.dataset.column

import java.io.File

import codecdb.dataset.column.csv.CSVColumnReader
import codecdb.dataset.column.json.JsonColumnReader
import codecdb.dataset.schema.Schema
import org.junit.Test
import org.junit.Assert._

import scala.io.Source

class ParserColumnReaderTest {

  @Test
  def testParseColumnReader1: Unit = {
    val schema = Schema.fromParquetFile(new File("src/test/resource/filefmt/test_col_reader.schema").toURI)

    val cr = new CSVColumnReader
    var cols = cr.readColumn(new File("src/test/resource/filefmt/test_col_reader.csv").toURI, schema)

    assertEquals(5, cols.size)

    cols.foreach(col => {
      assertEquals(1, Source.fromFile(col.colFile).getLines().size)
    })

    schema.hasHeader = false
    cols = cr.readColumn(new File("src/test/resource/filefmt/test_col_reader.csv").toURI, schema)

    assertEquals(5, cols.size)

    // Header will be treated as malformated
    cols.foreach(col => {
      assertEquals(1, Source.fromFile(col.colFile).getLines().size)
    })
  }

  @Test
  def testParseColumnReader2: Unit = {
    val schema = Schema.fromParquetFile(new File("src/test/resource/filefmt/test_json_parser.schema").toURI)

    val cr = new JsonColumnReader
    val cols = cr.readColumn(new File("src/test/resource/filefmt/test_json_parser.json").toURI, schema)

    assertEquals(3, cols.size)

    cols.foreach(col => {
      assertEquals(5, Source.fromFile(col.colFile).getLines().size)
    })

  }
}